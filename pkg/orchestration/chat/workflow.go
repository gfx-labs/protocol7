package chat

import (
	"errors"
	"slices"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"
	"github.com/gfx-labs/protocol7/pkg/orchestration/upgrade"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// versionUpgradeWake gates the hourly wake-up of an idle chat. A history from
// before it waited for a message with no timer, and replays that way.
const versionUpgradeWake = "upgrade-wake"

// versionStopKeepsQueue gates what a stop does to the messages that wait. A
// history from before it dropped them, and replays that way.
const versionStopKeepsQueue = "stop-keeps-queue"

// upgradeWake is how long an idle chat waits for a message before it looks
// again at whether a newer worker build is the target. Without it a chat that
// nobody writes to would stay on its build, and keep that build from
// draining, until the next message.
const upgradeWake = time.Hour

// run is one run of a chat. It opens a session with a clone of each
// repository, answers each message with one turn, in the order the messages
// came, and closes the session when the chat is closed or cancelled.
func run(ctx workflow.Context, state State) (*Report, error) {
	if err := workflow.SetQueryHandler(ctx, QueryState, func() (*navi.ChatState, error) { return state.ChatState, nil }); err != nil {
		return &Report{}, err
	}
	// The session's agent calls these. A call from any other session is
	// refused before it enters history.
	sessions := func() []string {
		if state.session == nil {
			return nil
		}
		return []string{state.session.ID}
	}
	if err := p7.HandleTools(ctx, chatTools(&state), sessions); err != nil {
		return &Report{}, err
	}
	// The search attribute carries over a continue-as-new by itself.
	if err := workflow.SetUpdateHandler(ctx, UpdateArchive, func(ctx workflow.Context, archive *Archive) error {
		if err := workflow.UpsertTypedSearchAttributes(ctx, AttributeArchived.ValueSet(archive.Archived)); err != nil {
			return err
		}
		state.Archived = archive.Archived
		return nil
	}); err != nil {
		return &Report{}, err
	}

	// cancelTurn is set while a turn runs.
	var cancelTurn workflow.CancelFunc
	messages := workflow.GetSignalChannel(ctx, SignalMessage)
	stops := workflow.GetSignalChannel(ctx, SignalStop)
	dequeues := workflow.GetSignalChannel(ctx, SignalDequeue)
	closes := workflow.GetSignalChannel(ctx, SignalClose)
	children := workflow.GetSignalChannel(ctx, SignalReport)
	escalations := workflow.GetSignalChannel(ctx, SignalEscalation)
	addMessage := func(message *Message) {
		if message.GetId() != "" && message.GetText() != "" {
			state.Pending = append(state.Pending, message)
		}
	}
	// takeNotice applies what an escalation told the chat. A person's answer
	// is a message like any other, and the turn it starts continues the chat.
	takeNotice := func(notice *EscalationNotice) {
		if notice == nil {
			return
		}
		if message, ok := takeEscalation(&state, notice); ok {
			addMessage(message)
		}
	}
	stopTurn := func(ctx workflow.Context, action *Action) {
		state.StoppedBy = action.GetBy()
		// A stop ends the running turn only. The messages that wait are
		// dropped when the person asks for that.
		if action.GetDiscard() || workflow.GetVersion(ctx, versionStopKeepsQueue, workflow.DefaultVersion, 1) == workflow.DefaultVersion {
			state.Pending = nil
		}
		if cancelTurn != nil {
			cancelTurn()
		}
	}
	dequeue := func(request *Dequeue) {
		state.Pending = slices.DeleteFunc(state.Pending, func(message *Message) bool { return message.Id == request.GetId() })
	}
	closeChat := func(action *Action) {
		if !state.Closing {
			state.ClosedBy = action.GetBy()
		}
		state.Closing = true
		if cancelTurn != nil {
			cancelTurn()
		}
	}
	// Signals are taken as they arrive, also while the session opens or a
	// turn runs, so the state always shows what waits.
	workflow.Go(ctx, func(ctx workflow.Context) {
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(messages, func(channel workflow.ReceiveChannel, _ bool) {
			var message *Message
			channel.Receive(ctx, &message)
			addMessage(message)
		})
		selector.AddReceive(stops, func(channel workflow.ReceiveChannel, _ bool) {
			var action *Action
			channel.Receive(ctx, &action)
			stopTurn(ctx, action)
		})
		selector.AddReceive(dequeues, func(channel workflow.ReceiveChannel, _ bool) {
			var request *Dequeue
			channel.Receive(ctx, &request)
			dequeue(request)
		})
		selector.AddReceive(children, func(channel workflow.ReceiveChannel, _ bool) {
			var result *Report
			channel.Receive(ctx, &result)
			for index, child := range state.Children {
				if child.Id == result.GetChatId() && child.Message == result.GetMessage() && result.GetAttention() && !child.Done {
					state.Children[index].Attention = true
					state.Children[index].Result = result
				}
			}
		})
		selector.AddReceive(escalations, func(channel workflow.ReceiveChannel, _ bool) {
			var notice *EscalationNotice
			channel.Receive(ctx, &notice)
			takeNotice(notice)
		})
		selector.AddReceive(closes, func(channel workflow.ReceiveChannel, _ bool) {
			var action *Action
			channel.Receive(ctx, &action)
			closeChat(action)
		})
		cancelled := false
		selector.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) { cancelled = true })
		for !cancelled {
			selector.Select(ctx)
		}
		state.Closing = true
	})

	if state.session == nil {
		state.Status = StatusOpening
		state.Request = withPool(state.Request)
		session, err := p7.OpenSession(ctx, sessionSpec(state.Request, chatTools(&state)))
		if err != nil {
			state.Status, state.Error = StatusFailed, Cause(err)
			report(ctx, state, &Report{Error: state.Error, Idle: true, Ended: true})
			return &Report{}, err
		}
		state.session = session
		state.syncSession()
	}

	for !state.Closing {
		state.Status = idleStatus(&state)
		if liveChildren(&state) == 0 && upgrade.Due(ctx) {
			// No turn runs, so no tool call is expected. One that is still
			// being answered finishes first, or the agent would get no result.
			if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
				break
			}
			// Take what the server already delivered, so no signal is lost
			// between this run and the next.
			// Each value is decoded into a fresh variable: a payload with a
			// field left out would otherwise keep the previous value.
			for {
				var action *Action
				if !stops.ReceiveAsync(&action) {
					break
				}
				stopTurn(ctx, action)
			}
			for {
				var request *Dequeue
				if !dequeues.ReceiveAsync(&request) {
					break
				}
				dequeue(request)
			}
			for {
				var message *Message
				if !messages.ReceiveAsync(&message) {
					break
				}
				addMessage(message)
			}
			for {
				var notice *EscalationNotice
				if !escalations.ReceiveAsync(&notice) {
					break
				}
				takeNotice(notice)
			}
			for {
				var action *Action
				if !closes.ReceiveAsync(&action) {
					break
				}
				closeChat(action)
			}
			if !state.Closing {
				// Auto-upgrade when a newer build is the target, so that the
				// next run is on it and this build can drain.
				return &Report{}, upgrade.ContinueAsNew(ctx, WorkflowName, state.ChatState)
			}
			break
		}
		waiting := func() bool { return state.Closing || len(state.Pending) > 0 }
		if workflow.GetVersion(ctx, versionUpgradeWake, workflow.DefaultVersion, 1) >= 1 {
			woken, err := workflow.AwaitWithTimeout(ctx, upgradeWake, waiting)
			if err != nil || state.Closing {
				break
			}
			if !woken {
				// Nothing came: back to the top, where a newer target
				// build or a suggestion to continue as new is acted on.
				continue
			}
		} else if err := workflow.Await(ctx, waiting); err != nil || state.Closing {
			break
		}

		message := state.Pending[0]
		state.Pending = state.Pending[1:]
		state.Current, state.Status = message, StatusRunning
		if message.From != nil {
			if state.Senders == nil {
				state.Senders = map[string]string{}
			}
			state.Senders[message.Id] = message.From.Login
		}
		prompt, err := turnPrompt(ctx, &state, message)
		var outcome turnOutcome
		if err == nil {
			turnCtx, cancel := workflow.WithCancel(ctx)
			cancelTurn = cancel
			state.Attention = false
			outcome, err = runTurn(turnCtx, &state, message, prompt)
			// A turn may move the session to another worker of its pool.
			state.syncSession()
			cancelTurn = nil
			cancel()
		}
		turn := outcome.Turn

		state.Current = nil
		state.Turns++
		switch {
		case err == nil:
			state.Briefed = true
			state.LastStop, state.Error = turn.StopReason, ""
			if outcome.GaveUp {
				state.Error = giveUpError(turn)
			}
		case temporal.IsCanceledError(err):
			// The stop came before the turn started, so there is no turn
			// result. The message was not sent.
			state.LastStop, state.Error = p7.StopCancelled, ""
		default:
			// The chat stays open: the next message may well run.
			state.LastStop, state.Error = "", Cause(err)
		}
		if state.Request.Escalation != nil && state.Request.Parent != "" && message.From == nil {
			state.Attention = true
		}
		summary := &Report{ChatId: state.Request.Id, Attention: state.Attention, Message: message.Id, StopReason: state.LastStop, Text: turn.Text, Error: state.Error, Idle: len(state.Pending) == 0, GaveUp: outcome.GaveUp, Resumes: state.Resumes}
		if state.LastStop == p7.StopCancelled && !state.Closing {
			// Only a person's stop cancels a turn while the chat stays open.
			summary.Stopped = true
			if state.StoppedBy != nil {
				summary.StoppedBy = state.StoppedBy.Login
			}
		}
		state.Result = summary
		report(ctx, state, state.Result)
		// A child that waits for an answer to an escalation is not done.
		if state.Request.Parent != "" && !state.Attention && len(state.Pending) == 0 && openEscalations(&state) == 0 {
			state.Closing = true
		}
	}

	state.Status = StatusClosing
	state.Pending = nil
	// The chat may be ending because the workflow was cancelled. The
	// session is closed all the same.
	closeCtx, _ := workflow.NewDisconnectedContext(ctx)
	closeChildren(closeCtx, &state)
	withdrawEscalations(closeCtx, &state)
	// Even a ready timed await consumes command IDs, so historical roots must skip it.
	if state.Request.Parent != "" || len(state.Children) > 0 {
		_, _ = workflow.AwaitWithTimeout(closeCtx, 30*time.Second, func() bool { return workflow.AllHandlersFinished(closeCtx) })
	}
	if err := state.session.Close(closeCtx); err != nil {
		state.Status, state.Error = StatusFailed, Cause(err)
		report(closeCtx, state, &Report{Error: state.Error, Idle: true, Ended: true})
		return &Report{}, err
	}
	state.Status = StatusClosed
	report(closeCtx, state, &Report{Idle: true, Ended: true})
	// A chat closed before its first turn has no result yet.
	if state.Result == nil {
		state.Result = &Report{ChatId: state.Request.Id}
	}
	state.Result.Ended = true
	state.Result.Idle = true
	return state.Result, ctx.Err()
}

// turnPrompt is the prompt of the turn that answers message: its text, and a
// hidden context of the instructions and brief on the first turn, the picks,
// a person's guidance and the app's turn context.
func turnPrompt(ctx workflow.Context, state *State, message *Message) (p7.TextPrompt, error) {
	prompt := p7.TextPrompt{Text: message.Text, TurnID: message.Id, Retry: turnRetry}
	if !state.Briefed {
		first, err := firstContext(ctx, state)
		if err != nil {
			return prompt, err
		}
		prompt.Context = first
	}
	if picks := pickContext(message.Picks); picks != "" {
		prompt.Context = strings.TrimSpace(prompt.Context + "\n\n" + picks)
	}
	if guidance := guidanceContext(state.Request, message); guidance != "" {
		prompt.Context = strings.TrimSpace(prompt.Context + "\n\n" + guidance)
	}
	if extend := optionsOf(state).TurnContext; extend != nil {
		if extra := extend(ctx, &Chat{State: state}, message); extra != "" {
			prompt.Context = strings.TrimSpace(prompt.Context + "\n\n" + extra)
		}
	}
	return prompt, nil
}

// report tells the chat's owner, if it has one, what happened. An owner that
// has ended is not an error of the chat.
func report(ctx workflow.Context, state State, value *Report) {
	if state.Request.Owner == "" {
		return
	}
	if value.ChatId == "" {
		// The reports of a chat that failed to open or that closed name it
		// too: an owner that runs several chats tells them apart by it.
		value.ChatId = state.Request.Id
	}
	_ = workflow.SignalExternalWorkflow(ctx, state.Request.Owner, "", SignalReport, value).Get(ctx, nil)
}

// Cause is what a person is told about a failed step: the message of the
// error the activity returned, without Temporal's description of the activity.
func Cause(err error) string {
	if application, ok := errors.AsType[*temporal.ApplicationError](err); ok {
		return application.Message()
	}
	return err.Error()
}

// turnRetry keeps p7's ten attempts, which carry a turn across a worker
// restart, but reports a harness that cannot start at once: the chat stays
// open and the person sends the message again.
var turnRetry = &temporal.RetryPolicy{
	InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumInterval: time.Minute, MaximumAttempts: 10,
	NonRetryableErrorTypes: []string{p7.ErrorHarnessStopped},
}

// Placement is where a session runs: in a pod of its own on the kubernetes
// backend, and otherwise as a process on the host's core worker. The chat,
// the review and the issue work place their sessions with it.
func Placement(backend, host string) p7.Placement {
	if backend == p7.BackendKubernetes {
		return p7.Placement{Backend: p7.BackendKubernetes}
	}
	return p7.Placement{Backend: p7.BackendHost, Host: host}
}

func sessionSpec(request *Request, tools []p7.Tool) p7.SessionSpec {
	spec := p7.SessionSpec{
		Models:    request.Models,
		Placement: Placement(request.Backend, request.Host),
		Tools:     tools,
		// A person waits for the chat to open. A clone or a harness start
		// that fails three times within six seconds is reported, not
		// retried for minutes.
		OpenRetry: &temporal.RetryPolicy{InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumAttempts: 3},
		// A chat is often left for a while. Its processes stop after this
		// long with no turn, and the next message starts them again.
		IdleLimit: 30 * time.Minute,
	}
	spec.Pool = poolSpec(request)
	// Only what the scope reaches is cloned, whatever the request lists.
	for _, repository := range ScopedRepositories(request.Repositories, request.Scope) {
		spec.Inputs = append(spec.Inputs, p7.Input{
			Source:      p7.GitClone{Repository: repository.Url, Branch: Branch(request.Id)},
			Destination: "/" + repository.Name,
		})
	}
	return spec
}
