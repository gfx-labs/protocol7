package chat

import (
	"fmt"
	"strings"
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	// EscalationWorkflowName is the registered name of the escalation workflow.
	EscalationWorkflowName = navi.EscalationWorkflow

	// SignalEscalation is what an escalation sends the chat that raised it,
	// on each change of its state. It carries an EscalationNotice.
	SignalEscalation = navi.SignalEscalation
	// SignalEscalationHandling is sent by the escalation handler when it
	// takes an open escalation. It carries an EscalationClaim.
	SignalEscalationHandling = navi.SignalEscalationHandling
	// SignalEscalationForward is sent by the escalation handler when it
	// gives an escalation to a person. It carries an EscalationForward.
	SignalEscalationForward = navi.SignalEscalationForward
	// UpdateEscalationResolve answers an escalation. It carries an
	// EscalationAnswer, and is refused when the escalation is resolved
	// already or the answer is empty.
	UpdateEscalationResolve = navi.UpdateEscalationResolve
	// UpdateEscalationDecide is the escalation handler's decision: resolved,
	// forwarded or declined. It carries an EscalationDecision and is refused
	// unless the escalation is open or being handled.
	UpdateEscalationDecide = navi.UpdateEscalationDecide
	// UpdateEscalationReview is a person's review of what the handler
	// decided: an override of its resolution or its refusal, with a new
	// answer for the chat, or an acknowledgement of a refusal. It carries an
	// EscalationReview.
	UpdateEscalationReview = navi.UpdateEscalationReview
	// QueryEscalation returns the escalation's EscalationState.
	QueryEscalation = navi.QueryEscalation

	// MemoReason is the memo field the page lists an escalation's reason
	// from.
	MemoReason = navi.MemoReason
	// MemoOutcome is the memo field that says what the handler decided, once
	// it did: resolved, forwarded or declined, with " (reviewed)" after a
	// person reviewed it.
	MemoOutcome = navi.MemoOutcome

	// HandlerLogin names the escalation handler where a person's login would
	// stand, on an answer.
	HandlerLogin = "escalation-handler"

	// handlerDeadline is how long an escalation may wait for the handler to
	// decide before it is forwarded to a person on its own.
	handlerDeadline = 45 * time.Minute
	// reviewWindow is how long the escalation of a resolution or a refusal
	// that the handler made stays open for a person to override it.
	reviewWindow = 7 * 24 * time.Hour
	// versionEscalationHandler gates the hand-off to the handler: an
	// escalation that started before it was forwarded at once.
	versionEscalationHandler = "escalation-handler"

	// EscalationFieldLimit bounds each text of an escalation, in bytes. A
	// probe's plan may be long.
	EscalationFieldLimit = 64 << 10
	// MaxOpenEscalations bounds the escalations of one chat that wait at
	// once, and MaxEscalations those it may raise in all, so that the chat's
	// state stays small.
	MaxOpenEscalations = 4
	MaxEscalations     = 16
)

// EscalationWorkflowID is the workflow ID of an escalation.
func EscalationWorkflowID(id string) string { return navi.EscalationWorkflowID(id) }

// EscalationStatus is the state of an escalation. SelfdevEscalationState
// says it, so Temporal's search attributes are the record.
type EscalationStatus = navi.EscalationStatus

// States of an escalation.
const (
	// EscalationOpen is a new escalation that nobody took.
	EscalationOpen = navi.EscalationOpen
	// EscalationHandling is an escalation that the handler works on.
	EscalationHandling = navi.EscalationHandling
	// EscalationForwarded is an escalation that waits for a person: the
	// handler gave it up, or there is no handler. It shows in navi's list of
	// what asks people.
	EscalationForwarded = navi.EscalationForwarded
	// EscalationResolved is an answered escalation, or one that the chat
	// withdrew by ending.
	EscalationResolved = navi.EscalationResolved
	// EscalationDeclined is an escalation that the handler refused: the chat
	// was told why and a person is notified. It waits in navi's list for a
	// person to acknowledge it or to override it with an answer.
	EscalationDeclined = navi.EscalationDeclined
)

// Outcome is what the handler decided about an escalation.
type Outcome = navi.Outcome

// Outcomes, one for each escalation.
const (
	// OutcomeResolved: the handler answered the chat.
	OutcomeResolved = navi.OutcomeResolved
	// OutcomeForwarded: a person has to act, and the handler wrote what for.
	OutcomeForwarded = navi.OutcomeForwarded
	// OutcomeDeclined: the request should not be done. The chat was told why.
	OutcomeDeclined = navi.OutcomeDeclined
)

// EscalationDecision is what the handler decided and why. A person sees it
// on the escalation in navi.
type EscalationDecision = navi.EscalationDecision

// reviewable says the handler resolved or declined the escalation, which a
// person may review.
func reviewable(d *EscalationDecision) bool {
	return d != nil && (d.Outcome == OutcomeResolved || d.Outcome == OutcomeDeclined)
}

// EscalationReview is a person's review of a decision of the handler.
type EscalationReview = navi.EscalationReview

// EscalationRequest is what a chat asks for.
type EscalationRequest = navi.EscalationRequest

// EscalationSession is what the handler's chat takes from the chat it serves.
type EscalationSession = navi.EscalationSession

// EscalationAnswer answers an escalation.
type EscalationAnswer = navi.EscalationAnswer

// EscalationClaim is the payload of SignalEscalationHandling.
type EscalationClaim = navi.EscalationClaim

// EscalationForward is the payload of SignalEscalationForward.
type EscalationForward = navi.EscalationForward

// EscalationState is everything the escalation workflow keeps.
type EscalationState = navi.EscalationState

// EscalationRef is what a chat keeps of an escalation it raised.
type EscalationRef = navi.EscalationRef

// EscalationNotice is the payload of SignalEscalation: the escalation's new
// state and, once resolved, the answer.
type EscalationNotice = navi.EscalationNotice

// EscalationSearchAttributes are the search attributes an escalation
// workflow is started with. The state changes with the workflow.
func EscalationSearchAttributes(request *EscalationRequest) temporal.SearchAttributes {
	updates := []temporal.SearchAttributeUpdate{
		AttributeKind.ValueSet(string(KindEscalation)),
		AttributeParent.ValueSet(request.Chat),
		AttributeEscalationState.ValueSet(string(EscalationOpen)),
	}
	if request.StartedBy != nil {
		updates = append(updates, AttributeStartedBy.ValueSet(request.StartedBy.Login))
	}
	if request.Issue != nil {
		updates = append(updates, AttributeIssue.ValueSet(fmt.Sprintf("%s#%d", request.Issue.Repo, request.Issue.Number)))
	}
	if request.Project != "" {
		updates = append(updates, AttributeProject.ValueSet(request.Project))
	}
	return temporal.NewSearchAttributes(updates...)
}

// EscalationMemo is the memo an escalation workflow is started with: what
// the page lists it by.
func EscalationMemo(request *EscalationRequest) map[string]any {
	return map[string]any{MemoTitle: request.Title, MemoReason: oneLine(request.Why, 200)}
}

// handoffOptions are those of the hand-off to the app's handler, which an
// escalation waits for.
var handoffOptions = workflow.ActivityOptions{
	TaskQueue:           TaskQueue,
	StartToCloseTimeout: 30 * time.Second,
	RetryPolicy:         &temporal.RetryPolicy{InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumAttempts: 3},
}

// Validate refuses a request that lacks what a person needs to act on it.
func validateEscalation(r *EscalationRequest) error {
	for name, text := range map[string]string{"need": r.Need, "why": r.Why, "tried": r.Tried, "next": r.Next} {
		if len(text) > EscalationFieldLimit {
			return fmt.Errorf("%s is longer than %d bytes", name, EscalationFieldLimit)
		}
	}
	if strings.TrimSpace(r.Need) == "" || strings.TrimSpace(r.Why) == "" {
		return fmt.Errorf("an escalation says what is needed and why this chat cannot get it itself")
	}
	return nil
}

// oneLine is the first line of text, trimmed and cut to limit characters.
func oneLine(text string, limit int) string {
	line, _, _ := strings.Cut(strings.TrimSpace(text), "\n")
	runes := []rune(strings.TrimSpace(line))
	if len(runes) > limit {
		return strings.TrimSpace(string(runes[:limit])) + "..."
	}
	return string(runes)
}

// runEscalation is one escalation: a chat asked for something outside its
// reach. It is handed to the app's escalation handler, if the app has one
// (Options.EscalationHandoff), which resolves it, forwards it to a person or
// declines it. Without one it goes to a person. It waits for what a person has
// to say, and sends the answer to the chat.
//
// A new escalation is open. The handler takes it with SignalEscalationHandling
// (handling) and decides with UpdateEscalationDecide. When the handler gives
// it up (SignalEscalationForward), fails to take it, or does not decide within
// handlerDeadline, it is forwarded: it shows in navi's list of what asks
// people, and a person's answer resolves it. A resolution or a refusal of the
// handler stays open for reviewWindow, so that a person can override it.
//
// handoff names the activity that gives the escalation to the app's handler.
// Empty forwards it to a person at once. validateDecision, when set, refuses a
// decision of the handler the app does not accept.
func runEscalation(ctx workflow.Context, state *EscalationState, handoff string, validateDecision func(*EscalationDecision) error) (*EscalationState, error) {
	if state == nil {
		state = &EscalationState{}
	}
	if err := workflow.SetQueryHandler(ctx, QueryEscalation, func() (*EscalationState, error) { return state, nil }); err != nil {
		return state, err
	}
	if state.Status == "" {
		state.Status, state.Raised, state.Changed = EscalationOpen, navi.Timestamp(workflow.Now(ctx)), navi.Timestamp(workflow.Now(ctx))
	}
	chatID := WorkflowID(state.Request.Chat)
	// tell sends the chat what changed. The chat having ended is no concern of
	// the escalation.
	tell := func(ctx workflow.Context, notice *EscalationNotice) error {
		return workflow.SignalExternalWorkflow(ctx, chatID, "", SignalEscalation, notice).Get(ctx, nil)
	}
	// setState records the state in the search attribute, which navi lists by.
	setState := func(ctx workflow.Context, status EscalationStatus) error {
		state.Status, state.Changed = status, navi.Timestamp(workflow.Now(ctx))
		return workflow.UpsertTypedSearchAttributes(ctx, AttributeEscalationState.ValueSet(string(status)))
	}
	// move changes the state, in the search attribute and in the chat.
	move := func(ctx workflow.Context, status EscalationStatus, answer *EscalationAnswer) error {
		if err := setState(ctx, status); err != nil {
			return err
		}
		err := tell(ctx, &EscalationNotice{Id: state.Request.Id, Status: status, Answer: answer})
		if status == EscalationResolved || status == EscalationDeclined {
			state.Delivered = err == nil
		}
		return nil
	}
	// remember puts the handler's outcome in the memo, which the list shows.
	remember := func(ctx workflow.Context) error {
		if state.Decision == nil {
			return nil
		}
		outcome := string(state.Decision.Outcome)
		if reviewed(state) {
			outcome += " (reviewed)"
		}
		return workflow.UpsertMemo(ctx, map[string]any{MemoOutcome: outcome})
	}

	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateEscalationResolve, func(ctx workflow.Context, answer *EscalationAnswer) (*EscalationState, error) {
		answer.Text = strings.TrimSpace(answer.Text)
		state.Answer = answer
		// The state is set before the chat is told, so that a second answer
		// that comes while this one waits for the chat is refused.
		if err := move(ctx, EscalationResolved, answer); err != nil {
			return state, err
		}
		return state, nil
	}, workflow.UpdateHandlerOptions{Validator: func(_ workflow.Context, answer *EscalationAnswer) error {
		switch {
		case state.Status == EscalationResolved:
			return fmt.Errorf("this escalation is resolved already")
		case state.Status == EscalationDeclined:
			return fmt.Errorf("the handler declined this escalation: review it instead of answering")
		case strings.TrimSpace(answer.Text) == "":
			return fmt.Errorf("the answer is empty")
		case len(answer.Text) > EscalationFieldLimit:
			return fmt.Errorf("the answer is longer than %d bytes", EscalationFieldLimit)
		}
		return nil
	}}); err != nil {
		return state, err
	}

	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateEscalationDecide, func(ctx workflow.Context, decision *EscalationDecision) (*EscalationState, error) {
		decision.At = navi.Timestamp(workflow.Now(ctx))
		decision.Text, decision.Reasoning = strings.TrimSpace(decision.Text), strings.TrimSpace(decision.Reasoning)
		state.Decision = decision
		if state.Handler == "" {
			state.Handler = decision.Handler
		}
		var err error
		switch decision.Outcome {
		case OutcomeResolved, OutcomeDeclined:
			answer := &EscalationAnswer{Text: decision.Text, By: &Person{Login: HandlerLogin}, Restricted: state.Request.Session != nil && state.Request.Session.Restricted}
			state.Answer = answer
			status := EscalationResolved
			if decision.Outcome == OutcomeDeclined {
				status = EscalationDeclined
			}
			err = move(ctx, status, answer)
		default:
			state.Note = decision.Text
			err = move(ctx, EscalationForwarded, nil)
		}
		if err != nil {
			return state, err
		}
		return state, remember(ctx)
	}, workflow.UpdateHandlerOptions{Validator: func(_ workflow.Context, decision *EscalationDecision) error {
		switch {
		case state.Status != EscalationOpen && state.Status != EscalationHandling:
			return fmt.Errorf("this escalation is %s already", state.Status)
		case decision.Outcome != OutcomeResolved && decision.Outcome != OutcomeForwarded && decision.Outcome != OutcomeDeclined:
			return fmt.Errorf("the outcome is resolved, forwarded or declined")
		case strings.TrimSpace(decision.Text) == "":
			return fmt.Errorf("a decision says what the answer, the summary or the reason is")
		case strings.TrimSpace(decision.Reasoning) == "":
			return fmt.Errorf("a decision gives its reasoning, which a person reads")
		case len(decision.Text) > EscalationFieldLimit || len(decision.Reasoning) > EscalationFieldLimit:
			return fmt.Errorf("the decision is longer than %d bytes", EscalationFieldLimit)
		}
		if validateDecision != nil {
			return validateDecision(decision)
		}
		return nil
	}}); err != nil {
		return state, err
	}

	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateEscalationReview, func(ctx workflow.Context, review *EscalationReview) (*EscalationState, error) {
		review.Text, review.At = strings.TrimSpace(review.Text), navi.Timestamp(workflow.Now(ctx))
		state.Review = review
		if state.Status == EscalationDeclined {
			if err := setState(ctx, EscalationResolved); err != nil {
				return state, err
			}
		}
		if review.Text != "" {
			// The person's answer replaces the handler's for the chat.
			answer := &EscalationAnswer{Text: review.Text, By: review.By, Restricted: review.Restricted}
			err := tell(ctx, &EscalationNotice{Id: state.Request.Id, Status: EscalationResolved, Answer: answer, Override: true})
			state.Delivered = err == nil
		}
		return state, remember(ctx)
	}, workflow.UpdateHandlerOptions{Validator: func(_ workflow.Context, review *EscalationReview) error {
		switch {
		case !reviewable(state.Decision) || state.Status != EscalationResolved && state.Status != EscalationDeclined:
			return fmt.Errorf("the handler resolved or declined nothing here, so there is nothing to review")
		case reviewed(state):
			return fmt.Errorf("a person reviewed this escalation already")
		case strings.TrimSpace(review.Text) == "" && state.Status == EscalationResolved:
			return fmt.Errorf("say what the chat should do instead: the handler's answer reached it already")
		case len(review.Text) > EscalationFieldLimit:
			return fmt.Errorf("the answer is longer than %d bytes", EscalationFieldLimit)
		}
		return nil
	}}); err != nil {
		return state, err
	}

	// The handler's signals are taken as they arrive.
	handling := workflow.GetSignalChannel(ctx, SignalEscalationHandling)
	forwards := workflow.GetSignalChannel(ctx, SignalEscalationForward)
	// giveUp forwards an escalation that the handler did not decide.
	giveUp := func(ctx workflow.Context, note, reasoning string) {
		if state.Status != EscalationOpen && state.Status != EscalationHandling {
			return
		}
		state.Decision = &EscalationDecision{Outcome: OutcomeForwarded, Text: note, Reasoning: reasoning, Handler: state.Handler, At: navi.Timestamp(workflow.Now(ctx))}
		_ = forward(ctx, state, note, move)
		_ = remember(ctx)
	}
	workflow.Go(ctx, func(ctx workflow.Context) {
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(handling, func(channel workflow.ReceiveChannel, _ bool) {
			var taken *EscalationClaim
			channel.Receive(ctx, &taken)
			// Only an open escalation can be taken.
			if state.Status == EscalationOpen {
				state.Handler = taken.GetHandler()
				_ = move(ctx, EscalationHandling, nil)
			}
		})
		selector.AddReceive(forwards, func(channel workflow.ReceiveChannel, _ bool) {
			var given *EscalationForward
			channel.Receive(ctx, &given)
			giveUp(ctx, given.GetNote(), given.GetReasoning())
		})
		cancelled := false
		selector.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) { cancelled = true })
		for !cancelled && state.Status != EscalationResolved {
			selector.Select(ctx)
		}
	})

	if state.Status == EscalationOpen {
		if workflow.GetVersion(ctx, versionEscalationHandler, workflow.DefaultVersion, 1) == workflow.DefaultVersion || handoff == "" {
			// Before the handler, and in an app that has none, an escalation
			// goes straight to a person.
			if err := forward(ctx, state, "", move); err != nil {
				return state, err
			}
		} else {
			// The handler decides within its time, or a person gets the escalation.
			workflow.Go(ctx, func(ctx workflow.Context) {
				if err := workflow.Sleep(ctx, handlerDeadline); err == nil {
					giveUp(ctx, "The escalation handler did not decide within "+handlerDeadline.String()+".", "The handler's deadline passed with the escalation still open.")
				}
			})
			handoffCtx := workflow.WithActivityOptions(ctx, handoffOptions)
			if err := workflow.ExecuteActivity(handoffCtx, handoff, state.Request).Get(ctx, nil); err != nil {
				giveUp(ctx, "The escalation handler could not take this escalation: "+Cause(err)+".", "The hand-off to the handler failed, so the handler never saw it.")
			}
		}
	}

	decided := func() bool { return state.Status == EscalationResolved || state.Status == EscalationDeclined }
	if err := workflow.Await(ctx, decided); err != nil {
		// The chat ended, and with it the question. The escalation is closed
		// as resolved, with no answer, so that it leaves the list.
		if state.Status != EscalationResolved {
			closeCtx, _ := workflow.NewDisconnectedContext(ctx)
			state.Withdrawn = true
			if err := setState(closeCtx, EscalationResolved); err != nil {
				return state, err
			}
		}
		return state, nil
	}
	// A resolution or a refusal of the handler waits for a person's review.
	if reviewable(state.Decision) && !reviewed(state) {
		if _, err := workflow.AwaitWithTimeout(ctx, reviewWindow, func() bool { return reviewed(state) }); err != nil {
			return state, nil
		}
	}
	// The answer reaches the chat before the workflow ends.
	if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
		return state, err
	}
	return state, nil
}

// Reviewed says a person reviewed the handler's decision.
func reviewed(s *EscalationState) bool { return s.Review != nil }

// forward gives the escalation to a person.
func forward(ctx workflow.Context, state *EscalationState, note string, move func(workflow.Context, EscalationStatus, *EscalationAnswer) error) error {
	state.Note = note
	return move(ctx, EscalationForwarded, nil)
}

// answerMessage is the message that carries an answer to the chat. Its ID
// is the escalation's, so that a signal that came twice is taken once.
func answerMessage(ref *EscalationRef, answer *EscalationAnswer) *Message {
	who := "a person"
	if answer.By != nil && answer.By.Login == HandlerLogin {
		who = "the escalation handler (an agent, not a person)"
	} else if answer.By != nil && answer.By.Login != "" {
		who = answer.By.Login
	}
	text := fmt.Sprintf("Answer to your escalation %s (%q), from %s:\n\n%s\n\nGo on with what you said you would do once unblocked.", ref.Id, ref.Title, who, answer.Text)
	return &Message{Id: "escalation-" + ref.Id, Text: text, Restricted: answer.Restricted}
}

// declinedMessage tells the chat that its escalation was declined. It gives
// the chat no new permission and no way round the refusal.
func declinedMessage(ref *EscalationRef, answer *EscalationAnswer) *Message {
	text := fmt.Sprintf("Your escalation %s (%q) was declined by the escalation handler, because the request should not be done:\n\n%s\n\nDo not do what it asked for, and do not escalate the same request again. A person has been told. Go on with what you can do within your own reach, and say in your last message of the turn what is left undone.", ref.Id, ref.Title, answer.Text)
	return &Message{Id: "escalation-" + ref.Id, Text: text, Restricted: answer.Restricted}
}

// overrideMessage tells the chat that a person replaced the handler's decision.
func overrideMessage(ref *EscalationRef, answer *EscalationAnswer) *Message {
	who := "a person"
	if answer.By != nil && answer.By.Login != "" {
		who = answer.By.Login
	}
	text := fmt.Sprintf("%s reviewed the escalation handler's decision on your escalation %s (%q) and overrides it. What they say replaces the handler's answer:\n\n%s\n\nGo on from that.", who, ref.Id, ref.Title, answer.Text)
	return &Message{Id: "escalation-" + ref.Id + "-override", Text: text, Restricted: answer.Restricted}
}

// openEscalations counts the escalations of the chat that wait.
func openEscalations(state *State) int {
	count := 0
	for _, ref := range state.Escalations {
		if navi.EscalationWaiting(ref.Status) {
			count++
		}
	}
	return count
}

// idleStatus is what a chat with no turn to run is: idle, or escalated while
// it waits for an answer.
func idleStatus(state *State) Status {
	if openEscalations(state) > 0 {
		return StatusEscalated
	}
	return StatusIdle
}

// takeEscalation applies what an escalation told the chat. The answer comes
// as a message, which the chat takes like any other. It returns the message
// to queue, if any.
func takeEscalation(state *State, notice *EscalationNotice) (*Message, bool) {
	for index, ref := range state.Escalations {
		if ref.Id != notice.Id {
			continue
		}
		if notice.Override {
			// A person's review of the handler's decision. It reaches the chat
			// once, whatever the chat does meanwhile.
			if ref.Overridden || navi.EscalationWaiting(ref.Status) || notice.Answer == nil {
				return nil, false
			}
			state.Escalations[index].Overridden = true
			return overrideMessage(state.Escalations[index], notice.Answer), true
		}
		if !navi.EscalationWaiting(ref.Status) {
			// A signal that came again.
			return nil, false
		}
		state.Escalations[index].Status = notice.Status
		if state.Status == StatusIdle || state.Status == StatusEscalated {
			state.Status = idleStatus(state)
		}
		if notice.Answer == nil {
			return nil, false
		}
		switch notice.Status {
		case EscalationResolved:
			return answerMessage(state.Escalations[index], notice.Answer), true
		case EscalationDeclined:
			return declinedMessage(state.Escalations[index], notice.Answer), true
		}
		return nil, false
	}
	return nil, false
}
