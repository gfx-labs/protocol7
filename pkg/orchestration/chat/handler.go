package chat

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/orchestration/upgrade"
	"github.com/gfx-labs/protocol7/pkg/support/ids"
)

// The escalation handler decides what happens to an escalation before it
// reaches a person.
//
// One long-lived workflow, HandlerWorkflow, runs in each project's namespace.
// An escalation workflow hands its request to it (ActivityEscalationHandoff,
// which signals the handler workflow and starts it if it does not run). For
// each escalation the handler workflow starts a chat of its own, the handler
// chat, given the escalation, the recent state of the chat that raised it, its
// issue and its repositories. That chat decides with one of three tools:
// escalation_resolve (answer the chat and record a learning),
// escalation_forward (write a summary for a person) or escalation_decline
// (refuse, tell the chat why, notify a person). It has one turn to do so. A
// turn that ends with no decision, a failure and a timeout are all a forward.
//
// The handler has the reach of the chat it serves and no more: its chat is
// started with that chat's host, backend, repositories, project, restriction
// and issue, has no tool that escalates, spawns chats or runs probes, and
// nothing in it grants a permission.
const (
	// HandlerWorkflowName is the registered name of the handler workflow, and
	// HandlerWorkflowID the ID of the one that runs in a namespace.
	HandlerWorkflowName = navi.HandlerWorkflow
	HandlerWorkflowID   = "escalation-handler"
	// SignalEscalationIntake hands the handler workflow a HandlerIntake.
	SignalEscalationIntake = navi.SignalEscalationIntake
	// QueryHandler returns the handler workflow's HandlerState.
	QueryHandler = "state"

	// MaxHandlers bounds the handler chats that run at once. The others wait.
	MaxHandlers = 4
	// handlerTurnBudget is how long the handler's chat has to decide.
	handlerTurnBudget = 30 * time.Minute
	// handlerChatTimeout is the longest a handler chat lives, whatever happens.
	handlerChatTimeout = 2 * time.Hour
	// handlerWake is how often an idle handler workflow looks for a newer
	// worker build.
	handlerWake = time.Hour
	// maxResolvedRecords and maxSeen bound what the handler workflow
	// remembers, so that its state stays small.
	maxResolvedRecords = 200
	maxSeen            = 500
	// repeatSimilarity is how alike two escalations of a chat must be, as the
	// overlap of their words, to count as the same thing.
	repeatSimilarity = 0.6
)

// HandlerIntake is what an escalation hands the handler.
type HandlerIntake struct {
	Request EscalationRequest `json:"request"`
	// Models is the handler's own model choice (--handler-model), and Model
	// its name. Empty uses the models of the chat it serves.
	Models []string `json:"models,omitempty"`
	Model  string   `json:"model,omitempty"`
}

// ResolvedRecord is what the handler remembers of an escalation it resolved
// or declined, for the loop guard.
type ResolvedRecord struct {
	Chat       string    `json:"chat"`
	Escalation string    `json:"escalation"`
	Outcome    Outcome   `json:"outcome"`
	Words      []string  `json:"words"`
	At         time.Time `json:"at"`
}

// HandlerState is everything the handler workflow keeps. It is the workflow's
// input, so it is also what one run hands to the next.
type HandlerState struct {
	// Queue holds the escalations that wait for a handler chat.
	Queue []HandlerIntake `json:"queue,omitempty"`
	// Seen holds the IDs of the escalations taken, so that a hand-off that
	// came twice is handled once.
	Seen []string `json:"seen,omitempty"`
	// Resolved is what the loop guard compares a new escalation with.
	Resolved []ResolvedRecord `json:"resolved,omitempty"`
	// Active counts the handler chats that run.
	Active int `json:"active,omitempty"`
	// Handled counts the escalations the handler took in all.
	Handled int `json:"handled,omitempty"`
}

// words is the set of words of an escalation's need, which the loop guard
// compares: lower case, letters and digits, at least three of them.
func words(text string) []string {
	var found []string
	for _, word := range strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
	}) {
		if len(word) >= 3 {
			found = append(found, word)
		}
	}
	slices.Sort(found)
	return slices.Compact(found)
}

// similarity is the overlap of two sorted word sets: the shared words over
// all words.
func similarity(a, b []string) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	shared := 0
	for _, word := range a {
		if _, ok := slices.BinarySearch(b, word); ok {
			shared++
		}
	}
	return float64(shared) / float64(len(a)+len(b)-shared)
}

// escalationWords is what identifies an escalation as the same thing again:
// its need and its title.
func escalationWords(request EscalationRequest) []string {
	return words(request.Title + " " + request.Need)
}

// guard is the loop guard. It returns why the handler forwards the escalation
// at once, without a handler chat, or "" when the handler may try.
//
// The handler never escalates to itself because its chat has no escalate tool
// and raiseEscalation refuses a handler's chat. Besides that:
//
//   - A chat that escalates the same thing again after the handler resolved
//     or declined it is forwarded: the first answer did not help, or the chat
//     did not take it, and a second attempt of the handler would loop.
func (s *HandlerState) guard(request EscalationRequest) (note, reasoning string) {
	mine := escalationWords(request)
	for _, record := range s.Resolved {
		if record.Chat == request.Chat && similarity(mine, record.Words) >= repeatSimilarity {
			return fmt.Sprintf("The chat raised this again after the handler %s escalation %s. The handler's first answer did not unblock it, so it is yours. Read that escalation first, for what was tried.", record.Outcome, record.Escalation),
				"Loop guard: the same chat escalated the same thing twice after a decision of the handler, so the handler forwards at once and does not try again."
		}
	}
	return "", ""
}

// remember records a decision for the loop guard.
func (s *HandlerState) remember(request EscalationRequest, outcome Outcome, now time.Time) {
	if outcome != OutcomeResolved && outcome != OutcomeDeclined {
		return
	}
	s.Resolved = append(s.Resolved, ResolvedRecord{Chat: request.Chat, Escalation: request.ID, Outcome: outcome, Words: escalationWords(request), At: now})
	if over := len(s.Resolved) - maxResolvedRecords; over > 0 {
		s.Resolved = slices.Clone(s.Resolved[over:])
	}
}

// see records an escalation as taken and says whether it is new.
func (s *HandlerState) see(id string) bool {
	if slices.Contains(s.Seen, id) {
		return false
	}
	s.Seen = append(s.Seen, id)
	if over := len(s.Seen) - maxSeen; over > 0 {
		s.Seen = slices.Clone(s.Seen[over:])
	}
	return true
}

// HandlerWorkflow is the escalation handler: it takes the escalations that
// are handed to it and runs a handler chat for each, at most MaxHandlers at
// once. It runs for ever, and continues as new when it is idle and a newer
// worker build is the target or Temporal suggests it.
func HandlerWorkflow(ctx workflow.Context, state HandlerState) error {
	if err := workflow.SetQueryHandler(ctx, QueryHandler, func() (HandlerState, error) { return state, nil }); err != nil {
		return err
	}
	intakes := workflow.GetSignalChannel(ctx, SignalEscalationIntake)
	reports := workflow.GetSignalChannel(ctx, SignalReport)
	// ended holds the handler chats whose turn ended or that ended, with the
	// error of one that failed.
	ended := map[string]string{}
	take := func(intake HandlerIntake) {
		if state.see(intake.Request.ID) {
			state.Queue = append(state.Queue, intake)
		}
	}
	workflow.Go(ctx, func(ctx workflow.Context) {
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(intakes, func(channel workflow.ReceiveChannel, _ bool) {
			var intake HandlerIntake
			channel.Receive(ctx, &intake)
			take(intake)
		})
		selector.AddReceive(reports, func(channel workflow.ReceiveChannel, _ bool) {
			var report Report
			channel.Receive(ctx, &report)
			if report.Ended || report.Message != "" {
				ended[report.ChatID] = report.Error
			}
		})
		cancelled := false
		selector.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) { cancelled = true })
		for !cancelled {
			selector.Select(ctx)
		}
	})

	for {
		ready := func() bool { return len(state.Queue) > 0 && state.Active < MaxHandlers }
		if _, err := workflow.AwaitWithTimeout(ctx, handlerWake, ready); err != nil {
			return err
		}
		if ready() {
			intake := state.Queue[0]
			state.Queue = state.Queue[1:]
			state.Active++
			state.Handled++
			workflow.Go(ctx, func(ctx workflow.Context) {
				defer func() { state.Active-- }()
				handleEscalation(ctx, &state, intake, ended)
			})
			continue
		}
		if state.Active == 0 && len(state.Queue) == 0 && upgrade.Due(ctx) {
			if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
				return err
			}
			// Take what the server already delivered, so no signal is lost
			// between this run and the next.
			for {
				var intake HandlerIntake
				if !intakes.ReceiveAsync(&intake) {
					break
				}
				take(intake)
			}
			if len(state.Queue) == 0 {
				return upgrade.ContinueAsNew(ctx, HandlerWorkflowName, state)
			}
		}
	}
}

// handlerActivity is how the handler workflow reaches Temporal for what a
// workflow cannot read: the state of another workflow.
var handlerActivity = workflow.ActivityOptions{
	TaskQueue:           TaskQueue,
	StartToCloseTimeout: 30 * time.Second,
	RetryPolicy:         &temporal.RetryPolicy{InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumAttempts: 4, NonRetryableErrorTypes: []string{ErrorRefused}},
}

// handoffOptions are those of the hand-off, which an escalation waits for.
var handoffOptions = workflow.ActivityOptions{
	TaskQueue:           TaskQueue,
	StartToCloseTimeout: 30 * time.Second,
	RetryPolicy:         &temporal.RetryPolicy{InitialInterval: 2 * time.Second, BackoffCoefficient: 2, MaximumAttempts: 3},
}

// handleEscalation takes one escalation through the handler: the loop guard,
// the claim, the handler chat's turn, and a forward when the chat decided
// nothing.
func handleEscalation(ctx workflow.Context, state *HandlerState, intake HandlerIntake, ended map[string]string) {
	request := intake.Request
	escalation := EscalationWorkflowID(request.ID)
	forward := func(note, reasoning string) {
		_ = workflow.SignalExternalWorkflow(ctx, escalation, "", SignalEscalationForward, EscalationForward{Note: note, Reasoning: reasoning}).Get(ctx, nil)
	}
	if note, reasoning := state.guard(request); note != "" {
		forward(note, reasoning)
		return
	}
	var chatID string
	if err := workflow.SideEffect(ctx, func(workflow.Context) any { return ids.New() }).Get(&chatID); err != nil {
		forward("The handler could not start: "+err.Error(), "The handler failed before it read the escalation.")
		return
	}
	var brief string
	options := workflow.WithActivityOptions(ctx, handlerActivity)
	if err := workflow.ExecuteActivity(options, ActivityEscalationBrief, request).Get(ctx, &brief); err != nil {
		brief = "The recent state of the chat could not be read: " + cause(err)
	}
	_ = workflow.SignalExternalWorkflow(ctx, escalation, "", SignalEscalationHandling, EscalationClaim{Handler: chatID}).Get(ctx, nil)

	chatRequest := handlerChat(workflow.GetInfo(ctx).WorkflowExecution.ID, chatID, intake)
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: WorkflowID(chatID), TaskQueue: TaskQueue,
		// The chat is closed below. It must not die with a run of this
		// workflow that continues as new.
		ParentClosePolicy:        enumspb.PARENT_CLOSE_POLICY_ABANDON,
		TypedSearchAttributes:    SearchAttributes(chatRequest),
		Memo:                     Memo(chatRequest),
		WorkflowExecutionTimeout: handlerChatTimeout,
	})
	future := workflow.ExecuteChildWorkflow(childCtx, WorkflowName, State{Request: chatRequest, Pending: []Message{{ID: chatID, Text: handlerTask(request, brief)}}})
	if err := future.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
		forward("The handler's chat could not be started: "+cause(err)+".", "The handler never read the escalation.")
		return
	}
	// One attempt: the chat has one turn.
	decided, err := workflow.AwaitWithTimeout(ctx, handlerTurnBudget, func() bool { _, ok := ended[chatID]; return ok })
	if err != nil {
		return
	}
	closeCtx, _ := workflow.NewDisconnectedContext(ctx)
	_ = workflow.SignalExternalWorkflow(closeCtx, WorkflowID(chatID), "", SignalClose, Action{}).Get(closeCtx, nil)

	var outcome EscalationState
	if err := workflow.ExecuteActivity(options, ActivityEscalationOutcome, request.ID).Get(ctx, &outcome); err != nil {
		outcome = EscalationState{}
	}
	switch outcome.Status {
	case EscalationOpen, EscalationHandling, "":
		note, reasoning := "The handler's chat ended its turn without deciding, so it is yours.", "One attempt: the handler's chat ended its turn with no resolution, forward or refusal."
		switch {
		case !decided:
			note, reasoning = fmt.Sprintf("The handler's chat did not decide within %s, so it is yours.", handlerTurnBudget), "One attempt: the handler ran out of time."
		case ended[chatID] != "":
			note, reasoning = "The handler's chat failed: "+ended[chatID]+". It is yours.", "One attempt: the handler's chat failed."
		}
		forward(note, reasoning)
	}
	if outcome.Decision != nil {
		state.remember(request, outcome.Decision.Outcome, workflow.Now(ctx))
	}
}

// handlerChat is the request of the handler's chat for an escalation: the
// reach of the chat it serves, and no more.
func handlerChat(owner, id string, intake HandlerIntake) Request {
	request := intake.Request
	chat := Request{
		ID: id, Title: "Escalation handler: " + request.Title, Owner: owner, Handler: true,
		Issue: request.Issue, Project: request.Project,
		Serves: &Serving{Escalation: request.ID, Chat: request.Chat},
	}
	if session := request.Session; session != nil {
		chat.Host, chat.Backend, chat.Model, chat.Models = session.Host, session.Backend, session.Model, session.Models
		chat.Restricted = session.Restricted
		// The handler holds the scope of the chat it serves, and no more.
		chat.Scope = inheritScope(session.Scope, nil)
		chat.Repositories = ScopedRepositories(session.Repositories, chat.Scope)
	}
	if len(intake.Models) > 0 {
		chat.Model, chat.Models = intake.Model, intake.Models
	}
	return chat
}

// handlerTask is the handler chat's first message: the escalation and what
// is known of the chat that raised it.
func handlerTask(request EscalationRequest, brief string) string {
	var text strings.Builder
	fmt.Fprintf(&text, "Decide escalation %s of the chat %s.\n\n", request.ID, request.Chat)
	if request.Issue != nil {
		fmt.Fprintf(&text, "The chat works on the issue %s#%d. Read it with `gh issue view %d --repo %s --comments`.\n\n", request.Issue.Repo, request.Issue.Number, request.Issue.Number, request.Issue.Repo)
	}
	fmt.Fprintf(&text, "## What the chat asks\n\nNeed:\n%s\n\nWhy it cannot get it itself:\n%s\n", request.Need, request.Why)
	if request.Tried != "" {
		fmt.Fprintf(&text, "\nWhat it tried:\n%s\n", request.Tried)
	}
	if request.Next != "" {
		fmt.Fprintf(&text, "\nWhat it will do once unblocked:\n%s\n", request.Next)
	}
	fmt.Fprintf(&text, "\n## The chat's recent state\n\n%s\n", brief)
	return text.String()
}

// handlerInstructions are the rules of the handler's chat, in place of the
// ordinary chat's.
func handlerInstructions(text *strings.Builder, request Request) {
	fmt.Fprintf(text, "\nYou are the escalation handler for escalation %s of the chat %s. No person watches this chat. A chat that cannot go on asked for something, and you decide what happens to it before it reaches a person. You have one turn, and you end it by calling exactly one of escalation_resolve, escalation_forward and escalation_decline.\n", request.Serves.Escalation, request.Serves.Chat)
	text.WriteString("\nWhat you may fix is what a chat with the right knowledge could fix:\n")
	text.WriteString("- Answer from the documentation, the code or the learnings (learning_search, and the clones in your working directory).\n")
	text.WriteString("- Point out a different approach: a chat that asks for an operator's script to verify a thing can often verify it itself.\n")
	text.WriteString("- Restart its turn: an answer to the chat starts its next turn, so \"carry on, nothing was wrong\" is an answer.\n")
	text.WriteString("- Correct the issue's text with issue_edit, when an unclear or wrong issue is what blocks the chat.\n")
	text.WriteString("\nYou have no more authority than the chat you serve, and you never grant it any. You cannot give a chat a credential, access, a permission, a merge that rules block, a hypervisor or cluster write, or money. Anything that needs one of those, a judgement call that is a person's to make, or anything you are not sure of, you forward: escalation_forward with a short summary for the person. Never answer with a guess dressed as an answer, and never tell a chat that a person approved something that nobody did.\n")
	text.WriteString("\nThe three outcomes:\n")
	text.WriteString("- escalation_resolve: you can unblock the chat. First record a learning with learning_record (the gap and the fix: what the chat did not know and what it should do, scope and repositories as they fit, and this escalation's ID; no secrets, tokens or logs). It is accepted at once, and chats read it from now on. Then call escalation_resolve with the answer the chat gets and your reasoning. It refuses until a learning is recorded.\n")
	text.WriteString("- escalation_forward: a person has to act. The summary says what is needed, why you could not do it, and the exact action asked for (who, where, what to run or decide), so that the person needs nothing else to act on it.\n")
	text.WriteString("- escalation_decline: the request should not be done (it is unsafe, goes round a rule, is out of the project's scope, or the issue says not to). The chat is told why and a person is notified.\n")
	text.WriteString("Your reasoning is what a person reads to check your decision, so say what you looked at and why you chose this outcome.\n")
	text.WriteString("\nThis is one attempt. If you cannot decide, forward. You have no escalate tool: you do not escalate to yourself. Do not change code, push branches or open pull requests.\n")
	text.WriteString(learningInstructions)
}
