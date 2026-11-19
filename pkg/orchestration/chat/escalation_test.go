package chat_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// A chat that calls escalate starts an escalation workflow linked to it, ends
// its turn, and waits without a turn running. The escalation's answer, which
// arrives as a signal, starts the next turn as a message.
func TestEscalateRaisesAnEscalationAndTheAnswerResumesTheChat(t *testing.T) {
	env, fake := newEnvironment(t)
	var requests []chat.EscalationRequest
	env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, _ workflow.Context, args converter.EncodedValues) {
		var state chat.EscalationState
		if err := args.Get(&state); err != nil {
			t.Error(err)
		}
		requests = append(requests, state.Request)
		if want := chat.EscalationWorkflowID(state.Request.ID); info.WorkflowExecution.ID != want {
			t.Errorf("escalation workflow ID %q, want %q", info.WorkflowExecution.ID, want)
		}
	})
	env.OnWorkflow(chat.EscalationWorkflowName, mock.Anything, mock.Anything).Return(chat.EscalationState{}, nil)
	var reports []chat.Report
	env.OnSignalExternalWorkflow(mock.Anything, "owner", "", chat.SignalReport, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		reports = append(reports, args.Get(4).(chat.Report))
	})

	var result string
	fake.onSlow = func() {
		fake.mu.Lock()
		session := fake.opened[0].ID
		fake.mu.Unlock()
		toolCall(t, env, session, "escalate", map[string]string{
			"need": "A token for the registry\nwith push access", "why": "The chat has no credentials", "tried": "gh auth status", "next": "push the image",
		}, func(res p7.ToolResult) {
			if res.IsError {
				t.Errorf("escalate: %+v", res)
			}
			result = res.Content[0].Text
			close(fake.release)
		})
	}
	var waiting chat.State
	env.RegisterDelayedCallback(func() {
		waiting = queryState(t, env)
		id := waiting.Escalations[0].ID
		// The escalation was forwarded, which the chat is told.
		env.SignalWorkflow(chat.SignalEscalation, chat.EscalationNotice{ID: id, Status: chat.EscalationForwarded})
	}, time.Minute)
	var forwarded chat.State
	env.RegisterDelayedCallback(func() {
		forwarded = queryState(t, env)
		env.SignalWorkflow(chat.SignalEscalation, chat.EscalationNotice{
			ID: forwarded.Escalations[0].ID, Status: chat.EscalationResolved,
			Answer: &chat.EscalationAnswer{Text: "use the token in vault", By: &chat.Person{Login: "alice@example.com"}},
		})
		// A signal that comes again changes nothing.
		env.SignalWorkflow(chat.SignalEscalation, chat.EscalationNotice{
			ID: forwarded.Escalations[0].ID, Status: chat.EscalationResolved,
			Answer: &chat.EscalationAnswer{Text: "use the token in vault"},
		})
	}, 2*time.Minute)
	var resumed chat.State
	env.RegisterDelayedCallback(func() {
		resumed = queryState(t, env)
		env.SignalWorkflow(chat.SignalClose, nil)
	}, 3*time.Minute)
	state := newChat("slow")
	state.Request.Owner = "owner"
	state.Request.Issue = &chat.Issue{Repo: "gfx-labs/protocol7-selfdev", Number: 77}
	state.Request.Project = "tenant"
	state.Request.StartedBy = &chat.Person{Login: "bob@example.com"}
	env.ExecuteWorkflow(chat.WorkflowName, state)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}

	if len(requests) != 1 {
		t.Fatalf("started %d escalations, want one", len(requests))
	}
	request := requests[0]
	if request.Chat != "c1" || request.Need != "A token for the registry\nwith push access" || request.Why != "The chat has no credentials" ||
		request.Tried != "gh auth status" || request.Next != "push the image" || request.Title != "A token for the registry" ||
		request.Issue == nil || request.Issue.Number != 77 || request.Project != "tenant" || request.StartedBy.Login != "bob@example.com" {
		t.Errorf("escalation request %+v", request)
	}
	if !strings.Contains(result, request.ID) || !strings.Contains(result, "End your turn") {
		t.Errorf("tool result %q should name the escalation and tell the agent to end its turn", result)
	}
	// The chat waits on the escalation, with no turn running.
	if waiting.Status != chat.StatusEscalated || len(waiting.Escalations) != 1 || waiting.Escalations[0].Status != chat.EscalationOpen {
		t.Errorf("waiting: status %s, escalations %+v, want escalated and one open", waiting.Status, waiting.Escalations)
	}
	if forwarded.Status != chat.StatusEscalated || forwarded.Escalations[0].Status != chat.EscalationForwarded || len(forwarded.Pending) != 0 {
		t.Errorf("forwarded: status %s, escalations %+v, pending %+v", forwarded.Status, forwarded.Escalations, forwarded.Pending)
	}
	// The answer is a message: one turn answers it, once.
	if len(fake.turns) != 2 {
		t.Fatalf("%d turns, want the slow one and the one that answers the escalation", len(fake.turns))
	}
	answer := fake.turns[1]
	if answer.Turn != "escalation-"+request.ID || !strings.Contains(answer.Prompt, "use the token in vault") || !strings.Contains(answer.Prompt, "alice@example.com") {
		t.Errorf("answer turn %+v", answer)
	}
	if resumed.Escalations[0].Status != chat.EscalationResolved || resumed.Status != chat.StatusIdle {
		t.Errorf("resumed: status %s, escalations %+v, want idle and resolved", resumed.Status, resumed.Escalations)
	}
	// The issue's workflow hears of the escalation at once.
	var told *chat.EscalationRef
	for _, report := range reports {
		if report.Escalation != nil {
			told = report.Escalation
		}
	}
	if told == nil || told.ID != request.ID {
		t.Errorf("owner's reports %+v, want one that carries the escalation", reports)
	}
}

// escalate refuses what a person cannot act on, and more escalations than a
// chat may have waiting.
func TestEscalateRefusesAnEmptyRequestAndTooManyOpen(t *testing.T) {
	env, fake := newEnvironment(t)
	env.OnWorkflow(chat.EscalationWorkflowName, mock.Anything, mock.Anything).Return(chat.EscalationState{}, nil)
	refused := 0
	fake.onSlow = func() {
		fake.mu.Lock()
		session := fake.opened[0].ID
		fake.mu.Unlock()
		isError := func(res p7.ToolResult) {
			if !res.IsError {
				t.Errorf("escalate was accepted: %+v", res)
			}
			refused++
		}
		toolCall(t, env, session, "escalate", map[string]string{"need": "x", "why": "  "}, isError)
		toolCall(t, env, session, "escalate", map[string]string{"need": "x", "why": "y"}, func(p7.ToolResult) {
			toolCall(t, env, session, "escalate", map[string]string{"need": "second", "why": "y"}, isError)
			close(fake.release)
		})
	}
	state := newChat("slow")
	for range chat.MaxOpenEscalations - 1 {
		state.Escalations = append(state.Escalations, chat.EscalationRef{ID: "old" + string(rune('a'+len(state.Escalations))), Status: chat.EscalationForwarded})
	}
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, time.Minute)
	env.OnRequestCancelExternalWorkflow(mock.Anything, mock.Anything, "").Return(nil)
	env.ExecuteWorkflow(chat.WorkflowName, state)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if refused != 2 {
		t.Errorf("%d refusals, want an empty request and the one past the bound", refused)
	}
}

// probe_escalate goes through the same mechanism: an escalation whose need is
// the plan, and a result that tells the agent to end its turn.
func TestProbeEscalateRaisesAnEscalation(t *testing.T) {
	env, fake := newEnvironment(t)
	env.OnWorkflow(chat.EscalationWorkflowName, mock.Anything, mock.Anything).Return(chat.EscalationState{}, nil)
	var request chat.EscalationRequest
	env.SetOnChildWorkflowStartedListener(func(_ *workflow.Info, _ workflow.Context, args converter.EncodedValues) {
		var state chat.EscalationState
		_ = args.Get(&state)
		request = state.Request
	})
	var result p7.ToolResult
	fake.onSlow = func() {
		fake.mu.Lock()
		session := fake.opened[0].ID
		fake.mu.Unlock()
		toolCall(t, env, session, "probe_escalate", map[string]string{"plan": "kubectl apply the pod with egress"}, func(res p7.ToolResult) {
			result = res
			close(fake.release)
		})
	}
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, time.Minute)
	env.OnRequestCancelExternalWorkflow(mock.Anything, mock.Anything, "").Return(nil)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("slow"))
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if result.IsError || !strings.Contains(result.Content[0].Text, request.ID) {
		t.Errorf("probe_escalate result %+v", result)
	}
	if request.Title != "Probe review" || !strings.Contains(request.Need, "kubectl apply the pod with egress") || request.Why == "" || request.Chat != "c1" {
		t.Errorf("escalation request %+v", request)
	}
	if state := queryState(t, env); len(state.Children) != 0 {
		t.Errorf("probe_escalate started a child chat: %+v", state.Children)
	}
}

// A child chat that escalated is not done: it stays open until the answer
// has been given and its turn ended, and closing the chat withdraws the
// escalation.
func TestAChildWaitsOnItsEscalationAndWithdrawsItWhenClosed(t *testing.T) {
	env, fake := newEnvironment(t)
	env.OnWorkflow(chat.EscalationWorkflowName, mock.Anything, mock.Anything).Return(chat.EscalationState{}, nil)
	env.OnSignalExternalWorkflow(mock.Anything, "parent", "", chat.SignalReport, mock.Anything).Return(nil)
	var cancelled []string
	env.OnRequestCancelExternalWorkflow(mock.Anything, mock.Anything, "").Return(nil).Run(func(args mock.Arguments) {
		cancelled = append(cancelled, args.Get(1).(string))
	})
	fake.onSlow = func() {
		fake.mu.Lock()
		session := fake.opened[0].ID
		fake.mu.Unlock()
		toolCall(t, env, session, "escalate", map[string]string{"need": "n", "why": "w"}, func(p7.ToolResult) { close(fake.release) })
	}
	var waiting chat.State
	env.RegisterDelayedCallback(func() {
		waiting = queryState(t, env)
		env.SignalWorkflow(chat.SignalClose, nil)
	}, time.Hour)
	state := newChat("slow")
	state.Request.Parent, state.Request.Owner = "parent", "parent"
	env.ExecuteWorkflow(chat.WorkflowName, state)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if waiting.Status != chat.StatusEscalated || waiting.Closing {
		t.Errorf("the child closed itself while its escalation waited: %+v", waiting)
	}
	if len(cancelled) != 1 || cancelled[0] != chat.EscalationWorkflowID(waiting.Escalations[0].ID) {
		t.Errorf("withdrawn %v, want the escalation of the chat", cancelled)
	}
}

func newEscalationEnvironment(t *testing.T) (*testsuite.TestWorkflowEnvironment, *[]chat.EscalationNotice) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(chat.EscalationWorkflow, workflow.RegisterOptions{Name: chat.EscalationWorkflowName})
	// These tests are of an escalation with no handler, as it ran before the
	// handler: it replays as it did. handler_test.go has the handler's.
	env.OnGetVersion("escalation-handler", workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
	notices := &[]chat.EscalationNotice{}
	env.OnSignalExternalWorkflow(mock.Anything, "chat-c1", "", chat.SignalEscalation, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		*notices = append(*notices, args.Get(4).(chat.EscalationNotice))
	})
	return env, notices
}

func newEscalation() chat.EscalationState {
	return chat.EscalationState{Request: chat.EscalationRequest{ID: "e1", Chat: "c1", Need: "n", Why: "w", Title: "n"}}
}

func queryEscalation(t *testing.T, env *testsuite.TestWorkflowEnvironment) chat.EscalationState {
	t.Helper()
	value, err := env.QueryWorkflow(chat.QueryEscalation)
	if err != nil {
		t.Fatal(err)
	}
	var state chat.EscalationState
	if err := value.Get(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func resolve(t *testing.T, env *testsuite.TestWorkflowEnvironment, text string, done func(chat.EscalationState, error)) {
	t.Helper()
	env.UpdateWorkflow(chat.UpdateEscalationResolve, "", &testsuite.TestUpdateCallback{
		OnAccept: func() {},
		OnReject: func(err error) { done(chat.EscalationState{}, err) },
		OnComplete: func(value any, err error) {
			state, _ := value.(chat.EscalationState)
			done(state, err)
		},
	}, chat.EscalationAnswer{Text: text, By: &chat.Person{Login: "alice@example.com"}})
}

// Without the handler (an escalation that started before it) a new escalation goes straight from open to forwarded and
// waits. A person's answer resolves it, reaches the chat and ends the
// workflow.
func TestEscalationIsForwardedAndAnswered(t *testing.T) {
	env, notices := newEscalationEnvironment(t)
	var before chat.EscalationState
	var refused, answered error
	var result chat.EscalationState
	env.RegisterDelayedCallback(func() {
		before = queryEscalation(t, env)
		resolve(t, env, "  ", func(_ chat.EscalationState, err error) { refused = err })
		resolve(t, env, " go ahead ", func(state chat.EscalationState, err error) { result, answered = state, err })
	}, time.Hour)
	env.ExecuteWorkflow(chat.EscalationWorkflowName, newEscalation())
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if before.Status != chat.EscalationForwarded {
		t.Errorf("status before the answer %s, want forwarded", before.Status)
	}
	if refused == nil || !strings.Contains(refused.Error(), "empty") {
		t.Errorf("an empty answer was not refused: %v", refused)
	}
	if answered != nil || result.Status != chat.EscalationResolved {
		t.Errorf("answer: error %v, state %+v", answered, result)
	}
	var final chat.EscalationState
	if err := env.GetWorkflowResult(&final); err != nil {
		t.Fatal(err)
	}
	if final.Status != chat.EscalationResolved || final.Answer == nil || final.Answer.Text != "go ahead" || final.Answer.By.Login != "alice@example.com" || !final.Delivered || final.Withdrawn {
		t.Errorf("final state %+v", final)
	}
	if len(*notices) != 2 || (*notices)[0].Status != chat.EscalationForwarded || (*notices)[1].Status != chat.EscalationResolved ||
		(*notices)[1].Answer == nil || (*notices)[1].Answer.Text != "go ahead" || (*notices)[1].ID != "e1" {
		t.Errorf("notices to the chat %+v, want forwarded and then resolved with the answer", *notices)
	}
}

// The handler's signals move an escalation only along open, handling and
// forwarded. A handler's claim on an escalation that was forwarded is ignored.
func TestEscalationHandlerStates(t *testing.T) {
	t.Run("claimed then forwarded", func(t *testing.T) {
		env, notices := newEscalationEnvironment(t)
		state := newEscalation()
		// A handler that already holds it, as a later routing step will.
		state.Status = chat.EscalationHandling
		state.Handler = "handler"
		var mid chat.EscalationState
		env.RegisterDelayedCallback(func() {
			mid = queryEscalation(t, env)
			env.SignalWorkflow(chat.SignalEscalationForward, chat.EscalationForward{Note: "needs a person"})
		}, time.Minute)
		var end chat.EscalationState
		env.RegisterDelayedCallback(func() {
			end = queryEscalation(t, env)
			resolve(t, env, "ok", func(chat.EscalationState, error) {})
		}, time.Hour)
		env.ExecuteWorkflow(chat.EscalationWorkflowName, state)
		if err := env.GetWorkflowError(); err != nil {
			t.Fatal(err)
		}
		if mid.Status != chat.EscalationHandling || end.Status != chat.EscalationForwarded || end.Note != "needs a person" {
			t.Errorf("states %s then %s (note %q), want handling then forwarded", mid.Status, end.Status, end.Note)
		}
		if len(*notices) != 2 || (*notices)[0].Status != chat.EscalationForwarded {
			t.Errorf("notices %+v", *notices)
		}
	})
	t.Run("a claim after forwarding is ignored", func(t *testing.T) {
		env, _ := newEscalationEnvironment(t)
		var end chat.EscalationState
		env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalEscalationHandling, chat.EscalationClaim{Handler: "late"}) }, time.Minute)
		env.RegisterDelayedCallback(func() {
			end = queryEscalation(t, env)
			resolve(t, env, "ok", func(chat.EscalationState, error) {})
		}, time.Hour)
		env.ExecuteWorkflow(chat.EscalationWorkflowName, newEscalation())
		if err := env.GetWorkflowError(); err != nil {
			t.Fatal(err)
		}
		if end.Status != chat.EscalationForwarded || end.Handler != "" {
			t.Errorf("state %s, handler %q, want forwarded with no handler", end.Status, end.Handler)
		}
	})
}

// The chat ending withdraws the escalation: it closes as resolved with no
// answer, and the chat is not told.
func TestEscalationIsWithdrawnWhenItsChatEnds(t *testing.T) {
	env, notices := newEscalationEnvironment(t)
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Hour)
	env.ExecuteWorkflow(chat.EscalationWorkflowName, newEscalation())
	if !env.IsWorkflowCompleted() {
		t.Fatal("the escalation did not end")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var final chat.EscalationState
	if err := env.GetWorkflowResult(&final); err != nil {
		t.Fatal(err)
	}
	if final.Status != chat.EscalationResolved || !final.Withdrawn || final.Answer != nil {
		t.Errorf("final state %+v, want resolved, withdrawn, no answer", final)
	}
	if len(*notices) != 1 {
		t.Errorf("notices %+v, want only the forwarding", *notices)
	}
}

// The escalation's search attributes are what navi lists by.
func TestEscalationSearchAttributes(t *testing.T) {
	request := chat.EscalationRequest{ID: "e1", Chat: "c1", Issue: &chat.Issue{Repo: "o/r", Number: 3}, Project: "tenant", StartedBy: &chat.Person{Login: "a@b"}}
	attributes := chat.EscalationSearchAttributes(request)
	get := func(key string) string {
		for k, v := range attributes.GetUntypedValues() {
			if k.GetName() == key {
				return v.(string)
			}
		}
		return ""
	}
	for key, want := range map[string]string{
		"SelfdevKind": "escalation", "SelfdevParent": "c1", "SelfdevIssue": "o/r#3", "SelfdevProject": "tenant",
		"SelfdevStartedBy": "a@b", "SelfdevEscalationState": "open",
	} {
		if got := get(key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
}
