package chat_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/domain/navi"
	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// session stands in for the core worker's session activities. A turn whose
// prompt is "slow" calls onSlow and then runs until the workflow cancels it.
// Temporal's test environment keeps its clock still while an activity runs,
// so what a test does during a turn is done from onSlow.
type session struct {
	mu      sync.Mutex
	opened  []domain.SessionRequest
	turns   []domain.TurnRequest
	closed  int
	onSlow  func()
	release chan struct{}
}

func (s *session) open(_ context.Context, request domain.SessionRequest) (domain.SessionHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.opened = append(s.opened, request)
	return domain.SessionHandle{ID: request.ID, TaskQueue: "local-test"}, nil
}

func (s *session) turn(_ context.Context, request domain.TurnRequest) (domain.TurnResult, error) {
	s.mu.Lock()
	s.turns = append(s.turns, request)
	s.mu.Unlock()
	if request.Prompt != "slow" {
		return domain.TurnResult{Text: "answer to " + request.Prompt, Turn: request.Turn, StopReason: p7.StopEndTurn}, nil
	}
	s.onSlow()
	<-s.release
	return domain.TurnResult{Turn: request.Turn, StopReason: p7.StopCancelled}, nil
}

func (s *session) close(context.Context, domain.CloseRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed++
	return nil
}

func (s *session) sessionID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opened[0].ID
}

// lookup is an app's tool: it runs an activity of the app and records what it
// read in the chat's state.
func lookup(current *chat.Chat) p7.Tool {
	return p7.Tool{
		Name:        "lookup",
		Description: "Look a word up.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"word":{"type":"string"}},"required":["word"]}`),
		Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct {
				Word string `json:"word"`
			}
			if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
				return p7.ToolResult{}, err
			}
			options := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{TaskQueue: chat.TaskQueue, StartToCloseTimeout: time.Minute})
			var meaning string
			if err := workflow.ExecuteActivity(options, "Lookup", arguments.Word).Get(ctx, &meaning); err != nil {
				return p7.ToolResult{}, err
			}
			current.State.LastLearning = arguments.Word
			return p7.TextResult(meaning), nil
		},
	}
}

// registry records what Register registers, for the test environment.
type registry struct {
	env *testsuite.TestWorkflowEnvironment
}

func (r registry) RegisterWorkflow(name string, function any) {
	r.env.RegisterWorkflowWithOptions(function, workflow.RegisterOptions{Name: name})
}

func (r registry) RegisterActivity(name string, function any) {
	r.env.RegisterActivityWithOptions(function, activity.RegisterOptions{Name: name})
}

// newEnvironment is a test environment with the chat registered as an app
// registers it: with a tool of its own, its instructions, a per-turn context
// and its activity.
func newEnvironment(t *testing.T) (*testsuite.TestWorkflowEnvironment, *session) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fake := &session{release: make(chan struct{})}
	// The test environment does not end a cancelled activity's context.
	env.SetOnActivityCanceledListener(func(*activity.Info) { close(fake.release) })
	err := chat.Register(registry{env}, chat.Options{
		Tools: func(current *chat.Chat) []p7.Tool { return []p7.Tool{lookup(current)} },
		Instructions: func(request *chat.Request) (string, error) {
			return "app instructions for " + request.Id, nil
		},
		TurnContext: func(_ workflow.Context, _ *chat.Chat, message *chat.Message) string {
			return "turn context for " + message.Id
		},
		Activities: map[string]any{
			"Lookup": func(_ context.Context, word string) (string, error) { return word + " means a test", nil },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	env.RegisterActivityWithOptions(fake.open, activity.RegisterOptions{Name: domain.ActivityOpenSession})
	env.RegisterActivityWithOptions(fake.turn, activity.RegisterOptions{Name: domain.ActivityRunTurn})
	env.RegisterActivityWithOptions(fake.close, activity.RegisterOptions{Name: domain.ActivityCloseSession})
	env.OnGetVersion("chat-model-limit", workflow.DefaultVersion, 2).Return(workflow.DefaultVersion)
	return env, fake
}

func newChat(first string) *navi.ChatState {
	return &navi.ChatState{
		Request: &chat.Request{
			Id: "c1", Title: "test", Host: "test",
			Repositories: []*chat.Repository{{Name: "repo", Url: "/remote/repo.git"}},
		},
		Pending: []*chat.Message{{Id: "m1", Text: first}},
	}
}

func queryState(t *testing.T, env *testsuite.TestWorkflowEnvironment) *navi.ChatState {
	t.Helper()
	value, err := env.QueryWorkflow(chat.QueryState)
	if err != nil {
		t.Fatal(err)
	}
	var state *navi.ChatState
	if err := value.Get(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

// callTool calls a tool as the session's agent would, and hands the result to
// complete.
func callTool(t *testing.T, env *testsuite.TestWorkflowEnvironment, session, tool string, arguments any, complete func(p7.ToolResult)) {
	t.Helper()
	raw, err := json.Marshal(arguments)
	if err != nil {
		t.Fatal(err)
	}
	// The test environment takes a second update with an ID it has seen, in
	// any workflow, as the first one again, so each update has its own ID.
	env.UpdateWorkflow(domain.UpdateToolCall, session+"-"+tool, &testsuite.TestUpdateCallback{
		OnAccept: func() {},
		OnReject: func(err error) { t.Errorf("%s rejected: %v", tool, err) },
		OnComplete: func(value any, err error) {
			if err != nil {
				t.Errorf("%s: %v", tool, err)
				return
			}
			complete(value.(p7.ToolResult))
		},
	}, p7.ToolCall{Session: session, Tool: tool, Arguments: raw})
}

// A message is answered by a turn on the session. The first turn carries the
// app's instructions, every turn the app's turn context, and the session is
// offered the app's tool beside the base's. The app's tool runs the app's
// activity and changes the chat's state.
func TestATurnAndAToolTheAppAdds(t *testing.T) {
	env, fake := newEnvironment(t)
	var looked p7.ToolResult
	env.RegisterDelayedCallback(func() {
		callTool(t, env, fake.sessionID(), "lookup", map[string]string{"word": "chat"}, func(result p7.ToolResult) { looked = result })
	}, time.Minute)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chat.SignalMessage, &chat.Message{Id: "m2", Text: "second"})
	}, 2*time.Minute)
	env.RegisterDelayedCallback(func() {
		if state := queryState(t, env); state.LastLearning != "chat" || state.Turns != 2 {
			t.Errorf("state: last learning %q, turns %d, want the tool's record and 2", state.LastLearning, state.Turns)
		}
		// A close with no payload ends the chat too.
		env.SignalWorkflow(chat.SignalClose, nil)
	}, 3*time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("first"))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(fake.opened) != 1 {
		t.Fatalf("opened %d sessions, want one", len(fake.opened))
	}
	var offered []string
	for _, tool := range fake.opened[0].Tools {
		offered = append(offered, tool.Name)
	}
	if got := strings.Join(offered, ","); got != "lookup,chat_spawn,chat_await,chat_message,chat_attention,escalate" {
		t.Errorf("offered tools %s, want the app's lookup then the base's", got)
	}
	if looked.IsError || len(looked.Content) != 1 || looked.Content[0].Text != "chat means a test" {
		t.Errorf("lookup returned %+v, want the activity's text", looked)
	}
	if len(fake.turns) != 2 {
		t.Fatalf("%d turns, want 2", len(fake.turns))
	}
	if first := fake.turns[0].Context; !strings.HasPrefix(first, "app instructions for c1") || !strings.HasSuffix(first, "turn context for m1") {
		t.Errorf("first turn's context %q, want the app's instructions and turn context", first)
	}
	if second := fake.turns[1].Context; second != "turn context for m2" {
		t.Errorf("second turn's context %q, want only the turn context", second)
	}
	if fake.closed != 1 || queryState(t, env).Status != chat.StatusClosed {
		t.Errorf("closed %d times, want the session closed once and the chat closed", fake.closed)
	}
}

// Stop ends the running turn. The messages that wait stay queued and the next
// turn takes them.
func TestStopEndsTheTurnAndKeepsTheQueue(t *testing.T) {
	env, fake := newEnvironment(t)
	fake.onSlow = func() {
		env.SignalWorkflow(chat.SignalMessage, &chat.Message{Id: "m2", Text: "second"})
		env.SignalWorkflow(chat.SignalStop, &chat.Action{By: &chat.Person{Login: "alice@example.com"}})
	}
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, &chat.Action{}) }, 2*time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("slow"))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(fake.turns) != 2 || fake.turns[1].Prompt != "second" {
		t.Errorf("turns %+v, want the stopped one and then second", fake.turns)
	}
	if state := queryState(t, env); state.StoppedBy == nil || state.StoppedBy.Login != "alice@example.com" {
		t.Errorf("stopped by %+v, want alice", state.StoppedBy)
	}
}

// The escalate tool starts an escalation workflow. With no handler in the app
// it goes to a person at once. The person's answer reaches the chat as a
// message, which a turn answers.
func TestAnEscalationIsAnsweredByAPerson(t *testing.T) {
	env, fake := newEnvironment(t)
	var escalated p7.ToolResult
	fake.onSlow = func() {
		callTool(t, env, fake.sessionID(), "escalate", map[string]string{
			"need": "A registry token", "why": "The chat has no credentials",
		}, func(result p7.ToolResult) {
			escalated = result
			close(fake.release)
		})
	}
	// The test environment sends a child's signal to its parent through a
	// mock. It is passed on to the chat as Temporal would.
	env.OnSignalExternalWorkflow(mock.Anything, chat.WorkflowID("c1"), "", chat.SignalEscalation, mock.Anything).Return(
		func(_, _, _, signal string, notice any) error {
			env.SignalWorkflow(signal, notice)
			return nil
		})
	// A chat that closes with an escalation waiting withdraws it.
	env.OnRequestCancelExternalWorkflow(mock.Anything, mock.Anything, "").Return(nil)
	var waiting *navi.ChatState
	var forwarded *chat.EscalationState
	env.RegisterDelayedCallback(func() {
		waiting = queryState(t, env)
		if len(waiting.Escalations) != 1 {
			t.Errorf("escalations %+v, want one", waiting.Escalations)
			return
		}
		id := chat.EscalationWorkflowID(waiting.Escalations[0].Id)
		value, err := env.QueryWorkflowByID(id, chat.QueryEscalation)
		if err != nil {
			t.Error(err)
			return
		}
		if err := value.Get(&forwarded); err != nil {
			t.Error(err)
			return
		}
		err = env.UpdateWorkflowByID(id, chat.UpdateEscalationResolve, "resolve", &testsuite.TestUpdateCallback{
			OnAccept: func() {},
			OnReject: func(err error) { t.Errorf("resolve rejected: %v", err) },
			OnComplete: func(_ any, err error) {
				if err != nil {
					t.Errorf("resolve: %v", err)
				}
			},
		}, &chat.EscalationAnswer{Text: "use the token in vault", By: &chat.Person{Login: "alice@example.com"}})
		if err != nil {
			t.Error(err)
		}
	}, time.Minute)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, &chat.Action{}) }, 2*time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("slow"))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if escalated.IsError || !strings.Contains(escalated.Content[0].Text, "End your turn") {
		t.Errorf("escalate returned %+v", escalated)
	}
	if waiting.Status != chat.StatusEscalated {
		t.Errorf("status while it waits %s, want escalated", waiting.Status)
	}
	if forwarded.Status != chat.EscalationForwarded {
		t.Errorf("escalation status %s, want forwarded to a person, since the app has no handler", forwarded.Status)
	}
	if len(fake.turns) != 2 {
		t.Fatalf("%d turns, want the slow one and the answer's", len(fake.turns))
	}
	answer := fake.turns[1]
	if !strings.HasPrefix(answer.Turn, "escalation-") || !strings.Contains(answer.Prompt, "use the token in vault") || !strings.Contains(answer.Prompt, "alice@example.com") {
		t.Errorf("answer turn %+v, want the person's answer as a message", answer)
	}
}

// An idle chat continues as new when the deployment's target build changed,
// onto the new build, with its session and state. It is found at the next
// hourly look.
func TestAnIdleChatContinuesAsNewOntoANewBuild(t *testing.T) {
	env, fake := newEnvironment(t)
	env.RegisterDelayedCallback(func() { env.SetTargetWorkerDeploymentVersionChanged(true) }, 90*time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("first"))

	var continued *workflow.ContinueAsNewError
	if err := env.GetWorkflowError(); !errors.As(err, &continued) {
		t.Fatalf("workflow ended with %v, want a continue as new", err)
	}
	if continued.InitialVersioningBehavior != workflow.ContinueAsNewVersioningBehaviorAutoUpgrade {
		t.Errorf("versioning behavior %v, want auto-upgrade", continued.InitialVersioningBehavior)
	}
	if continued.WorkflowType.Name != chat.WorkflowName {
		t.Errorf("continues as %q, want %q", continued.WorkflowType.Name, chat.WorkflowName)
	}
	var next *navi.ChatState
	if err := converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next); err != nil {
		t.Fatal(err)
	}
	if fake.closed != 0 || next.Session == nil || next.Turns != 1 || !next.Briefed {
		t.Errorf("closed %d times, next state %+v, want the session open, one turn and briefed", fake.closed, next)
	}
}
