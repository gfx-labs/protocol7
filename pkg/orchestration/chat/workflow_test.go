package chat_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/design"
	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// session stands in for the core worker's session activities. A turn whose
// prompt is "slow" calls onSlow and then runs until the workflow cancels it.
//
// Temporal's test environment keeps its clock still while an activity runs,
// so what a test does during a turn is done from onSlow, not from a delayed
// callback.
type session struct {
	mu      sync.Mutex
	opened  []domain.SessionRequest
	openErr error
	turns   []domain.TurnRequest
	closed  int
	onSlow  func()
	release chan struct{}
	// stops are the stop reasons of the next turns that are not "slow", in
	// order. When they run out a turn ends with end_turn.
	stops []string
	// onStop runs inside a turn that takes a stop reason from stops.
	onStop func()
}

func (s *session) open(_ context.Context, request domain.SessionRequest) (domain.SessionHandle, error) {
	s.mu.Lock()
	s.opened = append(s.opened, request)
	s.mu.Unlock()
	if s.openErr != nil {
		return domain.SessionHandle{}, s.openErr
	}
	return domain.SessionHandle{ID: request.ID, TaskQueue: "local-test"}, nil
}

func (s *session) turn(_ context.Context, request domain.TurnRequest) (domain.TurnResult, error) {
	s.mu.Lock()
	s.turns = append(s.turns, request)
	s.mu.Unlock()
	if request.Prompt != "slow" {
		stop := "end_turn"
		if len(s.stops) > 0 {
			stop, s.stops = s.stops[0], s.stops[1:]
			if s.onStop != nil {
				s.onStop()
				<-s.release
			}
		}
		result := domain.TurnResult{Text: request.Prompt, Turn: request.Turn, StopReason: stop}
		if stop == domain.StopInterrupted {
			result.Failure = &domain.TurnFailure{Message: "the session's processes stopped during the turn"}
		}
		return result, nil
	}
	s.onSlow()
	<-s.release
	return domain.TurnResult{Turn: request.Turn, StopReason: "cancelled"}, nil
}

func (s *session) close(context.Context, domain.CloseRequest) error {
	s.mu.Lock()
	s.closed++
	s.mu.Unlock()
	return nil
}

func newEnvironment(t *testing.T) (*testsuite.TestWorkflowEnvironment, *session) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fake := &session{release: make(chan struct{})}
	// The test environment does not end a cancelled activity's context.
	env.SetOnActivityCanceledListener(func(*activity.Info) { close(fake.release) })
	env.RegisterWorkflowWithOptions(chat.Workflow, workflow.RegisterOptions{Name: chat.WorkflowName})
	env.RegisterWorkflowWithOptions(chat.EscalationWorkflow, workflow.RegisterOptions{Name: chat.EscalationWorkflowName})
	env.RegisterActivityWithOptions(fake.open, activity.RegisterOptions{Name: domain.ActivityOpenSession})
	env.RegisterActivityWithOptions(fake.turn, activity.RegisterOptions{Name: domain.ActivityRunTurn})
	env.RegisterActivityWithOptions(fake.close, activity.RegisterOptions{Name: domain.ActivityCloseSession})
	// Register native workflow/activity types before configuring SDK mocks.
	env.OnGetVersion("chat-model-limit", workflow.DefaultVersion, 2).Return(workflow.DefaultVersion)
	return env, fake
}

func newChat(first string) chat.State {
	return chat.State{
		Request: chat.Request{
			ID: "c1", Title: "test", Host: "test",
			Repositories: []chat.Repository{{Name: "repo", URL: "/remote/repo.git"}},
		},
		Pending: []chat.Message{{ID: "m1", Text: first}},
	}
}

func queryState(t *testing.T, env *testsuite.TestWorkflowEnvironment) chat.State {
	t.Helper()
	value, err := env.QueryWorkflow(chat.QueryState)
	if err != nil {
		t.Fatal(err)
	}
	var state chat.State
	if err := value.Get(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

// Messages are answered one turn each, in order, and only the first turn
// carries the instructions. Who sent each message and who closed the chat
// are kept. The other tests stop and close with no payload, as the server
// sent before it named people.
func TestMessagesAreAnsweredInOrder(t *testing.T) {
	env, fake := newEnvironment(t)
	alice := &chat.Person{Login: "alice@example.com", Name: "Alice"}
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chat.SignalMessage, chat.Message{ID: "m2", Text: "second", From: alice})
		env.SignalWorkflow(chat.SignalMessage, chat.Message{ID: "m3", Text: "third"})
	}, time.Minute)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, chat.Action{By: alice}) }, 2*time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("first"))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(fake.opened) != 1 || fake.opened[0].Inputs[0].Branch != "chat/c1" {
		t.Fatalf("opened %+v, want one session with its clone on chat/c1", fake.opened)
	}
	var got []string
	for index, turn := range fake.turns {
		got = append(got, turn.Turn+":"+turn.Prompt)
		if (turn.Context != "") != (index == 0) {
			t.Errorf("turn %d: has instructions = %v, want them on the first turn only", index, turn.Context != "")
		}
	}
	want := []string{"m1:first", "m2:second", "m3:third"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Errorf("turns %v, want %v", got, want)
	}
	if state := queryState(t, env); state.Status != chat.StatusClosed || state.Turns != 3 || fake.closed != 1 {
		t.Errorf("status %s, turns %d, closed %d times, want closed, 3 and once", state.Status, state.Turns, fake.closed)
	} else if len(state.Senders) != 1 || state.Senders["m2"] != alice.Login || state.ClosedBy == nil || *state.ClosedBy != *alice {
		t.Errorf("senders %v, closed by %+v, want only m2 from %s, closed by %+v", state.Senders, state.ClosedBy, alice.Login, alice)
	}
}

// A session that cannot be opened fails the chat after a few attempts, and
// the chat's error is the activity's own message.
func TestAChatWhoseSessionCannotOpenFailsWithTheCause(t *testing.T) {
	env, fake := newEnvironment(t)
	fake.openErr = temporal.NewApplicationError("the agent harness stopped while it started: no login", p7.ErrorHarnessStopped)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("first"))

	if env.GetWorkflowError() == nil {
		t.Fatal("the workflow ended without an error")
	}
	state := queryState(t, env)
	if state.Status != chat.StatusFailed || state.Error != "the agent harness stopped while it started: no login" || len(fake.opened) != 3 {
		t.Errorf("status %s, error %q, %d attempts, want failed, the activity's message and 3", state.Status, state.Error, len(fake.opened))
	}
}

// Stop ends the running turn only. What waits stays queued, and the next
// turn takes it. The chat stays open.
func TestStopCancelsTheTurnAndKeepsWhatWaits(t *testing.T) {
	env, fake := newEnvironment(t)
	fake.onSlow = func() {
		env.SignalWorkflow(chat.SignalMessage, chat.Message{ID: "m2", Text: "second"})
		env.SignalWorkflow(chat.SignalMessage, chat.Message{ID: "m3", Text: "third"})
		env.SignalWorkflow(chat.SignalStop, nil)
	}
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, 2*time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("slow"))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(fake.turns) != 3 || fake.turns[1].Prompt != "second" || fake.turns[2].Prompt != "third" {
		t.Errorf("turns %+v, want the stopped one and then \"second\" and \"third\"", fake.turns)
	}
}

// A stop that says to discard drops what waits, and the chat answers the next
// message.
func TestStopAndDiscardDropsWhatWaits(t *testing.T) {
	env, fake := newEnvironment(t)
	fake.onSlow = func() {
		env.SignalWorkflow(chat.SignalMessage, chat.Message{ID: "m2", Text: "dropped"})
		env.SignalWorkflow(chat.SignalStop, chat.Action{Discard: true})
	}
	var afterStop chat.State
	env.RegisterDelayedCallback(func() {
		afterStop = queryState(t, env)
		env.SignalWorkflow(chat.SignalMessage, chat.Message{ID: "m3", Text: "after"})
	}, time.Minute)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, 2*time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("slow"))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if afterStop.Status != chat.StatusIdle || afterStop.LastStop != "cancelled" || len(afterStop.Pending) != 0 {
		t.Errorf("after stop: status %s, last stop %q, %d waiting, want idle, cancelled and none", afterStop.Status, afterStop.LastStop, len(afterStop.Pending))
	}
	if len(fake.turns) != 2 || fake.turns[1].Prompt != "after" {
		t.Errorf("turns %+v, want the stopped one and then \"after\"", fake.turns)
	}
}

// A history from before the change replays as it ran: the stop drops what
// waits.
func TestStopOfAnOldHistoryDropsWhatWaits(t *testing.T) {
	env, fake := newEnvironment(t)
	env.OnGetVersion("stop-keeps-queue", workflow.DefaultVersion, 1).Return(workflow.DefaultVersion)
	fake.onSlow = func() {
		env.SignalWorkflow(chat.SignalMessage, chat.Message{ID: "m2", Text: "dropped"})
		env.SignalWorkflow(chat.SignalStop, nil)
	}
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, 2*time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("slow"))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(fake.turns) != 1 {
		t.Errorf("turns %+v, want only the stopped one", fake.turns)
	}
}

// A message that waits can be removed from the queue. The rest stay.
func TestADequeuedMessageIsNotSent(t *testing.T) {
	env, fake := newEnvironment(t)
	fake.onSlow = func() {
		env.SignalWorkflow(chat.SignalMessage, chat.Message{ID: "m2", Text: "removed"})
		env.SignalWorkflow(chat.SignalMessage, chat.Message{ID: "m3", Text: "kept"})
		env.SignalWorkflow(chat.SignalDequeue, chat.Dequeue{ID: "m2"})
		env.SignalWorkflow(chat.SignalStop, nil)
	}
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, 2*time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("slow"))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(fake.turns) != 2 || fake.turns[1].Prompt != "kept" {
		t.Errorf("turns %+v, want the stopped one and then \"kept\"", fake.turns)
	}
}

// Closing during a turn stops the turn and closes the session.
func TestCloseDuringATurn(t *testing.T) {
	env, fake := newEnvironment(t)
	fake.onSlow = func() { env.SignalWorkflow(chat.SignalClose, nil) }
	env.ExecuteWorkflow(chat.WorkflowName, newChat("slow"))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if state := queryState(t, env); state.Status != chat.StatusClosed || fake.closed != 1 {
		t.Errorf("status %s, closed %d times, want closed and once", state.Status, fake.closed)
	}
}

// A cancelled workflow still closes its session.
func TestCancellingTheWorkflowClosesTheSession(t *testing.T) {
	env, fake := newEnvironment(t)
	fake.onSlow = env.CancelWorkflow
	env.ExecuteWorkflow(chat.WorkflowName, newChat("slow"))

	if !env.IsWorkflowCompleted() {
		t.Fatal("the workflow did not end")
	}
	if fake.closed != 1 {
		t.Errorf("closed %d times, want once", fake.closed)
	}
}

// The session's agent shows a board with design_show, and the chat keeps
// it. A call from another session is refused before it runs. The elements
// a person picked go to the agent in the hidden context of their message.
func TestTheAgentShowsABoardAndPicksReachIt(t *testing.T) {
	env, fake := newEnvironment(t)
	var checked []string
	env.RegisterActivityWithOptions(func(_ context.Context, session, dir string) (design.View, error) {
		checked = append(checked, session+":"+dir)
		return design.View{Dir: dir, Title: "Checkout", Artboards: []design.Placed{{Artboard: design.Artboard{ID: "desktop", Page: "index.html", Width: 1440, Height: 900}}}}, nil
	}, activity.RegisterOptions{Name: design.ActivityCheck})

	call := func(session string, done func(any, error)) {
		env.UpdateWorkflow(domain.UpdateToolCall, session+"-call", &testsuite.TestUpdateCallback{
			OnAccept:   func() {},
			OnReject:   func(err error) { done(nil, err) },
			OnComplete: done,
		}, p7.ToolCall{Session: session, Tool: "design_show", Arguments: []byte(`{"dir":"repo/design/checkout"}`)})
	}
	var refused error
	var shown p7.ToolResult
	env.RegisterDelayedCallback(func() {
		call("intruder", func(_ any, err error) { refused = err })
		call(fake.opened[0].ID, func(result any, err error) {
			if err != nil {
				t.Errorf("design_show: %v", err)
				return
			}
			shown = result.(p7.ToolResult)
		})
	}, time.Minute)
	pick := chat.Pick{Artboard: "desktop", Page: "index.html", Selector: "main > button", Tag: "button", Text: "Pay"}
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(chat.SignalMessage, chat.Message{ID: "m2", Text: "make it bigger", Picks: []chat.Pick{pick}})
	}, 2*time.Minute)
	env.RegisterDelayedCallback(func() {
		if board := queryState(t, env).Board; board == nil || board.Dir != "repo/design/checkout" {
			t.Errorf("the chat's board is %+v, want repo/design/checkout", board)
		}
		env.SignalWorkflow(chat.SignalClose, nil)
	}, 3*time.Minute)
	state := newChat("first")
	env.ExecuteWorkflow(chat.WorkflowName, state)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(fake.opened) != 1 || fake.opened[0].Owner == "" {
		t.Fatalf("opened %+v, want one owned session", fake.opened)
	}
	if refused == nil || !strings.Contains(refused.Error(), "not one this workflow opened") {
		t.Errorf("a call from another session: got %v, want it refused", refused)
	}
	if want := []string{fake.opened[0].ID + ":repo/design/checkout"}; !slices.Equal(checked, want) {
		t.Errorf("checked %q, want %q", checked, want)
	}
	if shown.IsError || len(shown.Content) != 1 || !strings.Contains(shown.Content[0].Text, "desktop: index.html at 1440x900") {
		t.Errorf("design_show returned %+v, want the board described", shown)
	}
	if len(fake.turns) != 2 || !strings.Contains(fake.turns[1].Context, "main > button (button)") || fake.turns[1].Prompt != "make it bigger" {
		t.Errorf("turns %+v, want the second to carry the pick in its context", fake.turns)
	}
}

// A chat with an owner reports each turn's text and its end to the owner, and
// a chat about an issue gets the issue rules in place of the page's.
func TestAnIssueChatReportsToItsOwner(t *testing.T) {
	env, fake := newEnvironment(t)
	var reports []chat.Report
	env.OnSignalExternalWorkflow(mock.Anything, "issue-x", "", chat.SignalReport, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		reports = append(reports, args.Get(4).(chat.Report))
	})
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, time.Minute)
	state := newChat("first")
	state.Request.Issue = &chat.Issue{Repo: "gfx-labs/r", Number: 7}
	state.Request.Owner = "issue-x"
	env.ExecuteWorkflow(chat.WorkflowName, state)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fake.turns[0].Context, "gfx-labs/r#7 by yourself") || strings.Contains(fake.turns[0].Context, "only when the person asks") {
		t.Errorf("instructions %q, want the issue rules", fake.turns[0].Context)
	}
	if len(reports) != 2 || reports[0].Message != "m1" || !reports[0].Idle || reports[0].StopReason != "end_turn" || !reports[1].Ended {
		t.Errorf("reports %+v, want the turn of m1 and then the end", reports)
	}
}

// Archiving sets the search attribute the page's list filters on, and
// unarchiving clears it.
func TestArchiveSetsTheSearchAttribute(t *testing.T) {
	env, _ := newEnvironment(t)
	var seen []bool
	env.OnUpsertTypedSearchAttributes(mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		archived, _ := args.Get(0).(temporal.SearchAttributes).GetBool(chat.AttributeArchived)
		seen = append(seen, archived)
	})
	update := func(archived bool) func() {
		return func() {
			env.UpdateWorkflow(chat.UpdateArchive, "", &testsuite.TestUpdateCallback{
				OnReject: func(err error) { t.Error(err) },
				OnComplete: func(_ any, err error) {
					if err != nil {
						t.Error(err)
					}
				},
			}, chat.Archive{Archived: archived})
		}
	}
	env.RegisterDelayedCallback(update(true), time.Minute)
	env.RegisterDelayedCallback(func() {
		if !queryState(t, env).Archived {
			t.Error("the state does not say archived")
		}
	}, 2*time.Minute)
	env.RegisterDelayedCallback(update(false), 3*time.Minute)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, 4*time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("first"))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(seen, []bool{true, false}) {
		t.Errorf("upserts %v, want archived then not", seen)
	}
}

// A turn that ended because the session's processes stopped is continued in a
// new turn, after a wait on a workflow timer. The new turn says what happened
// and carries the message again. The chat then reports one answer with the
// number of resumes.
func TestAnInterruptedTurnIsResumed(t *testing.T) {
	env, fake := newEnvironment(t)
	fake.stops = []string{domain.StopInterrupted, domain.StopInterrupted}
	var reports []chat.Report
	env.OnSignalExternalWorkflow(mock.Anything, "issue-x", "", chat.SignalReport, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		reports = append(reports, args.Get(4).(chat.Report))
	})
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, time.Hour)
	state := newChat("build it")
	state.Request.Owner = "issue-x"
	env.ExecuteWorkflow(chat.WorkflowName, state)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(fake.turns) != 3 {
		t.Fatalf("%d turns, want the cut off one, two resumes", len(fake.turns))
	}
	for _, index := range []int{1, 2} {
		turn := fake.turns[index]
		if turn.Turn == fake.turns[0].Turn || !strings.Contains(turn.Context, "cut off") || !strings.Contains(turn.Context, "build it") || !strings.Contains(turn.Context, "git status") {
			t.Errorf("resume %d is %+v, want a new turn that says it was cut off, names git status and repeats the request", index, turn)
		}
	}
	if len(reports) < 1 || reports[0].Resumes != 2 || reports[0].GaveUp || reports[0].StopReason != "end_turn" || reports[0].Error != "" {
		t.Errorf("reports %+v, want one answer after two resumes", reports)
	}
}

// A person's stop is not process loss, even when the turn it stopped comes
// back as interrupted. It is not resumed, and the report says who stopped it.
func TestAStoppedTurnIsNotResumed(t *testing.T) {
	env, fake := newEnvironment(t)
	fake.stops = []string{domain.StopInterrupted}
	fake.onStop = func() { env.SignalWorkflow(chat.SignalStop, chat.Action{By: &chat.Person{Login: "alice@example.com"}}) }
	var reports []chat.Report
	env.OnSignalExternalWorkflow(mock.Anything, "issue-x", "", chat.SignalReport, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		reports = append(reports, args.Get(4).(chat.Report))
	})
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, time.Hour)
	state := newChat("work")
	state.Request.Owner = "issue-x"
	env.ExecuteWorkflow(chat.WorkflowName, state)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(fake.turns) != 1 {
		t.Errorf("%d turns, want the stopped one only", len(fake.turns))
	}
	if len(reports) < 1 || reports[0].Resumes != 0 || reports[0].GaveUp {
		t.Errorf("reports %+v, want no resume", reports)
	}
}

// A session whose processes keep stopping is resumed a bounded number of
// times. Then the chat reports that it gave up, stays open, and answers the
// next message.
func TestTheChatGivesUpAfterTheLastResume(t *testing.T) {
	env, fake := newEnvironment(t)
	fake.stops = []string{domain.StopInterrupted, domain.StopInterrupted, domain.StopInterrupted, domain.StopInterrupted, domain.StopInterrupted}
	var reports []chat.Report
	env.OnSignalExternalWorkflow(mock.Anything, "issue-x", "", chat.SignalReport, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		reports = append(reports, args.Get(4).(chat.Report))
	})
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalMessage, chat.Message{ID: "m2", Text: "again"}) }, time.Hour)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, 2*time.Hour)
	state := newChat("work")
	state.Request.Owner = "issue-x"
	env.ExecuteWorkflow(chat.WorkflowName, state)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(reports) < 2 || !reports[0].GaveUp || reports[0].Resumes != 3 || !strings.Contains(reports[0].Error, "cut off 4 times") || !reports[0].Idle {
		t.Fatalf("reports %+v, want the first to say the chat gave up after 3 resumes", reports)
	}
	if reports[1].Message != "m2" || reports[1].GaveUp || reports[1].Error != "" {
		t.Errorf("second report %+v, want the next message answered", reports[1])
	}
	if len(fake.turns) != 6 {
		t.Errorf("%d turns, want 4 for the first message and the resumes of the second to run on", len(fake.turns))
	}
}

// An idle chat continues as new when the deployment's target version changed,
// and asks to start on it. Nothing needs to be written to it: it looks again
// every hour. Without a change it stays, and Temporal's own suggestion
// continues as new without the upgrade.
func TestTheChatContinuesAsNewWhenTheTargetVersionChanges(t *testing.T) {
	for _, test := range []struct {
		name               string
		suggested, changed bool
		want               workflow.ContinueAsNewVersioningBehavior
		continues          bool
	}{
		{"unchanged", false, false, 0, false},
		{"suggested", true, false, workflow.ContinueAsNewVersioningBehaviorUnspecified, true},
		{"target changed", false, true, workflow.ContinueAsNewVersioningBehaviorAutoUpgrade, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			env, fake := newEnvironment(t)
			env.SetContinueAsNewSuggested(test.suggested)
			env.SetTargetWorkerDeploymentVersionChanged(test.changed)
			env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, chat.Action{}) }, 3*time.Hour)
			env.ExecuteWorkflow(chat.WorkflowName, newChat("first"))

			err := env.GetWorkflowError()
			var continued *workflow.ContinueAsNewError
			if got := errors.As(err, &continued); got != test.continues {
				t.Fatalf("continued as new = %v (%v), want %v", got, err, test.continues)
			}
			if !test.continues {
				if fake.closed != 1 {
					t.Errorf("closed %d times, want the chat to run until its close", fake.closed)
				}
				return
			}
			if continued.InitialVersioningBehavior != test.want {
				t.Errorf("versioning behavior %v, want %v", continued.InitialVersioningBehavior, test.want)
			}
			// The session stays open for the next run, which also gets the
			// message that was not answered yet.
			var next chat.State
			if err := converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next); err != nil {
				t.Fatal(err)
			}
			if fake.closed != 0 || next.Session == nil || len(next.Pending) != 1 {
				t.Errorf("closed %d times, next state %+v, want the session open and one message pending", fake.closed, next)
			}
		})
	}
}

// A chat that has been idle sees a target version that changed while it
// waited, at its next hourly look, and not before.
func TestAnIdleChatSeesAChangeOfTargetWhileItWaits(t *testing.T) {
	env, _ := newEnvironment(t)
	env.RegisterDelayedCallback(func() { env.SetTargetWorkerDeploymentVersionChanged(true) }, 90*time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("first"))

	var continued *workflow.ContinueAsNewError
	if err := env.GetWorkflowError(); !errors.As(err, &continued) {
		t.Fatalf("workflow ended with %v, want a continue as new", err)
	}
	if continued.InitialVersioningBehavior != workflow.ContinueAsNewVersioningBehaviorAutoUpgrade {
		t.Errorf("versioning behavior %v, want auto-upgrade", continued.InitialVersioningBehavior)
	}
}
