package learning_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"
	base "github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/orchestration/learning"
	"github.com/gfx-labs/protocol7/pkg/orchestration/learning/learningtest"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

var session = &navi.ChatRequest{Host: "host", Backend: "k8s", Project: "core", Repositories: []*navi.Repository{{Name: "protocol7", Url: "git@github.com:gfx-labs/protocol7.git"}}}

// pass is a run of the pass's workflow as Temporal's visibility holds it.
type pass struct {
	id      string
	started time.Time
	result  *learning.CurateResult
}

// passes answers the visibility queries about passes, and the log's about
// entries.
func passes(t *testing.T, log *learningtest.Log, running, completed []pass) client.Client {
	c := &mocks.Client{}
	data := converter.GetDefaultDataConverter()
	build := func(list []pass) *workflowservice.ListWorkflowExecutionsResponse {
		response := &workflowservice.ListWorkflowExecutionsResponse{}
		for _, p := range list {
			info := &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: p.id}, StartTime: timestamppb.New(p.started)}
			if p.result != nil {
				payload, err := data.ToPayload(*p.result)
				require.NoError(t, err)
				info.Memo = &commonpb.Memo{Fields: map[string]*commonpb.Payload{learning.MemoCurate: payload}}
			}
			response.Executions = append(response.Executions, info)
		}
		return response
	}
	c.On("ListWorkflow", mock.Anything, mock.Anything).Return(func(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
		switch {
		case strings.Contains(request.GetQuery(), learning.CurateWorkflowName) && strings.Contains(request.GetQuery(), "'Running'"):
			return build(running), nil
		case strings.Contains(request.GetQuery(), learning.CurateWorkflowName):
			return build(completed), nil
		}
		return log.Client.ListWorkflow(ctx, request)
	})
	return c
}

func recorded(id string, at time.Time) learning.Entry {
	e := entry(id, "Learning "+id, selfdev)
	e.State, e.Recorded = learning.Open, at
	return e
}

// The check opens a pass only when a learning is open and, for a scheduled
// run, one was recorded since the last pass that ran. It never opens one
// while an earlier pass runs, and a manual run needs only an open learning.
func TestCurateCheck(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	last := now.Add(-3 * time.Hour)
	older, newer := now.Add(-5*time.Hour), now.Add(-time.Hour)
	ran := &learning.CurateResult{Pass: "learning-curate-0", Cursor: last}
	skipped := &learning.CurateResult{Pass: "skip", Cursor: time.Time{}}
	input := learning.CheckInput{Pass: "learning-curate-1", Started: now, Trigger: learning.TriggerSchedule}

	for name, test := range map[string]struct {
		entries   []learning.Entry
		running   []pass
		completed []pass
		input     func(learning.CheckInput) learning.CheckInput
		skip      string
		open, new int
	}{
		"nothing is open": {skip: "no learning is open"},
		"nothing new since the last pass": {
			entries: []learning.Entry{recorded("a1", older)}, completed: []pass{{id: "p0", result: ran}},
			skip: "0 of 1 open learnings were recorded since the last pass",
		},
		"one new": {
			entries: []learning.Entry{recorded("a1", older), recorded("a2", newer)}, completed: []pass{{id: "p0", result: ran}},
			open: 2, new: 1,
		},
		"a pass that skipped is not the last pass": {
			entries: []learning.Entry{recorded("a1", older)}, completed: []pass{{id: "skip", result: skipped}, {id: "nomemo"}},
			open: 1, new: 1,
		},
		"the newest cursor of the passes read is the last": {
			entries:   []learning.Entry{recorded("a1", older), recorded("a2", newer)},
			completed: []pass{{id: "p0", result: &learning.CurateResult{Cursor: now.Add(-6 * time.Hour)}}, {id: "p1", result: ran}},
			open:      2, new: 1,
		},
		"no pass has run": {entries: []learning.Entry{recorded("a1", older)}, open: 1, new: 1},
		"an earlier pass runs": {
			entries: []learning.Entry{recorded("a1", newer)}, running: []pass{{id: "learning-curate-0", started: now.Add(-time.Minute)}},
			skip: "the pass learning-curate-0 is running",
		},
		"a later pass runs, and yields": {
			entries: []learning.Entry{recorded("a1", newer)}, running: []pass{{id: "learning-curate-2", started: now.Add(time.Second)}},
			open: 1, new: 1,
		},
		"itself running is no other": {
			entries: []learning.Entry{recorded("a1", newer)}, running: []pass{{id: "learning-curate-1", started: now}},
			open: 1, new: 1,
		},
		"a manual run needs only an open learning": {
			entries: []learning.Entry{recorded("a1", older)}, completed: []pass{{id: "p0", result: ran}},
			input: func(in learning.CheckInput) learning.CheckInput { in.Trigger = learning.TriggerManual; return in },
			open:  1,
		},
		"a manual run does not run beside a pass": {
			entries: []learning.Entry{recorded("a1", older)}, running: []pass{{id: "learning-curate-0", started: now.Add(-time.Minute)}},
			input: func(in learning.CheckInput) learning.CheckInput { in.Trigger = learning.TriggerManual; return in },
			skip:  "is running",
		},
		"min new": {
			entries: []learning.Entry{recorded("a1", older), recorded("a2", newer)}, completed: []pass{{id: "p0", result: ran}},
			input: func(in learning.CheckInput) learning.CheckInput { in.Config.MinNew = 2; return in },
			skip:  "1 of 2 open learnings",
		},
	} {
		t.Run(name, func(t *testing.T) {
			log := learningtest.New(t)
			for _, e := range test.entries {
				log.Add(e)
			}
			// Settled entries are not open.
			settled := recorded("z1", newer)
			settled.State = learning.Applied
			log.Add(settled)
			activities := &learning.CurateActivities{Store: learning.Store{Client: passes(t, log, test.running, test.completed)}, Now: func() time.Time { return now }}
			in := input
			if test.input != nil {
				in = test.input(in)
			}
			plan, err := activities.Check(context.Background(), in)
			require.NoError(t, err)
			if test.skip != "" {
				assert.Contains(t, plan.Skip, test.skip)
				return
			}
			assert.Empty(t, plan.Skip)
			assert.Len(t, plan.Open, test.open)
			assert.Equal(t, test.new, plan.New)
			assert.Equal(t, now, plan.At)
			assert.Equal(t, now, plan.Cursor)
			for i := 1; i < len(plan.Open); i++ {
				assert.False(t, plan.Open[i].Recorded.Before(plan.Open[i-1].Recorded), "oldest first")
			}
		})
	}
}

// A backlog beyond MaxEntries is cut oldest first, and the cursor stops at the
// last entry given, so that the rest are new to the next pass.
func TestCurateCheckCutsTheBacklog(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	log := learningtest.New(t)
	for i, id := range []string{"a1", "a2", "a3", "a4"} {
		log.Add(recorded(id, now.Add(-time.Duration(10-i)*time.Hour)))
	}
	activities := &learning.CurateActivities{Store: learning.Store{Client: passes(t, log, nil, nil)}, Now: func() time.Time { return now }}
	plan, err := activities.Check(context.Background(), learning.CheckInput{Pass: "learning-curate-1", Started: now, Config: learning.CurateConfig{MaxEntries: 3}})
	require.NoError(t, err)
	require.Len(t, plan.Open, 3)
	assert.Equal(t, 4, plan.OpenTotal)
	assert.Equal(t, "a1", plan.Open[0].ID)
	assert.Equal(t, plan.Open[2].Recorded, plan.Cursor)
	assert.Less(t, plan.Cursor, plan.At)
	// The next pass sees the fourth as new.
	next := &learning.CurateActivities{Store: learning.Store{Client: passes(t, log, nil, []pass{{id: "p", result: &learning.CurateResult{Cursor: plan.Cursor}}})}, Now: func() time.Time { return now }}
	second, err := next.Check(context.Background(), learning.CheckInput{Pass: "learning-curate-2", Started: now})
	require.NoError(t, err)
	assert.Equal(t, 1, second.New)
}

func TestCurateOutcome(t *testing.T) {
	log := learningtest.New(t)
	ctx := context.Background()
	activities := log.Activities()
	for _, id := range []string{"a1", "a2", "a3", "a4"} {
		e := recorded(id, time.Now())
		log.Add(e)
	}
	for id, decision := range map[string]learning.Decision{
		"a1": {Action: "apply", PR: "https://github.com/gfx-labs/protocol7/pull/1"},
		"a2": {Action: "cover", Note: "AGENTS.md says it"},
		"a3": {Action: "reject", Note: "not general"},
	} {
		_, err := activities.Decide(ctx, learning.DecideRequest{ID: id, Decision: decision})
		require.NoError(t, err)
	}
	curate := &learning.CurateActivities{Store: log.Store()}
	got, err := curate.Outcome(ctx, learning.OutcomeRequest{IDs: []string{"a1", "a2", "a3", "a4"}})
	require.NoError(t, err)
	require.Len(t, got, 4)
	assert.Equal(t, learning.Settlement{ID: "a1", Title: "Learning a1", State: learning.Applied, PR: "https://github.com/gfx-labs/protocol7/pull/1"}, got[0])
	assert.Equal(t, learning.Covered, got[1].State)
	assert.Equal(t, "AGENTS.md says it", got[1].Note)
	assert.Equal(t, learning.Rejected, got[2].State)
	assert.Equal(t, learning.Open, got[3].State)
	_, err = curate.Outcome(ctx, learning.OutcomeRequest{IDs: []string{"nobody"}})
	assert.Error(t, err)

	result := learning.CurateResult{Settlements: got}
	assert.Equal(t, 1, result.Count(learning.Applied))
	assert.Equal(t, 1, result.Count(learning.Covered))
	assert.Equal(t, 1, result.Count(learning.Rejected))
	assert.Equal(t, 1, result.Count(learning.Open))
}

// stubChat stands for the base chat in the pass's tests: it keeps its start
// state and ends when it is told to close.
type stubChat struct {
	started chan *navi.ChatState
	closed  bool
}

func (s *stubChat) run(ctx workflow.Context, start *navi.ChatState) (*base.Report, error) {
	s.started <- start
	workflow.GetSignalChannel(ctx, base.SignalClose).Receive(ctx, nil)
	s.closed = true
	return &base.Report{}, nil
}

func newPass(t *testing.T) (*testsuite.TestWorkflowEnvironment, *stubChat) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetDataConverter(p7.DataConverter())
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: learning.CurateWorkflowPrefix + "test"})
	chat := &stubChat{started: make(chan *navi.ChatState, 1)}
	env.RegisterWorkflowWithOptions(learning.CurateWorkflow, workflow.RegisterOptions{Name: learning.CurateWorkflowName})
	env.RegisterWorkflowWithOptions(chat.run, workflow.RegisterOptions{Name: base.WorkflowName})
	env.RegisterActivityWithOptions(func(context.Context, learning.CheckInput) (learning.Plan, error) { return learning.Plan{}, nil }, activity.RegisterOptions{Name: learning.ActivityCurateCheck})
	env.RegisterActivityWithOptions(func(context.Context, learning.BriefRequest) (learning.Brief, error) { return learning.Brief{}, nil }, activity.RegisterOptions{Name: learning.ActivityCurateBrief})
	env.RegisterActivityWithOptions(func(context.Context, learning.OutcomeRequest) ([]learning.Settlement, error) { return nil, nil }, activity.RegisterOptions{Name: learning.ActivityCurateOutcome})
	return env, chat
}

var planned = learning.Plan{
	Open:      []learning.Entry{recorded("a1", time.Date(2026, 11, 22, 8, 0, 0, 0, time.UTC)), recorded("a2", time.Date(2026, 11, 22, 9, 0, 0, 0, time.UTC))},
	OpenTotal: 3, Cursor: time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC),
}

// A pass that finds nothing to do ends with the reason, and starts no chat.
func TestCurateWorkflowSkips(t *testing.T) {
	env, chat := newPass(t)
	env.OnActivity(learning.ActivityCurateCheck, mock.Anything, mock.Anything).Return(learning.Plan{Skip: "no learning is open"}, nil)
	env.ExecuteWorkflow(learning.CurateWorkflowName, learning.CurateInput{Session: session})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result learning.CurateResult
	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "no learning is open", result.Skipped)
	assert.Equal(t, learning.TriggerSchedule, result.Trigger)
	assert.Empty(t, result.Chat)
	assert.Empty(t, chat.started)
	env.AssertNotCalled(t, "ExecuteChildWorkflow")
}

// The pass runs one chat of the project with the app's brief as its first
// message, owned by the pass and no handler, with the session's reach. It
// ends when the chat reports its turn, closes the chat, and returns what
// became of each entry with the curator's last message.
func TestCurateWorkflowRunsTheCurator(t *testing.T) {
	env, chat := newPass(t)
	var check learning.CheckInput
	var brief learning.BriefRequest
	env.OnActivity(learning.ActivityCurateCheck, mock.Anything, mock.Anything).Return(func(_ context.Context, in learning.CheckInput) (learning.Plan, error) {
		check = in
		return planned, nil
	})
	env.OnActivity(learning.ActivityCurateBrief, mock.Anything, mock.Anything).Return(func(_ context.Context, in learning.BriefRequest) (learning.Brief, error) {
		brief = in
		return learning.Brief{Title: "Curate learnings", Message: "Settle these."}, nil
	})
	env.OnActivity(learning.ActivityCurateOutcome, mock.Anything, mock.Anything).Return([]learning.Settlement{
		{ID: "a1", Title: "A", State: learning.Applied, PR: "https://github.com/gfx-labs/protocol7/pull/1"},
		{ID: "a2", Title: "B", State: learning.Open},
	}, nil)
	var chatID string
	env.RegisterDelayedCallback(func() {
		start := <-chat.started
		chatID = start.GetRequest().GetId()
		env.SignalWorkflow(base.SignalReport, &base.Report{ChatId: chatID, Message: chatID, Started: true})
		env.SignalWorkflow(base.SignalReport, &base.Report{ChatId: "someone-else", Message: "m", Text: "not mine"})
		env.SignalWorkflow(base.SignalReport, &base.Report{ChatId: chatID, Message: chatID, Text: "Applied a1, left a2.", Idle: true})
	}, time.Hour)
	env.ExecuteWorkflow(learning.CurateWorkflowName, learning.CurateInput{Session: session, Trigger: learning.TriggerManual, Config: learning.CurateConfig{MaxEntries: 5}})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result learning.CurateResult
	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "Applied a1, left a2.", result.Note)
	assert.Equal(t, chatID, result.Chat)
	assert.Equal(t, 1, result.Count(learning.Applied))
	assert.Equal(t, 1, result.Count(learning.Open))
	assert.Equal(t, planned.Cursor, result.Cursor)
	assert.Equal(t, learning.TriggerManual, result.Trigger)
	assert.True(t, chat.closed, "the pass closes the chat")

	assert.Equal(t, result.Pass, check.Pass)
	assert.Equal(t, 5, check.Config.MaxEntries)
	assert.Equal(t, learning.TriggerManual, check.Trigger)
	assert.Len(t, brief.Entries, 2)
	assert.Equal(t, 1, brief.Left, "the entries the pass is not given")
	assert.Equal(t, chatID, brief.Chat, "the brief names the chat it is for")
}

// The curator's chat is the session's request, with the pass as its owner and
// a title and a message from the app, and it is recognised as the curator's.
func TestCurateWorkflowStartsTheChatOfTheSession(t *testing.T) {
	env, chat := newPass(t)
	env.OnActivity(learning.ActivityCurateCheck, mock.Anything, mock.Anything).Return(planned, nil)
	env.OnActivity(learning.ActivityCurateBrief, mock.Anything, mock.Anything).Return(learning.Brief{Title: "Curate learnings", Message: "Settle these."}, nil)
	env.OnActivity(learning.ActivityCurateOutcome, mock.Anything, mock.Anything).Return([]learning.Settlement{{ID: "a1", State: learning.Covered, Note: "x"}}, nil)
	greedy := proto.Clone(session).(*navi.ChatRequest)
	greedy.Handler, greedy.Parent, greedy.RequestTargets = true, "someone", []string{"oku"}
	var start *navi.ChatState
	env.RegisterDelayedCallback(func() {
		start = <-chat.started
		env.SignalWorkflow(base.SignalReport, &base.Report{ChatId: start.GetRequest().GetId(), Message: "m", Text: "done"})
	}, time.Hour)
	env.ExecuteWorkflow(learning.CurateWorkflowName, learning.CurateInput{Session: greedy})
	require.NoError(t, env.GetWorkflowError())
	request := start.GetRequest()
	assert.Equal(t, "Curate learnings", request.GetTitle())
	assert.True(t, strings.HasPrefix(request.GetOwner(), learning.CurateWorkflowPrefix), request.GetOwner())
	assert.True(t, learning.IsCuratorChat(request))
	assert.False(t, request.GetHandler(), "the curator is not the escalation handler")
	assert.Empty(t, request.GetParent())
	assert.Empty(t, request.GetRequestTargets())
	assert.Equal(t, "core", request.GetProject())
	assert.Equal(t, "host", request.GetHost())
	assert.Equal(t, "git@github.com:gfx-labs/protocol7.git", request.GetRepositories()[0].GetUrl())
	require.Len(t, start.GetPending(), 1)
	assert.Equal(t, "Settle these.", start.GetPending()[0].GetText())
	assert.True(t, greedy.Handler, "the input's session is not changed")
	assert.False(t, learning.IsCuratorChat(&navi.ChatRequest{Owner: "chat-abc"}))
	assert.False(t, learning.IsCuratorChat(nil))
}

// A pass whose curator never ends its turn fails after the budget, and so
// does one that ends with no note and nothing settled. A failed pass keeps no
// memo, so it is not the last pass.
func TestCurateWorkflowFails(t *testing.T) {
	t.Run("the turn runs out of time", func(t *testing.T) {
		env, chat := newPass(t)
		env.OnActivity(learning.ActivityCurateCheck, mock.Anything, mock.Anything).Return(planned, nil)
		env.OnActivity(learning.ActivityCurateBrief, mock.Anything, mock.Anything).Return(learning.Brief{Title: "t", Message: "m"}, nil)
		env.OnActivity(learning.ActivityCurateOutcome, mock.Anything, mock.Anything).Return([]learning.Settlement{{ID: "a1", State: learning.Open}}, nil)
		env.ExecuteWorkflow(learning.CurateWorkflowName, learning.CurateInput{Session: session, Config: learning.CurateConfig{TurnBudget: 30 * time.Minute}})
		require.True(t, env.IsWorkflowCompleted())
		err := env.GetWorkflowError()
		require.Error(t, err)
		assert.ErrorContains(t, err, "did not end its turn within 30m")
		assert.True(t, chat.closed, "the chat is closed all the same")
	})
	t.Run("no note and nothing settled", func(t *testing.T) {
		env, chat := newPass(t)
		env.OnActivity(learning.ActivityCurateCheck, mock.Anything, mock.Anything).Return(planned, nil)
		env.OnActivity(learning.ActivityCurateBrief, mock.Anything, mock.Anything).Return(learning.Brief{Title: "t", Message: "m"}, nil)
		env.OnActivity(learning.ActivityCurateOutcome, mock.Anything, mock.Anything).Return([]learning.Settlement{{ID: "a1", State: learning.Open}, {ID: "a2", State: learning.Proposed}}, nil)
		env.RegisterDelayedCallback(func() {
			start := <-chat.started
			env.SignalWorkflow(base.SignalReport, &base.Report{ChatId: start.GetRequest().GetId(), Message: "m"})
		}, time.Minute)
		env.ExecuteWorkflow(learning.CurateWorkflowName, learning.CurateInput{Session: session})
		require.Error(t, env.GetWorkflowError())
		assert.ErrorContains(t, env.GetWorkflowError(), "no pass note")
	})
	t.Run("a failed turn", func(t *testing.T) {
		env, chat := newPass(t)
		env.OnActivity(learning.ActivityCurateCheck, mock.Anything, mock.Anything).Return(planned, nil)
		env.OnActivity(learning.ActivityCurateBrief, mock.Anything, mock.Anything).Return(learning.Brief{Title: "t", Message: "m"}, nil)
		env.OnActivity(learning.ActivityCurateOutcome, mock.Anything, mock.Anything).Return([]learning.Settlement{{ID: "a1", State: learning.Applied, PR: "https://x.example/p/1"}}, nil)
		env.RegisterDelayedCallback(func() {
			start := <-chat.started
			env.SignalWorkflow(base.SignalReport, &base.Report{ChatId: start.GetRequest().GetId(), Message: "m", Text: "I did some", Error: "the session died"})
		}, time.Minute)
		env.ExecuteWorkflow(learning.CurateWorkflowName, learning.CurateInput{Session: session})
		require.Error(t, env.GetWorkflowError())
		assert.ErrorContains(t, env.GetWorkflowError(), "the session died")
	})
	t.Run("no session", func(t *testing.T) {
		env, _ := newPass(t)
		env.ExecuteWorkflow(learning.CurateWorkflowName, learning.CurateInput{})
		require.Error(t, env.GetWorkflowError())
	})
}

// The schedule is made once with the skip overlap policy, and a worker that
// starts again updates its interval and action and keeps a pause.
func TestEnsureSchedule(t *testing.T) {
	ctx := context.Background()
	input := learning.CurateInput{Session: session, Config: learning.CurateConfig{Every: 2 * time.Hour}}

	t.Run("create", func(t *testing.T) {
		c, schedules, handle := scheduleMocks()
		handle.On("Describe", mock.Anything).Return(nil, serviceerror.NewNotFound("no schedule"))
		var options client.ScheduleOptions
		schedules.On("Create", mock.Anything, mock.Anything).Run(func(args mock.Arguments) { options = args.Get(1).(client.ScheduleOptions) }).Return(handle, nil)
		require.NoError(t, learning.EnsureSchedule(ctx, c, input))
		assert.Equal(t, learning.CurateScheduleID, options.ID)
		assert.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, options.Overlap)
		assert.Equal(t, 2*time.Hour, options.Spec.Intervals[0].Every)
		action := options.Action.(*client.ScheduleWorkflowAction)
		assert.Equal(t, learning.CurateWorkflowName, action.Workflow)
		assert.Equal(t, learning.TaskQueue, action.TaskQueue)
		sent := action.Args[0].(learning.CurateInput)
		assert.Equal(t, learning.TriggerSchedule, sent.Trigger)
		assert.Equal(t, 30, sent.Config.MaxEntries, "the defaults are in the action")
		assert.Equal(t, "host", sent.Session.Host)
	})
	t.Run("update", func(t *testing.T) {
		c, _, handle := scheduleMocks()
		handle.On("Describe", mock.Anything).Return(&client.ScheduleDescription{}, nil)
		var updated *client.ScheduleUpdate
		handle.On("Update", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			options := args.Get(1).(client.ScheduleUpdateOptions)
			var err error
			updated, err = options.DoUpdate(client.ScheduleUpdateInput{Description: client.ScheduleDescription{Schedule: client.Schedule{State: &client.ScheduleState{Paused: true, Note: "a person paused it"}, Policy: &client.SchedulePolicies{}}}})
			require.NoError(t, err)
		}).Return(nil)
		require.NoError(t, learning.EnsureSchedule(ctx, c, input))
		assert.True(t, updated.Schedule.State.Paused, "a pause that a person set stays")
		assert.Equal(t, 2*time.Hour, updated.Schedule.Spec.Intervals[0].Every)
		assert.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, updated.Schedule.Policy.Overlap)
	})
	t.Run("no session", func(t *testing.T) {
		c, _, _ := scheduleMocks()
		require.Error(t, learning.EnsureSchedule(ctx, c, learning.CurateInput{}))
	})
}

// Turning the curator off deletes its schedule, and a namespace without one
// is not an error.
func TestRemoveSchedule(t *testing.T) {
	ctx := context.Background()
	c, _, handle := scheduleMocks()
	handle.On("Delete", mock.Anything).Return(nil).Once()
	removed, err := learning.RemoveSchedule(ctx, c)
	require.NoError(t, err)
	assert.True(t, removed)
	handle.On("Delete", mock.Anything).Return(serviceerror.NewNotFound("none")).Once()
	removed, err = learning.RemoveSchedule(ctx, c)
	require.NoError(t, err)
	assert.False(t, removed)
	handle.On("Delete", mock.Anything).Return(assert.AnError).Once()
	_, err = learning.RemoveSchedule(ctx, c)
	assert.Error(t, err)
}

func scheduleMocks() (*mocks.Client, *mocks.ScheduleClient, *mocks.ScheduleHandle) {
	c, schedules, handle := &mocks.Client{}, &mocks.ScheduleClient{}, &mocks.ScheduleHandle{}
	c.On("ScheduleClient").Return(schedules)
	schedules.On("GetHandle", mock.Anything, learning.CurateScheduleID).Return(handle)
	return c, schedules, handle
}

// A manual pass starts with a fresh ID of the pass prefix and the manual
// trigger.
func TestStartPass(t *testing.T) {
	c := &mocks.Client{}
	var options client.StartWorkflowOptions
	var args []any
	c.On("ExecuteWorkflow", mock.Anything, mock.Anything, learning.CurateWorkflowName, mock.Anything).Run(func(a mock.Arguments) {
		options = a.Get(1).(client.StartWorkflowOptions)
		args = []any{a.Get(3)}
	}).Return(&mocks.WorkflowRun{}, nil)
	id, err := learning.StartPass(context.Background(), c, learning.CurateInput{Session: session})
	require.NoError(t, err)
	assert.Equal(t, options.ID, id)
	assert.True(t, strings.HasPrefix(id, learning.CurateWorkflowPrefix))
	assert.Equal(t, learning.TriggerManual, args[0].(learning.CurateInput).Trigger)
}

func TestRegisterCurate(t *testing.T) {
	r := &names{}
	require.NoError(t, learning.RegisterCurate(r, &learning.CurateActivities{}))
	assert.Equal(t, []string{"selfdev.LearningCurate", "LearningCurateCheck", "LearningCurateOutcome"}, r.names)
	assert.Error(t, learning.RegisterCurate(nil, &learning.CurateActivities{}))
	assert.Error(t, learning.RegisterCurate(r, nil))
}
