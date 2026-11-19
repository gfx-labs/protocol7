package issue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	enumspb "go.temporal.io/api/enums/v1"

	"github.com/gfx-labs/protocol7/pkg/orchestration/issue"
)

// scanner is a test environment with a scan activity that records when it ran.
type scanner struct {
	env    *testsuite.TestWorkflowEnvironment
	start  time.Time
	passes []time.Duration // since the start
	// templates are the templates the scans ran with.
	templates []issue.Template
	// during runs inside the scan with its index, to send what arrives mid-pass.
	during func(index int)
	fail   error
}

func newScanner(t *testing.T) *scanner {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	s := &scanner{env: suite.NewTestWorkflowEnvironment()}
	s.start = s.env.Now()
	s.env.RegisterWorkflowWithOptions(issue.ScannerWorkflow, workflow.RegisterOptions{Name: issue.ScannerWorkflowName})
	s.env.RegisterActivityWithOptions(func(_ context.Context, template issue.Template) ([]issue.Ref, error) {
		index := len(s.passes)
		s.passes = append(s.passes, s.env.Now().Sub(s.start))
		s.templates = append(s.templates, template)
		if s.during != nil {
			s.during(index)
		}
		if s.fail != nil {
			return nil, s.fail
		}
		return []issue.Ref{{Repo: "gfx-labs/r", Number: index + 1}}, nil
	}, activity.RegisterOptions{Name: issue.ActivityScan})
	return s
}

func (s *scanner) at(delay time.Duration, call func()) {
	s.env.RegisterDelayedCallback(call, delay)
}

func (s *scanner) signal(delay time.Duration, name string, arg any) {
	s.at(delay, func() { s.env.SignalWorkflow(name, arg) })
}

// run runs the scanner for the time and cancels it. It returns the state the
// scanner answers its query with just before.
func (s *scanner) run(t *testing.T, state issue.ScannerState, lasting time.Duration) issue.ScannerState {
	t.Helper()
	var got issue.ScannerState
	s.at(lasting, func() {
		value, err := s.env.QueryWorkflow(issue.QueryState)
		require.NoError(t, err)
		require.NoError(t, value.Get(&got))
		s.env.CancelWorkflow()
	})
	s.env.ExecuteWorkflow(issue.ScannerWorkflowName, state)
	var canceled *temporal.CanceledError
	require.ErrorAs(t, s.env.GetWorkflowError(), &canceled)
	return got
}

var scanTemplate = issue.Template{Repos: []string{"gfx-labs/r"}, Label: "selfdev", Limit: 3}

// The scanner scans at once and then on its timer, in one execution.
func TestTheScannerScansOnItsTimer(t *testing.T) {
	s := newScanner(t)
	state := s.run(t, issue.ScannerState{Template: scanTemplate}, 5*time.Minute+time.Second)

	assert.Equal(t, []time.Duration{0, issue.ScanEvery, 2 * issue.ScanEvery}, s.passes)
	assert.Equal(t, 3, state.Passes)
	assert.Equal(t, []issue.Ref{{Repo: "gfx-labs/r", Number: 3}}, state.LastStarted)
	assert.Equal(t, scanTemplate, state.Template)
	assert.False(t, state.LastPass.IsZero())
	assert.Equal(t, state.LastPass.Add(issue.ScanEvery), state.NextWake)
}

// A failed scan is recorded and the scanner goes on.
func TestTheScannerSurvivesAFailedScan(t *testing.T) {
	s := newScanner(t)
	s.fail = errors.New("github is down")
	state := s.run(t, issue.ScannerState{Template: scanTemplate}, 3*time.Minute)

	assert.Contains(t, state.LastError, "github is down")
	assert.Greater(t, len(s.passes), 3, "the failing scan is retried by the activity, and the loop scans again later")
	assert.Empty(t, state.LastStarted)
}

// A wake scans after the label settle, not at the end of the timer.
func TestAWakeScansAfterTheSettle(t *testing.T) {
	s := newScanner(t)
	s.signal(30*time.Second, issue.SignalScannerWake, nil)
	s.run(t, issue.ScannerState{Template: scanTemplate}, time.Minute)

	require.Len(t, s.passes, 2)
	assert.Equal(t, 30*time.Second+issue.ScanSettle, s.passes[1])
}

// Wakes that arrive during a pass are coalesced into one more pass.
func TestWakesDuringAPassMakeOneMorePass(t *testing.T) {
	s := newScanner(t)
	s.during = func(index int) {
		if index != 0 {
			return
		}
		for range 5 {
			s.env.SignalWorkflow(issue.SignalScannerWake, nil)
		}
	}
	s.run(t, issue.ScannerState{Template: scanTemplate}, time.Minute)

	require.Len(t, s.passes, 2, "the first pass and one for the five wakes")
	assert.Equal(t, issue.ScanSettle, s.passes[1])
}

// Pause stops intake and the timer, a wake does nothing while paused, and
// resume scans at once.
func TestPauseStopsTheScanAndResumeStartsIt(t *testing.T) {
	s := newScanner(t)
	s.signal(time.Minute, issue.SignalScannerPause, nil)
	s.signal(2*time.Minute, issue.SignalScannerWake, nil)
	var paused issue.ScannerState
	s.at(10*time.Minute, func() {
		value, err := s.env.QueryWorkflow(issue.QueryState)
		require.NoError(t, err)
		require.NoError(t, value.Get(&paused))
	})
	s.signal(11*time.Minute, issue.SignalScannerResume, nil)
	state := s.run(t, issue.ScannerState{Template: scanTemplate}, 12*time.Minute)

	assert.True(t, paused.Paused)
	assert.True(t, paused.NextWake.IsZero(), "a paused scanner has no next wake")
	assert.Equal(t, []time.Duration{0, 11 * time.Minute}, s.passes)
	assert.False(t, state.Paused)
}

// A new template is used by the next pass, and a pause survives it.
func TestAnUpdatedTemplateReachesTheNextPass(t *testing.T) {
	s := newScanner(t)
	updated := issue.Template{Repos: []string{"gfx-labs/other"}, Label: "work", Limit: 1}
	s.signal(time.Minute, issue.SignalScannerTemplate, updated)
	s.signal(90*time.Second, issue.SignalScannerTemplate, issue.Template{}) // no repository: ignored
	state := s.run(t, issue.ScannerState{Template: scanTemplate}, 3*time.Minute)

	require.Len(t, s.templates, 2)
	assert.Equal(t, scanTemplate, s.templates[0])
	assert.Equal(t, updated, s.templates[1])
	assert.Equal(t, updated, state.Template)
}

// The run continues as new after its pass budget, with the template, the
// pause and the counters in its input.
func TestTheScannerContinuesAsNewAfterItsPasses(t *testing.T) {
	s := newScanner(t)
	updated := issue.Template{Repos: []string{"gfx-labs/other"}, Label: "work", Limit: 1}
	s.signal(time.Minute, issue.SignalScannerTemplate, updated)
	s.env.ExecuteWorkflow(issue.ScannerWorkflowName, issue.ScannerState{Template: scanTemplate})

	var continued *workflow.ContinueAsNewError
	require.ErrorAs(t, s.env.GetWorkflowError(), &continued)
	assert.Equal(t, issue.ScannerWorkflowName, continued.WorkflowType.Name)
	assert.Len(t, s.passes, issue.ScannerPassesPerRun)
	var next issue.ScannerState
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next))
	assert.Equal(t, updated, next.Template)
	assert.Equal(t, issue.ScannerPassesPerRun, next.Passes)
	assert.False(t, next.NextWake.IsZero(), "the next run keeps the cadence")
}

// Temporal's suggestion and a changed target version continue the run, keep a
// paused scanner paused with its template, and a changed target asks to start
// on the target.
func TestTheScannerContinuesAsNewWhenSuggestedOrUpgraded(t *testing.T) {
	for _, test := range []struct {
		name               string
		suggested, changed bool
		want               workflow.ContinueAsNewVersioningBehavior
	}{
		{"suggested", true, false, workflow.ContinueAsNewVersioningBehaviorUnspecified},
		{"target changed", false, true, workflow.ContinueAsNewVersioningBehaviorAutoUpgrade},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newScanner(t)
			s.env.SetContinueAsNewSuggested(test.suggested)
			s.env.SetTargetWorkerDeploymentVersionChanged(test.changed)
			woken := issue.ScannerState{Template: scanTemplate, Paused: true, Passes: 7, Woken: false, LastError: "earlier"}
			s.env.ExecuteWorkflow(issue.ScannerWorkflowName, woken)

			var continued *workflow.ContinueAsNewError
			require.ErrorAs(t, s.env.GetWorkflowError(), &continued)
			assert.Equal(t, test.want, continued.InitialVersioningBehavior)
			var next issue.ScannerState
			require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next))
			assert.Equal(t, woken, next)
			assert.Empty(t, s.passes, "a paused scanner scans nothing")
		})
	}
}

// A wake that no pass answered goes into the next run.
func TestAPendingWakeSurvivesContinueAsNew(t *testing.T) {
	s := newScanner(t)
	s.env.SetContinueAsNewSuggested(true)
	s.env.ExecuteWorkflow(issue.ScannerWorkflowName, issue.ScannerState{Template: scanTemplate, Woken: true, NextWake: s.start.Add(time.Hour)})

	var continued *workflow.ContinueAsNewError
	require.ErrorAs(t, s.env.GetWorkflowError(), &continued)
	var next issue.ScannerState
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next))
	assert.True(t, next.Woken)
}

// The worker starts the scanner or reuses the one that runs, tells it the
// template, and then deletes the old schedule.
func TestEnsureScannerStartsOrReusesAndSignals(t *testing.T) {
	c := &mocks.Client{}
	c.On("ExecuteWorkflow", mock.Anything, client.StartWorkflowOptions{
		ID: issue.ScannerID, TaskQueue: issue.TaskQueue, WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}, issue.ScannerWorkflowName, issue.ScannerState{Template: scanTemplate}).Return(&mocks.WorkflowRun{}, nil).Once()
	c.On("SignalWorkflow", mock.Anything, issue.ScannerID, "", issue.SignalScannerTemplate, scanTemplate).Return(nil).Once()
	require.NoError(t, issue.EnsureScanner(context.Background(), c, scanTemplate))
	c.AssertExpectations(t)

	require.Error(t, issue.EnsureScanner(context.Background(), c, issue.Template{}), "no repository")

	// A scanner that ended between the two calls is not an error.
	c = &mocks.Client{}
	c.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&mocks.WorkflowRun{}, nil)
	c.On("SignalWorkflow", mock.Anything, issue.ScannerID, "", issue.SignalScannerTemplate, scanTemplate).Return(serviceerror.NewNotFound("gone"))
	require.NoError(t, issue.EnsureScanner(context.Background(), c, scanTemplate))

	c = &mocks.Client{}
	c.On("ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("down"))
	require.Error(t, issue.EnsureScanner(context.Background(), c, scanTemplate))
}

func TestRemoveScheduleDeletesWhatExists(t *testing.T) {
	for name, test := range map[string]struct {
		err     error
		removed bool
		fails   bool
	}{
		"deleted": {removed: true},
		"absent":  {err: serviceerror.NewNotFound("no schedule")},
		"failed":  {err: serviceerror.NewUnavailable("down"), fails: true},
	} {
		t.Run(name, func(t *testing.T) {
			c, schedules, handle := &mocks.Client{}, &mocks.ScheduleClient{}, &mocks.ScheduleHandle{}
			c.On("ScheduleClient").Return(schedules)
			schedules.On("GetHandle", mock.Anything, issue.ScheduleID).Return(handle)
			handle.On("Delete", mock.Anything).Return(test.err).Once()
			removed, err := issue.RemoveSchedule(context.Background(), c)
			assert.Equal(t, test.removed, removed)
			assert.Equal(t, test.fails, err != nil)
		})
	}
}
