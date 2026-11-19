package issue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/orchestration/upgrade"
)

// The scanner is the one long-lived workflow that takes labelled issues. It
// replaced the Temporal Schedule `selfdev-issues`, which started a workflow
// every two minutes. The scanner loops: a pass runs the IssueScan activity,
// then it waits for the ScanEvery timer or a wake signal. It continues as new
// when Temporal suggests it, after ScannerPassesPerRun passes, and when the
// worker build changes.
const (
	// ScannerWorkflowName is the registered name of the scanner.
	ScannerWorkflowName = navi.IssueScannerWorkflow
	// ScannerID is the workflow ID of the scanner. There is one.
	ScannerID = navi.IssueScannerID

	// SignalScannerWake asks for a pass at once. The web server sends it on
	// an `issues/labeled` webhook for the configured label. Wakes that arrive
	// during a pass make one more pass. It carries no data.
	SignalScannerWake = navi.SignalScannerWake
	// SignalScannerPause stops intake, and SignalScannerResume starts it
	// again with a pass at once. Neither carries data.
	SignalScannerPause  = navi.SignalScannerPause
	SignalScannerResume = navi.SignalScannerResume
	// SignalScannerTemplate carries a Template. The scanner applies it at its
	// next pass. The worker sends it at startup.
	SignalScannerTemplate = navi.SignalScannerTemplate

	// ScannerPassesPerRun bounds the passes of one run, so that its history
	// stays small whatever Temporal suggests.
	ScannerPassesPerRun = 200
)

// ScannerState is everything the scanner keeps. It is the workflow's input,
// so it is also what a run hands to the next on Continue-As-New, and the
// answer of its `state` query.
type ScannerState = navi.ScannerState

// ScannerWorkflow is the long-lived scan loop.
func ScannerWorkflow(ctx workflow.Context, state ScannerState) error {
	if err := workflow.SetQueryHandler(ctx, QueryState, func() (ScannerState, error) { return state, nil }); err != nil {
		return err
	}
	wakes := workflow.GetSignalChannel(ctx, SignalScannerWake)
	pauses := workflow.GetSignalChannel(ctx, SignalScannerPause)
	resumes := workflow.GetSignalChannel(ctx, SignalScannerResume)
	templates := workflow.GetSignalChannel(ctx, SignalScannerTemplate)

	apply := func(kind string, template Template) {
		switch kind {
		case SignalScannerWake:
			state.Woken = state.Woken || !state.Paused
		case SignalScannerPause:
			state.Paused, state.Woken, state.NextWake = true, false, time.Time{}
		case SignalScannerResume:
			state.Paused, state.NextWake = false, time.Time{}
		case SignalScannerTemplate:
			// A template with no repository would scan nothing and could
			// not be told from a mistake.
			if len(template.Repos) > 0 {
				state.Template = template
			}
		}
	}
	take := func() {
		for wakes.ReceiveAsync(nil) {
			apply(SignalScannerWake, Template{})
		}
		for pauses.ReceiveAsync(nil) {
			apply(SignalScannerPause, Template{})
		}
		for resumes.ReceiveAsync(nil) {
			apply(SignalScannerResume, Template{})
		}
		var template Template
		for templates.ReceiveAsync(&template) {
			apply(SignalScannerTemplate, template)
		}
	}
	pass := func(settle bool) error {
		if settle {
			// GitHub's label-filtered list can lag a label that was just
			// written, which is what a wake announces (#63).
			if err := workflow.Sleep(ctx, scanSettle); err != nil {
				return err
			}
			// A template or a pause that came in meanwhile counts.
			take()
			if state.Paused {
				return nil
			}
		}
		var started []Ref
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, scanOptions), ActivityScan, state.Template).Get(ctx, &started)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		state.Passes++
		state.LastPass = workflow.Now(ctx)
		state.LastStarted, state.LastError = started, ""
		if err != nil {
			// The next pass tries again. A failed pass is no reason to stop
			// the scanner.
			state.LastError = cause(err)
		}
		state.NextWake = state.LastPass.Add(ScanEvery)
		return nil
	}

	runPasses := 0
	for {
		take()
		if upgrade.Due(ctx) || runPasses >= ScannerPassesPerRun {
			// The state carries a wake that no pass answered.
			return upgrade.ContinueAsNew(ctx, ScannerWorkflowName, state)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !state.Paused && (state.Woken || !workflow.Now(ctx).Before(state.NextWake)) {
			settle := state.Woken
			state.Woken = false
			if err := pass(settle); err != nil {
				return err
			}
			runPasses++
			// Signals that came in during the pass are taken at the top.
			continue
		}
		timerCtx, cancel := workflow.WithCancel(ctx)
		selector := workflow.NewSelector(ctx)
		if !state.Paused {
			selector.AddFuture(workflow.NewTimer(timerCtx, state.NextWake.Sub(workflow.Now(ctx))), func(workflow.Future) {})
		}
		// The signals are taken at the top of the loop. The callbacks only
		// put them back by applying them here.
		selector.AddReceive(wakes, func(channel workflow.ReceiveChannel, _ bool) {
			channel.Receive(ctx, nil)
			apply(SignalScannerWake, Template{})
		})
		selector.AddReceive(pauses, func(channel workflow.ReceiveChannel, _ bool) {
			channel.Receive(ctx, nil)
			apply(SignalScannerPause, Template{})
		})
		selector.AddReceive(resumes, func(channel workflow.ReceiveChannel, _ bool) {
			channel.Receive(ctx, nil)
			apply(SignalScannerResume, Template{})
		})
		selector.AddReceive(templates, func(channel workflow.ReceiveChannel, _ bool) {
			var template Template
			channel.Receive(ctx, &template)
			apply(SignalScannerTemplate, template)
		})
		selector.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) {})
		selector.Select(ctx)
		cancel()
	}
}

// EnsureScanner starts the scanner with the template if no scanner runs, and
// tells a running one the template, which it applies at its next pass. It
// does not touch a paused scanner's pause.
func EnsureScanner(ctx context.Context, temporalClient client.Client, template Template) error {
	if len(template.Repos) == 0 {
		return errors.New("the issue scanner needs at least one repository")
	}
	_, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: ScannerID, TaskQueue: TaskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}, ScannerWorkflowName, ScannerState{Template: template})
	if err != nil {
		return fmt.Errorf("start the scanner %s: %w", ScannerID, err)
	}
	// Redundant for a scanner that was just started, and the only way a new
	// template reaches one that was running.
	err = temporalClient.SignalWorkflow(ctx, ScannerID, "", SignalScannerTemplate, template)
	if _, gone := errors.AsType[*serviceerror.NotFound](err); err != nil && !gone {
		return fmt.Errorf("send the template to the scanner %s: %w", ScannerID, err)
	}
	return nil
}

// RemoveSchedule deletes the schedule `selfdev-issues` that the scanner
// replaced. It is not an error that there is none. Runs the schedule started
// are left to end by themselves.
func RemoveSchedule(ctx context.Context, temporalClient client.Client) (removed bool, err error) {
	err = temporalClient.ScheduleClient().GetHandle(ctx, ScheduleID).Delete(ctx)
	if _, gone := errors.AsType[*serviceerror.NotFound](err); gone {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("delete the schedule %s: %w", ScheduleID, err)
	}
	return true, nil
}
