package learning

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"

	base "github.com/gfx-labs/protocol7/pkg/orchestration/chat"
)

// CurateActivities are the pass's reads of the log and of Temporal. They run
// in the app's chat worker, which holds the Temporal client of the project's
// own namespace. They hold no prompt text: the app's ActivityCurateBrief does.
type CurateActivities struct {
	Store Store
	// Now is the clock. Nil is time.Now.
	Now func() time.Time
}

func (a *CurateActivities) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

const (
	passesRunning   = "WorkflowType = '" + CurateWorkflowName + "' AND ExecutionStatus = 'Running'"
	passesCompleted = "WorkflowType = '" + CurateWorkflowName + "' AND ExecutionStatus = 'Completed'"
	// lastPassWindow is how many completed passes are read to find the last
	// that ran: the ones that skipped hold no result, and the runs of a
	// schedule are many.
	lastPassWindow = 50
)

// Check says whether a pass is due: no earlier pass runs, an entry is open
// and, for a scheduled run, enough were recorded since the last pass that ran.
func (a *CurateActivities) Check(ctx context.Context, input CheckInput) (Plan, error) {
	config := input.Config.WithDefaults()
	plan := Plan{At: a.now().UTC()}
	plan.Cursor = plan.At

	running, err := a.Store.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{PageSize: 20, Query: passesRunning})
	if err != nil {
		return plan, err
	}
	for _, execution := range running.GetExecutions() {
		id := execution.GetExecution().GetWorkflowId()
		if id == input.Pass {
			continue
		}
		// Two runs that start together must not both skip: the later yields.
		started := execution.GetStartTime().AsTime()
		if started.Before(input.Started) || (started.Equal(input.Started) && id < input.Pass) {
			plan.Skip = fmt.Sprintf("the pass %s is running", id)
			return plan, nil
		}
	}

	open, err := a.Store.Open(ctx)
	if err != nil {
		return plan, err
	}
	plan.OpenTotal = len(open)
	if len(open) == 0 {
		plan.Skip = "no learning is open"
		return plan, nil
	}
	last, err := a.lastPass(ctx)
	if err != nil {
		return plan, err
	}
	plan.Since = last
	for _, entry := range open {
		if entry.Recorded.After(last) {
			plan.New++
		}
	}
	if input.Trigger != TriggerManual && plan.New < config.MinNew {
		plan.Skip = fmt.Sprintf("%d of %d open learnings were recorded since the last pass (%s), and a pass needs %d", plan.New, len(open), sinceText(last), config.MinNew)
		return plan, nil
	}
	plan.Open = open
	if len(open) > config.MaxEntries {
		plan.Open = open[:config.MaxEntries]
		// The entries not given are new to the next pass.
		plan.Cursor = plan.Open[len(plan.Open)-1].Recorded.UTC()
	}
	return plan, nil
}

func sinceText(t time.Time) string {
	if t.IsZero() {
		return "there was none"
	}
	return t.UTC().Format(time.RFC3339)
}

// lastPass is the cursor of the newest completed pass that ran, or the zero
// time.
func (a *CurateActivities) lastPass(ctx context.Context) (time.Time, error) {
	response, err := a.Store.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{PageSize: lastPassWindow, Query: passesCompleted})
	if err != nil {
		return time.Time{}, err
	}
	data := converter.GetDefaultDataConverter()
	var last time.Time
	for _, execution := range response.GetExecutions() {
		payload, ok := execution.GetMemo().GetFields()[MemoCurate]
		if !ok {
			continue
		}
		var result CurateResult
		if data.FromPayload(payload, &result) != nil {
			continue
		}
		if result.Cursor.After(last) {
			last = result.Cursor
		}
	}
	return last, nil
}

// Outcome reads each entry as the pass left it. A covered or rejected entry's
// workflow has ended, and Temporal holds it for its retention, far longer than
// a pass runs.
func (a *CurateActivities) Outcome(ctx context.Context, request OutcomeRequest) ([]Settlement, error) {
	settlements := make([]Settlement, 0, len(request.IDs))
	for _, id := range request.IDs {
		entry, err := a.Store.Get(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("read learning %s: %w", id, err)
		}
		settlements = append(settlements, Settlement{ID: entry.ID, Title: entry.Title, State: entry.State, PR: entry.PR, Note: entry.Note})
	}
	return settlements, nil
}

// RegisterCurate registers the pass and the SDK's two activities. The app
// registers its ActivityCurateBrief itself, under that name.
func RegisterCurate(registry base.Registry, activities *CurateActivities) error {
	if registry == nil || activities == nil {
		return fmt.Errorf("register the curator: no registry or no activities")
	}
	registry.RegisterWorkflow(CurateWorkflowName, CurateWorkflow)
	registry.RegisterActivity(ActivityCurateCheck, activities.Check)
	registry.RegisterActivity(ActivityCurateOutcome, activities.Outcome)
	return nil
}
