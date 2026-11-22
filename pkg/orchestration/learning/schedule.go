package learning

import (
	"context"
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

// scheduleCatchup is how late a missed run may still start: a worker that was
// down over a run does not start several when it returns.
const scheduleCatchup = 10 * time.Minute

// EnsureSchedule creates the namespace's curator schedule, or updates the one
// that exists to the input, and keeps a pause that a person set. The
// schedule's overlap policy skips a run while a pass runs, and the check of the
// pass skips a run that finds nothing new, so a quiet namespace starts passes
// that end in a second.
func EnsureSchedule(ctx context.Context, temporalClient client.Client, input CurateInput) error {
	config := input.Config.WithDefaults()
	input.Config = config
	if input.Session == nil {
		return errors.New("the curator schedule needs the session of the curator's chat")
	}
	if input.Trigger == "" {
		input.Trigger = TriggerSchedule
	}
	action := &client.ScheduleWorkflowAction{
		ID: CurateScheduleID, Workflow: CurateWorkflowName, TaskQueue: TaskQueue, Args: []any{input},
	}
	spec := client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: config.Every}}}
	schedules := temporalClient.ScheduleClient()
	handle := schedules.GetHandle(ctx, CurateScheduleID)
	_, err := handle.Describe(ctx)
	var missing *serviceerror.NotFound
	if errors.As(err, &missing) {
		_, err = schedules.Create(ctx, client.ScheduleOptions{
			ID: CurateScheduleID, Spec: spec, Action: action,
			Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, CatchupWindow: scheduleCatchup,
		})
		var exists *serviceerror.WorkflowExecutionAlreadyStarted
		if errors.As(err, &exists) {
			// Another worker that started with this one made it.
			return nil
		}
		if err != nil {
			return fmt.Errorf("create the curator schedule: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("read the curator schedule: %w", err)
	}
	err = handle.Update(ctx, client.ScheduleUpdateOptions{DoUpdate: func(update client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
		schedule := update.Description.Schedule
		schedule.Spec, schedule.Action = &spec, action
		schedule.Policy.Overlap, schedule.Policy.CatchupWindow = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, scheduleCatchup
		return &client.ScheduleUpdate{Schedule: &schedule}, nil
	}})
	if err != nil {
		return fmt.Errorf("update the curator schedule: %w", err)
	}
	return nil
}

// StartPass starts a pass now, outside the schedule, for a person or a tool
// that wants one. It runs when an entry is open and no earlier pass runs, and
// returns the pass's workflow ID.
func StartPass(ctx context.Context, temporalClient client.Client, input CurateInput) (string, error) {
	input.Config = input.Config.WithDefaults()
	input.Trigger = TriggerManual
	id := fmt.Sprintf("%smanual-%d", CurateWorkflowPrefix, time.Now().UnixMilli())
	_, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: TaskQueue}, CurateWorkflowName, input)
	if err != nil {
		return "", fmt.Errorf("start a curator pass: %w", err)
	}
	return id, nil
}

// RemoveSchedule deletes the curator schedule, for a process that has the
// curator off, so that turning it off sticks. It is not an error that there is
// none. A pass that runs is left to end.
func RemoveSchedule(ctx context.Context, temporalClient client.Client) (removed bool, err error) {
	err = temporalClient.ScheduleClient().GetHandle(ctx, CurateScheduleID).Delete(ctx)
	var missing *serviceerror.NotFound
	if errors.As(err, &missing) {
		return false, nil
	}
	return err == nil, err
}
