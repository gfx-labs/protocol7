package issue_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/gfx-labs/protocol7/pkg/orchestration/issue"
)

// scanHistory is the history one run of selfdev.IssueScan wrote, as the
// schedule started it. The scanner replaced the schedule, but runs that it
// started before the deploy still replay on the new build, so the scan
// workflow must not change under them. settle says whether the run is one of
// those that waited for labels to settle (#63), which wrote a version marker
// and a timer before the activity.
func scanHistory(t *testing.T, settle bool) *historypb.History {
	t.Helper()
	data := converter.GetDefaultDataConverter()
	payloads := func(values ...any) *commonpb.Payloads {
		out, err := data.ToPayloads(values...)
		require.NoError(t, err)
		return out
	}
	queue := &taskqueuepb.TaskQueue{Name: issue.TaskQueue}
	at := time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC)
	var events []*historypb.HistoryEvent
	add := func(kind enumspb.EventType, attributes func(*historypb.HistoryEvent)) int64 {
		event := &historypb.HistoryEvent{EventId: int64(len(events) + 1), EventTime: timestamppb.New(at), EventType: kind}
		at = at.Add(time.Second)
		attributes(event)
		events = append(events, event)
		return event.EventId
	}
	add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType: &commonpb.WorkflowType{Name: issue.ScanWorkflowName}, TaskQueue: queue, Input: payloads(scanTemplate), WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		}}
	})
	task := func() int64 {
		scheduled := add(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{TaskQueue: queue, StartToCloseTimeout: durationpb.New(10 * time.Second)}}
		})
		started := add(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{ScheduledEventId: scheduled}}
		})
		return add(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{ScheduledEventId: scheduled, StartedEventId: started}}
		})
	}
	completed := task()
	if settle {
		add(enumspb.EVENT_TYPE_MARKER_RECORDED, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
				MarkerName: "Version", WorkflowTaskCompletedEventId: completed,
				Details: map[string]*commonpb.Payloads{"change-id": payloads("issue-label-list-settle"), "version": payloads(1)},
			}}
		})
		// The SDK records the versions a workflow took in a search attribute.
		add(enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{UpsertWorkflowSearchAttributesEventAttributes: &historypb.UpsertWorkflowSearchAttributesEventAttributes{
				WorkflowTaskCompletedEventId: completed,
				SearchAttributes:             &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{"TemporalChangeVersion": mustPayload(t, []string{"issue-label-list-settle-1"})}},
			}}
		})
		timerID := strconv.Itoa(len(events) + 1)
		timer := add(enumspb.EVENT_TYPE_TIMER_STARTED, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
				TimerId: timerID, StartToFireTimeout: durationpb.New(5 * time.Second), WorkflowTaskCompletedEventId: completed,
			}}
		})
		add(enumspb.EVENT_TYPE_TIMER_FIRED, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{TimerId: timerID, StartedEventId: timer}}
		})
		completed = task()
	}
	activityID := strconv.Itoa(len(events) + 1)
	scheduled := add(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
			ActivityId: activityID, ActivityType: &commonpb.ActivityType{Name: issue.ActivityScan}, TaskQueue: queue, Input: payloads(scanTemplate),
			StartToCloseTimeout: durationpb.New(2 * time.Minute), WorkflowTaskCompletedEventId: completed,
		}}
	})
	started := add(enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{ScheduledEventId: scheduled}}
	})
	add(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
			ScheduledEventId: scheduled, StartedEventId: started, Result: payloads([]issue.Ref{{Repo: "gfx-labs/r", Number: 3}}),
		}}
	})
	completed = task()
	add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
			Result: payloads([]issue.Ref{{Repo: "gfx-labs/r", Number: 3}}), WorkflowTaskCompletedEventId: completed,
		}}
	})
	return &historypb.History{Events: events}
}

// Runs the schedule started before the deploy replay on the new build, both
// the ones from before the label settle and the ones after it.
func TestOldScanRunsStillReplay(t *testing.T) {
	for name, settle := range map[string]bool{"before the label settle": false, "after the label settle": true} {
		t.Run(name, func(t *testing.T) {
			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflowWithOptions(issue.ScanWorkflow, workflow.RegisterOptions{Name: issue.ScanWorkflowName})
			require.NoError(t, replayer.ReplayWorkflowHistory(nil, scanHistory(t, settle)))
		})
	}
}

func mustPayload(t *testing.T, value any) *commonpb.Payload {
	t.Helper()
	out, err := converter.GetDefaultDataConverter().ToPayload(value)
	require.NoError(t, err)
	return out
}
