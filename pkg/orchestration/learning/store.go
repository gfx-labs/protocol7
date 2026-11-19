package learning

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

// TaskQueue is the selfdev worker's queue, where the learning workflow runs.
// The chat package's TaskQueue is the same, and a test says so.
const TaskQueue = "selfdev"

// ListLimit bounds the entries one read returns, the newest first.
const ListLimit = navi.LearningListLimit

// Store is the log, read and written through the Temporal client of the
// project's namespace.
type Store struct {
	Client client.Client
}

// ErrExists is returned by Record for an ID that is recorded already. The
// entry is then not changed.
var ErrExists = errors.New("this learning is recorded already")

// Record starts the entry's workflow. The ID makes it idempotent: an
// activity that is retried records the entry once.
func (s Store) Record(ctx context.Context, entry Entry) error {
	entry.Normalize()
	if err := entry.Validate(); err != nil {
		return err
	}
	_, err := s.Client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    WorkflowID(entry.ID),
		TaskQueue:             TaskQueue,
		Memo:                  Memo(entry),
		TypedSearchAttributes: SearchAttributes(entry),
	}, WorkflowName, entry)
	var started *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &started) {
		return ErrExists
	}
	return err
}

// Query lists the proposed and the accepted entries, the ones whose workflow
// runs.
const listQuery = "WorkflowType = '" + WorkflowName + "' AND ExecutionStatus = 'Running'"

// List reads the entries that wait or are accepted, newest first, at most
// ListLimit. Each entry's state is the search attribute's, which a decision
// changes, and its text is the memo's.
func (s Store) List(ctx context.Context) ([]Entry, error) {
	data := converter.GetDefaultDataConverter()
	var entries []Entry
	var token []byte
	for len(entries) < ListLimit {
		response, err := s.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{PageSize: 100, NextPageToken: token, Query: listQuery})
		if err != nil {
			return nil, err
		}
		for _, execution := range response.GetExecutions() {
			var entry Entry
			if err := data.FromPayload(execution.GetMemo().GetFields()[MemoEntry], &entry); err != nil || entry.ID == "" {
				continue
			}
			if payload, ok := execution.GetSearchAttributes().GetIndexedFields()[AttributeState.GetName()]; ok {
				var state string
				if data.FromPayload(payload, &state) == nil && state != "" {
					entry.State = State(state)
				}
			}
			entries = append(entries, entry)
		}
		token = response.GetNextPageToken()
		if len(token) == 0 {
			break
		}
	}
	if len(entries) > ListLimit {
		entries = entries[:ListLimit]
	}
	sortNewest(entries)
	return entries, nil
}

func sortNewest(entries []Entry) {
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].Recorded.After(entries[j].Recorded) })
}

// Decide accepts, rejects or retires an entry, as an update that the
// workflow refuses when the state does not allow it. It returns the entry
// as it is after.
func (s Store) Decide(ctx context.Context, id string, decision Decision) (Entry, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	handle, err := s.Client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   WorkflowID(id),
		UpdateName:   UpdateDecide,
		Args:         []any{decision},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	var entry Entry
	if err == nil {
		err = handle.Get(ctx, &entry)
	}
	if err != nil {
		return Entry{}, err
	}
	return entry, nil
}
