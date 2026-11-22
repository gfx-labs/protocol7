package learning

import (
	"context"
	"errors"
	"slices"
	"sort"
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"

	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

// TaskQueue is the app worker's queue, where the learning workflow runs. It
// is the base chat's TaskQueue, and a test says so.
const TaskQueue = navi.TaskQueue

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

// listQuery lists the entries whose workflow runs: the open, accepted and
// applied ones. allQuery lists the covered, rejected and retired ones too,
// while Temporal's retention holds them.
const (
	listQuery = "WorkflowType = '" + WorkflowName + "' AND ExecutionStatus = 'Running'"
	allQuery  = "WorkflowType = '" + WorkflowName + "'"
)

// List reads the entries that run (open, accepted and applied), newest first,
// at most ListLimit. Each entry's state is the search attribute's, which a
// decision changes, and its text is the memo's.
func (s Store) List(ctx context.Context) ([]Entry, error) { return s.list(ctx, listQuery) }

// ListAll reads the entries that run and the settled ones that Temporal still
// holds, newest first, at most ListLimit. It is what a page that shows how
// entries were settled reads.
func (s Store) ListAll(ctx context.Context) ([]Entry, error) { return s.list(ctx, allQuery) }

// Open reads the open entries, oldest first. The curator takes them in that
// order, so that none waits for ever behind newer ones.
func (s Store) Open(ctx context.Context) ([]Entry, error) {
	entries, err := s.list(ctx, listQuery)
	if err != nil {
		return nil, err
	}
	open := entries[:0]
	for _, entry := range entries {
		if entry.State.IsOpen() {
			open = append(open, entry)
		}
	}
	slices.Reverse(open)
	return open, nil
}

func (s Store) list(ctx context.Context, query string) ([]Entry, error) {
	var entries []Entry
	var token []byte
	for len(entries) < ListLimit {
		response, err := s.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{PageSize: 100, NextPageToken: token, Query: query})
		if err != nil {
			return nil, err
		}
		for _, execution := range response.GetExecutions() {
			if entry, ok := entryOf(execution); ok {
				entries = append(entries, entry)
			}
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

// entryOf reads an entry from a workflow's memo and search attribute. It is
// false for a workflow that holds no entry.
func entryOf(execution *workflowpb.WorkflowExecutionInfo) (Entry, bool) {
	data := converter.GetDefaultDataConverter()
	var entry Entry
	if err := data.FromPayload(execution.GetMemo().GetFields()[MemoEntry], &entry); err != nil || entry.ID == "" {
		return Entry{}, false
	}
	if payload, ok := execution.GetSearchAttributes().GetIndexedFields()[AttributeState.GetName()]; ok {
		var state string
		if data.FromPayload(payload, &state) == nil && state != "" {
			entry.State = State(state)
		}
	}
	return entry, true
}

// ErrNotFound is returned by Get for an entry that is not in the log.
var ErrNotFound = errors.New("no such learning")

// Get reads one entry, in any state that Temporal still holds: the running
// ones, and a rejected or retired one until its retention ends. Its state is
// the search attribute's.
func (s Store) Get(ctx context.Context, id string) (Entry, error) {
	if !ValidID(id) {
		return Entry{}, ErrNotFound
	}
	response, err := s.Client.DescribeWorkflowExecution(ctx, WorkflowID(id), "")
	var missing *serviceerror.NotFound
	if errors.As(err, &missing) {
		return Entry{}, ErrNotFound
	}
	if err != nil {
		return Entry{}, err
	}
	entry, ok := entryOf(response.GetWorkflowExecutionInfo())
	if !ok {
		return Entry{}, ErrNotFound
	}
	return entry, nil
}

func sortNewest(entries []Entry) {
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].Recorded.After(entries[j].Recorded) })
}

// Decide moves an entry (accept, reject, retire, apply or cover), as an update that the
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
