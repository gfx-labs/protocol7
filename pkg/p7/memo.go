package p7

import (
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
)

// MemoSessions is the memo key under which a workflow that opens sessions
// lists them, as []SessionRecord. A memo comes back with every
// ListWorkflowExecutions result, so a viewer can find the sessions of a
// namespace without a registered search attribute.
const MemoSessions = "p7.sessions"

// SessionRecord is one session in a workflow's MemoSessions.
type SessionRecord struct {
	ID     string    `json:"id"`
	Models []string  `json:"models,omitempty"`
	Opened time.Time `json:"opened"`
	// Closed is zero while the session is open.
	Closed time.Time `json:"closed,omitzero"`
}

// versionSessionMemo guards the memo commands, which workflows that were
// running before the memo existed did not record.
// memoSessionLimit bounds the memo of a workflow that opens many sessions.
const memoSessionLimit = 20

const versionSessionMemo = "p7-session-memo"

// SessionRecords reads MemoSessions from a memo field. A missing field is
// no sessions.
func SessionRecords(memo map[string]*commonpb.Payload) ([]SessionRecord, error) {
	var records []SessionRecord
	payload := memo[MemoSessions]
	if payload == nil {
		return nil, nil
	}
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &records); err != nil {
		return nil, err
	}
	return records, nil
}

// recordSession adds or updates a session in the workflow's MemoSessions.
// The memo is a record for viewers, so failing to write it does not fail
// the session.
func recordSession(ctx workflow.Context, update func([]SessionRecord) []SessionRecord) {
	if workflow.GetVersion(ctx, versionSessionMemo, workflow.DefaultVersion, 1) < 1 {
		return
	}
	records, err := SessionRecords(workflow.GetInfo(ctx).Memo.GetFields())
	if err != nil {
		records = nil
	}
	if err := workflow.UpsertMemo(ctx, map[string]any{MemoSessions: update(slices.Clone(records))}); err != nil {
		workflow.GetLogger(ctx).Warn("record session in memo", "error", err)
	}
}

func recordOpened(ctx workflow.Context, id string, models []string) {
	recordSession(ctx, func(records []SessionRecord) []SessionRecord {
		records = append(records, SessionRecord{ID: id, Models: models, Opened: workflow.Now(ctx)})
		// A long-lived workflow keeps only its latest sessions in the memo.
		return records[max(0, len(records)-memoSessionLimit):]
	})
}

func recordClosed(ctx workflow.Context, id string) {
	recordSession(ctx, func(records []SessionRecord) []SessionRecord {
		for index := range records {
			if records[index].ID == id && records[index].Closed.IsZero() {
				records[index].Closed = workflow.Now(ctx)
			}
		}
		return records
	})
}
