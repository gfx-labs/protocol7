package learning

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/orchestration/upgrade"
)

// upgradeWake is how often an entry that waits looks for a newer build.
const upgradeWake = time.Hour

// SearchAttributes are the search attributes an entry's workflow starts with.
func SearchAttributes(entry Entry) temporal.SearchAttributes {
	updates := []temporal.SearchAttributeUpdate{
		AttributeKind.ValueSet(KindLearning),
		AttributeState.ValueSet(string(entry.State)),
	}
	if entry.Project != "" {
		updates = append(updates, AttributeProject.ValueSet(entry.Project))
	}
	return temporal.NewSearchAttributes(updates...)
}

// Memo is the memo an entry's workflow starts with: the entry, which never
// changes.
func Memo(entry Entry) map[string]any { return map[string]any{MemoEntry: entry} }

// versionSettleMemo guards the memo that a decision rewrites: an entry that
// was decided before it replays without the command.
const versionSettleMemo = "settle-memo"

// Workflow is one entry. It runs while the entry is open, accepted or applied
// and has no timeout: an open workflow is what makes the record last. A
// decision moves it, and a cover, a rejection or a retirement ends it. A
// decision that settles the entry rewrites its memo, so that a listing shows
// the PR and the note, and the entry's last state stays readable until its
// retention ends.
func Workflow(ctx workflow.Context, entry Entry) (Entry, error) {
	if err := workflow.SetQueryHandler(ctx, QueryEntry, func() (Entry, error) { return entry, nil }); err != nil {
		return entry, err
	}
	err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateDecide, func(ctx workflow.Context, decision Decision) (Entry, error) {
		next, err := entry.Settle(decision, workflow.Now(ctx))
		if err != nil {
			return entry, err
		}
		entry = next
		if err := workflow.UpsertTypedSearchAttributes(ctx, AttributeState.ValueSet(string(entry.State))); err != nil {
			return entry, err
		}
		if workflow.GetVersion(ctx, versionSettleMemo, workflow.DefaultVersion, 1) >= 1 {
			if err := workflow.UpsertMemo(ctx, Memo(entry)); err != nil {
				return entry, err
			}
		}
		return entry, nil
	}, workflow.UpdateHandlerOptions{Validator: func(ctx workflow.Context, decision Decision) error {
		_, err := entry.Settle(decision, workflow.Now(ctx))
		return err
	}})
	if err != nil {
		return entry, err
	}
	ended := func() bool { return entry.State.Ended() }
	for !ended() {
		// An entry waits for as long as it is accepted. A run pinned to its
		// build would hold that build for ever, so it looks now and then
		// whether a newer build is the target, and continues as new on it.
		if _, err := workflow.AwaitWithTimeout(ctx, upgradeWake, ended); err != nil {
			return entry, err
		}
		if !ended() && upgrade.Due(ctx) {
			if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
				return entry, err
			}
			return entry, upgrade.ContinueAsNew(ctx, WorkflowName, entry)
		}
	}
	if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
		return entry, err
	}
	return entry, nil
}
