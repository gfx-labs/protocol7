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

// Workflow is one entry. It runs while the entry is proposed or accepted and
// has no timeout: an open workflow is what makes the record last. A decision
// moves it, and a rejection or a retirement ends it.
func Workflow(ctx workflow.Context, entry Entry) (Entry, error) {
	if err := workflow.SetQueryHandler(ctx, QueryEntry, func() (Entry, error) { return entry, nil }); err != nil {
		return entry, err
	}
	err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateDecide, func(ctx workflow.Context, decision Decision) (Entry, error) {
		entry.State, _ = entry.Decide(decision.Action)
		return entry, workflow.UpsertTypedSearchAttributes(ctx, AttributeState.ValueSet(string(entry.State)))
	}, workflow.UpdateHandlerOptions{Validator: func(_ workflow.Context, decision Decision) error {
		_, err := entry.Decide(decision.Action)
		return err
	}})
	if err != nil {
		return entry, err
	}
	ended := func() bool { return entry.State == Rejected || entry.State == Retired }
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
