package learning

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/sdk/temporal"

	base "github.com/gfx-labs/protocol7/pkg/orchestration/chat"
)

// Names of the log's two activities. They run in the app's chat worker, which
// holds the Temporal client of the chat's own namespace, so a project reads
// and writes only its own log. The names are recorded in the histories of
// running chats and must not change.
//
// An app that curates the log (search, a brief for a first prompt, a check
// for duplicates) registers activities of its own for those, under the names
// "LearningSearch" and "LearningBrief" that its older histories hold. When it
// curates records too, its own "LearningRecord" replaces Activities.Record and
// calls it to write the entry.
const (
	ActivityRecord = "LearningRecord"
	ActivityDecide = "LearningDecide"
)

// ErrorRefused is the type of an error that retrying does not mend.
const ErrorRefused = "LearningRefused"

// Refuse wraps an error that retrying does not mend, with the type
// ErrorRefused.
func Refuse(err error) error {
	return temporal.NewNonRetryableApplicationError(err.Error(), ErrorRefused, nil)
}

// Activities are the log's writes from a chat. They hold no curation: an app
// builds its own on the Store.
type Activities struct {
	Store Store
}

// RecordRequest is the input of ActivityRecord.
type RecordRequest struct {
	Entry Entry `json:"entry"`
	// Handler says the chat is the escalation handler, whose entries are
	// accepted at once. The chat workflow sets it from its request.
	Handler bool `json:"handler,omitempty"`
}

// RecordResult says what became of the entry.
type RecordResult struct {
	ID    string `json:"id"`
	State State  `json:"state"`
}

// Record writes an entry: accepted when the handler recorded it, proposed
// otherwise. The entry's vector, when it has one, is stored as it is.
func (a *Activities) Record(ctx context.Context, request RecordRequest) (RecordResult, error) {
	entry := request.Entry
	entry.State = Proposed
	if request.Handler {
		entry.State = Accepted
	}
	entry.Normalize()
	if err := entry.Validate(); err != nil {
		return RecordResult{}, Refuse(err)
	}
	if err := a.Store.Record(ctx, entry); err != nil && !errors.Is(err, ErrExists) {
		return RecordResult{}, err
	}
	return RecordResult{ID: entry.ID, State: entry.State}, nil
}

// DecideRequest is the input of ActivityDecide.
type DecideRequest struct {
	ID       string   `json:"id"`
	Decision Decision `json:"decision"`
}

// Decide accepts, rejects or retires an entry, for the handler.
func (a *Activities) Decide(ctx context.Context, request DecideRequest) (RecordResult, error) {
	if !ValidID(request.ID) {
		return RecordResult{}, Refuse(fmt.Errorf("%q is not a learning's ID", request.ID))
	}
	entry, err := a.Store.Decide(ctx, request.ID, request.Decision)
	if err != nil {
		var application *temporal.ApplicationError
		if errors.As(err, &application) {
			return RecordResult{}, Refuse(fmt.Errorf("%s", application.Message()))
		}
		return RecordResult{}, err
	}
	return RecordResult{ID: entry.ID, State: entry.State}, nil
}

// Register registers the learning workflow and the log's two activities on
// the registry, each under its explicit name.
func Register(registry base.Registry, activities *Activities) error {
	if activities == nil {
		return fmt.Errorf("register learning: no activities")
	}
	if err := RegisterWorkflow(registry); err != nil {
		return err
	}
	registry.RegisterActivity(ActivityRecord, activities.Record)
	registry.RegisterActivity(ActivityDecide, activities.Decide)
	return nil
}

// RegisterWorkflow registers only the learning workflow, for an app that
// registers its own activities under the log's names.
func RegisterWorkflow(registry base.Registry) error {
	if registry == nil {
		return fmt.Errorf("register learning: no registry")
	}
	registry.RegisterWorkflow(WorkflowName, Workflow)
	return nil
}
