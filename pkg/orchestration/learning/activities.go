package learning

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.temporal.io/sdk/temporal"
)

// Names of the activities the chat's learning tools and first prompt call.
// They run in the selfdev worker, which holds the Temporal client of the
// chat's own namespace, so a project reads and writes only its own log.
const (
	ActivityRecord = "LearningRecord"
	ActivitySearch = "LearningSearch"
	ActivityBrief  = "LearningBrief"
	ActivityDecide = "LearningDecide"
)

// ErrorRefused is the type of an error that retrying does not mend.
const ErrorRefused = "LearningRefused"

func refuse(err error) error {
	return temporal.NewNonRetryableApplicationError(err.Error(), ErrorRefused, nil)
}

// Activities are the log's calls from a chat.
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
// otherwise.
func (a *Activities) Record(ctx context.Context, request RecordRequest) (RecordResult, error) {
	entry := request.Entry
	entry.State = Proposed
	if request.Handler {
		entry.State = Accepted
	}
	entry.Normalize()
	if err := entry.Validate(); err != nil {
		return RecordResult{}, refuse(err)
	}
	if err := a.Store.Record(ctx, entry); err != nil && !errors.Is(err, ErrExists) {
		return RecordResult{}, err
	}
	return RecordResult{ID: entry.ID, State: entry.State}, nil
}

// SearchRequest is the input of ActivitySearch.
type SearchRequest struct {
	Query Query `json:"query"`
	// State is accepted (the default) or proposed, which only the handler
	// asks for.
	State State `json:"state,omitempty"`
}

// Search answers a chat's search with entries as text, at most
// MaxSearchBytes. A search that finds nothing says so.
func (a *Activities) Search(ctx context.Context, request SearchRequest) (string, error) {
	entries, err := a.Store.List(ctx)
	if err != nil {
		return "", err
	}
	want := request.State
	if want == "" {
		want = Accepted
	}
	if want != Accepted && want != Proposed {
		return "", refuse(fmt.Errorf("state is accepted or proposed"))
	}
	request.Query.State = want
	found := Search(entries, request.Query)
	if len(found) == 0 {
		return "No " + string(want) + " learning matches.", nil
	}
	var text strings.Builder
	for _, entry := range found {
		note := fmt.Sprintf("id %s\n%s\n", entry.ID, Format(entry, MaxEntryBytes))
		if text.Len()+len(note) > MaxSearchBytes {
			break
		}
		text.WriteString(note)
	}
	return strings.TrimSpace(text.String()), nil
}

// BriefRequest is the input of ActivityBrief.
type BriefRequest struct {
	Repos []string `json:"repos"`
}

// Brief returns the block for a new chat's first prompt, empty when no
// accepted entry fits its repositories.
func (a *Activities) Brief(ctx context.Context, request BriefRequest) (string, error) {
	entries, err := a.Store.List(ctx)
	if err != nil {
		return "", err
	}
	return Brief(entries, request.Repos), nil
}

// DecideRequest is the input of ActivityDecide.
type DecideRequest struct {
	ID       string   `json:"id"`
	Decision Decision `json:"decision"`
}

// Decide accepts, rejects or retires an entry, for the handler.
func (a *Activities) Decide(ctx context.Context, request DecideRequest) (RecordResult, error) {
	if !idPattern.MatchString(request.ID) {
		return RecordResult{}, refuse(fmt.Errorf("%q is not a learning's ID", request.ID))
	}
	entry, err := a.Store.Decide(ctx, request.ID, request.Decision)
	if err != nil {
		var application *temporal.ApplicationError
		if errors.As(err, &application) {
			return RecordResult{}, refuse(fmt.Errorf("%s", application.Message()))
		}
		return RecordResult{}, err
	}
	return RecordResult{ID: entry.ID, State: entry.State}, nil
}
