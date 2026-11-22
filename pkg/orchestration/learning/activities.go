package learning

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.temporal.io/sdk/temporal"
)

// Names of the activities the chat's learning tools and first prompt call.
// They run in the app's chat worker, which holds the Temporal client of the
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
	// Semantic ranks and compares entries by meaning. Nil leaves everything
	// to word matching.
	Semantic *Semantic
}

// RecordRequest is the input of ActivityRecord.
type RecordRequest struct {
	Entry Entry `json:"entry"`
	// Handler says the chat is the escalation handler, whose entries are
	// accepted at once. The chat workflow sets it from its request.
	Handler bool `json:"handler,omitempty"`
	// Distinct says that the entry adds to the likely duplicates that an
	// earlier call reported, so that it is recorded all the same.
	Distinct bool `json:"distinct,omitempty"`
}

// RecordResult says what became of the entry.
type RecordResult struct {
	ID    string `json:"id"`
	State State  `json:"state"`
	// Held is true when the entry was not recorded because it likely
	// repeats Duplicates. The same call with Distinct set records it.
	Held       bool            `json:"held,omitempty"`
	Duplicates []Duplicate     `json:"duplicates,omitempty"`
	Area       *AreaSuggestion `json:"area,omitempty"`
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
	result := RecordResult{ID: entry.ID, State: entry.State}
	if a.Semantic != nil {
		check := a.Semantic.Check(ctx, a.Store, entry)
		entry.Vector, entry.VectorModel = check.Vector, check.Model
		result.Duplicates, result.Area = check.Duplicates, check.Area
		if len(check.Duplicates) > 0 && !request.Distinct {
			result.Held = true
			return result, nil
		}
	}
	if err := a.Store.Record(ctx, entry); err != nil && !errors.Is(err, ErrExists) {
		return RecordResult{}, err
	}
	return result, nil
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
	a.rank(ctx, entries, &request.Query)
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

// rank gives a query that has words the entries' similarity to them. When the
// embeddings server does not answer, the query keeps to word matching.
func (a *Activities) rank(ctx context.Context, entries []Entry, query *Query) {
	if a.Semantic == nil || strings.TrimSpace(query.Text) == "" {
		return
	}
	similarity, err := a.Semantic.Similarities(ctx, entries, query.Text)
	if err != nil {
		a.Semantic.logger().WarnContext(ctx, "learning search: the embeddings server did not answer, so the search matches words", "error", err)
		return
	}
	query.Similarity, query.MinSimilarity = similarity, a.Semantic.Settings.SearchMinScore
}

// BriefRequest is the input of ActivityBrief.
type BriefRequest struct {
	Repos []string `json:"repos"`
	// Text is what the chat was asked, to rank the entries by.
	Text string `json:"text,omitempty"`
}

// Brief returns the block for a new chat's first prompt, empty when no
// accepted entry fits its repositories.
func (a *Activities) Brief(ctx context.Context, request BriefRequest) (string, error) {
	entries, err := a.Store.List(ctx)
	if err != nil {
		return "", err
	}
	query := Query{Text: request.Text}
	a.rank(ctx, entries, &query)
	return BriefRanked(entries, request.Repos, query.Similarity), nil
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
