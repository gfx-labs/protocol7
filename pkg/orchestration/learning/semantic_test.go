package learning_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gfx-labs/protocol7/pkg/support/embedding"

	"github.com/gfx-labs/protocol7/pkg/orchestration/learning"
	"github.com/gfx-labs/protocol7/pkg/orchestration/learning/learningtest"
)

// concepts are the axes of the test embedder: a text points along each axis
// whose words it holds, so that texts that share a concept but no word are
// close, as an embeddings model makes them.
var concepts = [][]string{
	{"push", "upload", "commits", "protected", "branch"},
	{"image", "cache", "layers", "docker"},
	{"quota", "memory", "pending", "pods"},
}

type conceptEmbedder struct {
	down  bool
	calls int
}

func (e *conceptEmbedder) Model() string { return "concepts" }

func (e *conceptEmbedder) vector(text string) embedding.Vector {
	text = strings.ToLower(text)
	out := make(embedding.Vector, len(concepts))
	for i, words := range concepts {
		for _, word := range words {
			if strings.Contains(text, word) {
				out[i]++
			}
		}
	}
	return out
}

func (e *conceptEmbedder) EmbedDocuments(_ context.Context, documents ...string) ([]embedding.Vector, error) {
	e.calls++
	if e.down {
		return nil, errors.New("connection refused")
	}
	out := make([]embedding.Vector, len(documents))
	for i, document := range documents {
		out[i] = e.vector(document)
	}
	return out, nil
}

func (e *conceptEmbedder) EmbedQuery(ctx context.Context, _, query string) (embedding.Vector, error) {
	vectors, err := e.EmbedDocuments(ctx, query)
	if err != nil {
		return nil, err
	}
	return vectors[0], nil
}

func semanticActivities(log *learningtest.Log, embedder learning.Embedder) *learning.Activities {
	activities := log.Activities()
	activities.Semantic = &learning.Semantic{
		Embedder: embedder,
		Settings: learning.Settings{
			DuplicateThreshold: 0.9, SearchMinScore: 0.5, AreaMinScore: 0.5,
			Areas: []learning.Area{{Name: "ci", Description: "docker image builds and layer cache"}, {Name: "git", Description: "branch protection and pushes"}},
		},
	}
	return activities
}

func withText(id, title, problem string) learning.Entry {
	e := entry(id, title, wide)
	e.Problem, e.Cause, e.Fix = problem, "something stale", "do the other thing"
	return e
}

// A search with words ranks by meaning, so that a paraphrase that shares no
// word with the entry finds it, and an unrelated entry is not returned.
func TestSearchFindsAParaphrase(t *testing.T) {
	log := learningtest.New(t)
	activities := semanticActivities(log, &conceptEmbedder{})
	for _, e := range []learning.Entry{
		withText("git", "Direct pushes are refused", "git push to the protected branch failed"),
		withText("img", "Stale image layers", "the docker cache kept old layers"),
	} {
		_, err := activities.Record(t.Context(), learning.RecordRequest{Entry: e})
		require.NoError(t, err)
	}
	for _, e := range log.Entries() {
		require.NotEmpty(t, e.Vector, "an entry is stored with its vector")
		_, err := activities.Decide(t.Context(), learning.DecideRequest{ID: e.ID, Decision: learning.Decision{Action: "accept"}})
		require.NoError(t, err)
	}

	found, err := activities.Search(t.Context(), learning.SearchRequest{Query: learning.Query{Text: "cannot upload my commits"}})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(found, "id git"), found)
	assert.NotContains(t, found, "id img")

	// Word matching alone misses it.
	plain, err := log.Activities().Search(t.Context(), learning.SearchRequest{Query: learning.Query{Text: "cannot upload my commits"}})
	require.NoError(t, err)
	assert.Contains(t, plain, "No accepted learning matches")
}

// A record that repeats a recorded entry is held and reports it. The same
// call with distinct set records it. The area that fits is suggested.
func TestRecordReportsDuplicates(t *testing.T) {
	log := learningtest.New(t)
	activities := semanticActivities(log, &conceptEmbedder{})
	_, err := activities.Record(t.Context(), learning.RecordRequest{Entry: withText("a", "Direct pushes are refused", "git push to the protected branch failed"), Handler: true})
	require.NoError(t, err)

	again := withText("b", "Pushing to main fails", "the protected branch refuses a push")
	held, err := activities.Record(t.Context(), learning.RecordRequest{Entry: again})
	require.NoError(t, err)
	assert.True(t, held.Held)
	require.Len(t, held.Duplicates, 1)
	assert.Equal(t, "a", held.Duplicates[0].ID)
	require.NotNil(t, held.Area)
	assert.Equal(t, "git", held.Area.Name)
	assert.Len(t, log.Entries(), 1)

	recorded, err := activities.Record(t.Context(), learning.RecordRequest{Entry: again, Distinct: true})
	require.NoError(t, err)
	assert.False(t, recorded.Held)
	assert.Len(t, log.Entries(), 2)

	// An unrelated entry is no duplicate.
	other, err := activities.Record(t.Context(), learning.RecordRequest{Entry: withText("c", "Pods stay pending", "the memory quota is used up")})
	require.NoError(t, err)
	assert.False(t, other.Held)
	assert.Empty(t, other.Duplicates)
}

// With the embeddings server down, a record is still made, without a vector,
// and a search matches words.
func TestEmbeddingsDownFallsBackToWords(t *testing.T) {
	log := learningtest.New(t)
	activities := semanticActivities(log, &conceptEmbedder{down: true})
	result, err := activities.Record(t.Context(), learning.RecordRequest{Entry: withText("a", "Direct pushes are refused", "git push to the protected branch failed"), Handler: true})
	require.NoError(t, err)
	assert.False(t, result.Held)
	require.Len(t, log.Entries(), 1)
	assert.Empty(t, log.Entries()[0].Vector)

	found, err := activities.Search(t.Context(), learning.SearchRequest{Query: learning.Query{Text: "pushes"}})
	require.NoError(t, err)
	assert.Contains(t, found, "id a")
}

// An entry recorded before embeddings has no vector. It is embedded when a
// search needs it, once.
func TestEntryWithoutVectorIsEmbeddedOnce(t *testing.T) {
	log := learningtest.New(t)
	require.NoError(t, log.Store().Record(t.Context(), withText("old", "Direct pushes are refused", "git push to the protected branch failed")))
	embedder := &conceptEmbedder{}
	activities := semanticActivities(log, embedder)
	for range 2 {
		found, err := activities.Search(t.Context(), learning.SearchRequest{Query: learning.Query{Text: "upload commits"}})
		require.NoError(t, err)
		assert.Contains(t, found, "id old")
	}
	// Two queries, and one embedding of the entry.
	assert.Equal(t, 3, embedder.calls)
}
