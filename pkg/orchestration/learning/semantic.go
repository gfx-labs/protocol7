package learning

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	"github.com/gfx-labs/protocol7/pkg/support/embedding"
)

// The task each kind of query gives Qwen3-Embedding, which reads a query as
// an instruction and a question. A document is embedded as it is.
const (
	searchTask = "Given a question or a description of a failure, find the recorded learnings that answer it"
	areaTask   = "Given a learning that was recorded after a failure, find the area of the work it belongs to"
)

// MaxDuplicates is how many likely duplicates a record reports.
const MaxDuplicates = 3

// embedBatch bounds the texts of one request to the embeddings server.
const embedBatch = 32

// queryLimit bounds the text of a query that is embedded.
const queryLimit = 1000

// Embedder is the embeddings server as the learnings use it. An
// *embedding.Client is one.
type Embedder interface {
	Model() string
	EmbedDocuments(ctx context.Context, documents ...string) ([]embedding.Vector, error)
	EmbedQuery(ctx context.Context, task, query string) (embedding.Vector, error)
}

// Area is an area of the work, as the deployment's settings describe it.
type Area struct {
	Name        string
	Description string
}

// Settings are the thresholds of the semantic features, in the deployment's
// settings file.
type Settings struct {
	// DuplicateThreshold is the cosine similarity of two entries from which
	// a new entry is reported as a likely duplicate of the old one.
	DuplicateThreshold float64
	// SearchMinScore is the cosine similarity from which an entry answers a
	// search that gave words.
	SearchMinScore float64
	// AreaMinScore is the cosine similarity from which an area is suggested.
	AreaMinScore float64
	// Areas describe the areas of the work, for the suggestion.
	Areas []Area
}

// Semantic finds entries by meaning. Every method that asks the embeddings
// server reports its failure, and the caller falls back to word matching:
// the server being down never stops a record, a search or a chat.
type Semantic struct {
	Embedder Embedder
	Settings Settings
	Logger   *slog.Logger

	mu          sync.Mutex
	entryVector map[string]embedding.Vector
	areaVector  []embedding.Vector
}

// EmbeddingText is the text of an entry that is embedded.
func (e Entry) EmbeddingText() string {
	return fmt.Sprintf("%s\nProblem: %s\nCause: %s\nFix: %s", e.Title, e.Problem, e.Cause, e.Fix)
}

func (s *Semantic) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

// vectors returns the vector of each entry, in order. An entry carries its
// vector when it was recorded under this model. An older entry is embedded
// now, and the vector is kept in memory for the next call.
func (s *Semantic) vectors(ctx context.Context, entries []Entry) ([]embedding.Vector, error) {
	model := s.Embedder.Model()
	out := make([]embedding.Vector, len(entries))
	var missing []int
	s.mu.Lock()
	for i, entry := range entries {
		switch cached := s.entryVector[entry.ID]; {
		case entry.VectorModel == model && len(entry.Vector) > 0:
			out[i] = entry.Vector
		case len(cached) > 0:
			out[i] = cached
		default:
			missing = append(missing, i)
		}
	}
	s.mu.Unlock()
	for start := 0; start < len(missing); start += embedBatch {
		batch := missing[start:min(start+embedBatch, len(missing))]
		texts := make([]string, len(batch))
		for i, index := range batch {
			texts[i] = entries[index].EmbeddingText()
		}
		vectors, err := s.Embedder.EmbedDocuments(ctx, texts...)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		if s.entryVector == nil {
			s.entryVector = map[string]embedding.Vector{}
		}
		for i, index := range batch {
			out[index] = vectors[i]
			s.entryVector[entries[index].ID] = vectors[i]
		}
		s.mu.Unlock()
	}
	return out, nil
}

// Similarities is the cosine similarity of the text, as a query, to each of
// the entries, by ID.
func (s *Semantic) Similarities(ctx context.Context, entries []Entry, text string) (map[string]float64, error) {
	query, err := s.Embedder.EmbedQuery(ctx, searchTask, cut(text, queryLimit))
	if err != nil {
		return nil, err
	}
	vectors, err := s.vectors(ctx, entries)
	if err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(entries))
	for i, entry := range entries {
		similarity, err := embedding.Cosine(query, vectors[i])
		if err != nil {
			return nil, fmt.Errorf("compare with learning %s: %w", entry.ID, err)
		}
		out[entry.ID] = similarity
	}
	return out, nil
}

// Duplicate is an entry that an entry being recorded likely repeats.
type Duplicate struct {
	ID    string  `json:"id"`
	Title string  `json:"title"`
	State State   `json:"state"`
	Score float64 `json:"score"`
	// Note is the entry as the agent reads it, so that it can tell whether
	// its own adds anything.
	Note string `json:"note"`
}

// AreaSuggestion is the area an entry fits best.
type AreaSuggestion struct {
	Name  string  `json:"name"`
	Score float64 `json:"score"`
}

// Check is what the embeddings say about an entry that is being recorded.
type Check struct {
	// Vector and Model are the entry's own, to store with it. Empty when it
	// could not be embedded.
	Vector     embedding.Vector
	Model      string
	Duplicates []Duplicate
	Area       *AreaSuggestion
}

// Check embeds the entry and compares it with the entries that are in the log
// and with the areas. It never fails: a call that does not work is logged and
// its part of the result is left out.
func (s *Semantic) Check(ctx context.Context, store Store, entry Entry) Check {
	var check Check
	vectors, err := s.Embedder.EmbedDocuments(ctx, entry.EmbeddingText())
	if err != nil {
		s.logger().WarnContext(ctx, "learning record: the embeddings server did not answer, so the entry has no vector and no duplicate check", "id", entry.ID, "error", err)
		return check
	}
	check.Vector, check.Model = vectors[0], s.Embedder.Model()
	check.Duplicates = s.duplicates(ctx, store, entry, check.Vector)
	check.Area = s.area(ctx, entry)
	return check
}

func (s *Semantic) duplicates(ctx context.Context, store Store, entry Entry, vector embedding.Vector) []Duplicate {
	listed, err := store.List(ctx)
	if err != nil {
		s.logger().WarnContext(ctx, "learning record: could not list the log, so no duplicate check", "id", entry.ID, "error", err)
		return nil
	}
	// An activity that is retried after the entry was written finds the entry
	// itself, which is no duplicate.
	var others []Entry
	for _, other := range listed {
		if other.ID != entry.ID {
			others = append(others, other)
		}
	}
	vectors, err := s.vectors(ctx, others)
	if err != nil {
		s.logger().WarnContext(ctx, "learning record: could not embed the log, so no duplicate check", "id", entry.ID, "error", err)
		return nil
	}
	var found []Duplicate
	for i, other := range others {
		similarity, err := embedding.Cosine(vector, vectors[i])
		if err != nil {
			s.logger().WarnContext(ctx, "learning record: could not compare entries", "id", entry.ID, "other", other.ID, "error", err)
			continue
		}
		if similarity >= s.Settings.DuplicateThreshold {
			found = append(found, Duplicate{ID: other.ID, Title: other.Title, State: other.State, Score: similarity, Note: Format(other, MaxEntryBytes)})
		}
	}
	sort.SliceStable(found, func(i, j int) bool { return found[i].Score > found[j].Score })
	if len(found) > MaxDuplicates {
		found = found[:MaxDuplicates]
	}
	return found
}

// area returns the described area that fits the entry best, if it fits well
// enough.
func (s *Semantic) area(ctx context.Context, entry Entry) *AreaSuggestion {
	areas := s.Settings.Areas
	if len(areas) == 0 {
		return nil
	}
	s.mu.Lock()
	cached := s.areaVector
	s.mu.Unlock()
	if len(cached) != len(areas) {
		texts := make([]string, len(areas))
		for i, area := range areas {
			texts[i] = area.Name + ": " + area.Description
		}
		vectors, err := s.Embedder.EmbedDocuments(ctx, texts...)
		if err != nil {
			s.logger().WarnContext(ctx, "learning record: could not embed the areas, so no area suggestion", "id", entry.ID, "error", err)
			return nil
		}
		s.mu.Lock()
		s.areaVector = vectors
		s.mu.Unlock()
		cached = vectors
	}
	query, err := s.Embedder.EmbedQuery(ctx, areaTask, entry.EmbeddingText())
	if err != nil {
		s.logger().WarnContext(ctx, "learning record: could not embed the entry as a query, so no area suggestion", "id", entry.ID, "error", err)
		return nil
	}
	var best *AreaSuggestion
	for i, area := range areas {
		similarity, err := embedding.Cosine(query, cached[i])
		if err != nil {
			s.logger().WarnContext(ctx, "learning record: could not compare with an area", "id", entry.ID, "area", area.Name, "error", err)
			return nil
		}
		if best == nil || similarity > best.Score {
			best = &AreaSuggestion{Name: area.Name, Score: similarity}
		}
	}
	if best == nil || best.Score < s.Settings.AreaMinScore {
		return nil
	}
	return best
}

// ParseAreas turns the settings' area descriptions, name to description,
// into a list in the order of the names.
func ParseAreas(described map[string]string) ([]Area, error) {
	names := make([]string, 0, len(described))
	for name := range described {
		names = append(names, name)
	}
	sort.Strings(names)
	areas := make([]Area, 0, len(names))
	for _, name := range names {
		normal := strings.ToLower(strings.TrimSpace(name))
		description := strings.TrimSpace(described[name])
		if normal == "" || description == "" {
			return nil, fmt.Errorf("the area %q needs a name and a description", name)
		}
		areas = append(areas, Area{Name: normal, Description: description})
	}
	return areas, nil
}
