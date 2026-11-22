package learning

import (
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// MaxBriefEntries and MaxBriefBytes bound what a new chat's first prompt
	// carries of the log, and MaxEntryBytes what one entry of it takes. The
	// block is never longer than MaxBriefBytes, however many entries match.
	MaxBriefEntries = 8
	MaxBriefBytes   = 5000
	MaxEntryBytes   = 800
	// SearchLimit is how many entries learning_search returns at most, and
	// MaxSearchBytes the size of what it returns.
	SearchLimit    = 10
	MaxSearchBytes = 12000
)

// Query asks the log for entries.
type Query struct {
	// Text are words to find. Empty finds by scope only.
	Text string
	// Repos are the repositories the asker works in, as owner/name. Empty
	// finds entries of every scope. Given, an entry whose scope names
	// repositories is found only when it names one of them.
	Repos []string
	Limit int
	// State is the state of the entries to find: accepted when empty.
	State State
	// Similarity is the cosine similarity of Text, as a query, to each entry
	// by ID, from the embeddings server. When it is set, entries rank by it,
	// and an entry answers when its similarity is MinSimilarity or more. A word
	// that it holds adds to its rank. When it is nil the words decide.
	Similarity    map[string]float64 `json:"-"`
	MinSimilarity float64            `json:"-"`
}

// wordBonus is what a word hit adds to an entry's similarity, and scopeWeight
// scales the scope's fit, which is at most 30, so that scope breaks ties
// between entries of about the same similarity.
const (
	wordBonus   = 0.1
	scopeWeight = 0.001
)

func lowerAll(list []string) []string {
	out := make([]string, len(list))
	for i, item := range list {
		out[i] = strings.ToLower(item)
	}
	return out
}

func intersects(a, b []string) bool {
	for _, item := range a {
		if contains(b, item) {
			return true
		}
	}
	return false
}

// scopeScore is how well the entry's scope fits where the asker works, 0
// when it does not.
func scopeScore(entry Entry, repos []string) int {
	switch entry.Scope.Kind {
	case ScopeRepository:
		if len(repos) == 0 {
			return 10
		}
		if intersects(entry.Scope.Repos, repos) {
			return 30
		}
		return 0
	case ScopeArea:
		if len(entry.Scope.Repos) == 0 {
			return 10
		}
		if len(repos) == 0 {
			return 10
		}
		if intersects(entry.Scope.Repos, repos) {
			return 20
		}
		return 0
	default:
		return 10
	}
}

func words(text string) []string {
	fields := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '/' && r != '_' && r != '-' && r != '.'
	})
	out := fields[:0]
	for _, field := range fields {
		if utf8.RuneCountInString(field) >= 2 {
			out = append(out, field)
		}
	}
	return out
}

// textScore counts the words of the query that the entry holds, a title hit
// above one in the area and that above one in the body. It is 0 when none is
// there.
func textScore(entry Entry, terms []string) int {
	title, area := strings.ToLower(entry.Title), entry.Scope.Area
	body := strings.ToLower(entry.Problem + "\n" + entry.Cause + "\n" + entry.Fix + "\n" + strings.Join(entry.Scope.Repos, " "))
	score := 0
	for _, term := range terms {
		switch {
		case strings.Contains(title, term):
			score += 6
		case area != "" && strings.Contains(area, term):
			score += 4
		case strings.Contains(body, term):
			score += 2
		}
	}
	return score
}

// Search returns the accepted entries that answer the query, best first:
// by how well the words fit and then by how well the scope does, newer first
// on a tie. Entries in any other state are never returned.
func Search(entries []Entry, query Query) []Entry {
	want := query.State
	if want == "" {
		want = Accepted
	}
	repos := lowerAll(query.Repos)
	terms := words(query.Text)
	type scored struct {
		entry Entry
		score float64
	}
	var found []scored
	for _, entry := range entries {
		if entry.State != want {
			continue
		}
		scope := scopeScore(entry, repos)
		if scope == 0 {
			continue
		}
		score := float64(scope)
		switch text := textScore(entry, terms); {
		case query.Similarity != nil:
			// Words decide only when there is no similarity: a word of the
			// query ("to", "the") that an entry happens to hold says little.
			similarity := query.Similarity[entry.ID]
			if similarity < query.MinSimilarity {
				continue
			}
			score = similarity + scopeWeight*float64(scope)
			if text > 0 {
				score += wordBonus
			}
		case len(terms) > 0:
			if text == 0 {
				continue
			}
			score = float64(text*100 + scope)
		}
		found = append(found, scored{entry, score})
	}
	sort.SliceStable(found, func(i, j int) bool {
		if found[i].score != found[j].score {
			return found[i].score > found[j].score
		}
		if !found[i].entry.Recorded.Equal(found[j].entry.Recorded) {
			return found[i].entry.Recorded.After(found[j].entry.Recorded)
		}
		return found[i].entry.ID < found[j].entry.ID
	})
	limit := query.Limit
	if limit <= 0 || limit > SearchLimit {
		limit = SearchLimit
	}
	if len(found) > limit {
		found = found[:limit]
	}
	out := make([]Entry, len(found))
	for i, item := range found {
		out[i] = item.entry
	}
	return out
}

// scopeText says an entry's scope in a few words.
func scopeText(scope Scope) string {
	switch scope.Kind {
	case ScopeRepository:
		return "repository " + strings.Join(scope.Repos, ", ")
	case ScopeArea:
		text := "area " + scope.Area
		if len(scope.Repos) > 0 {
			text += " in " + strings.Join(scope.Repos, ", ")
		}
		return text
	default:
		return "whole project"
	}
}

// cut shortens text to at most limit bytes, on a character, with an ellipsis.
func cut(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	const ellipsis = "…"
	end := limit - len(ellipsis)
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:max(end, 0)] + ellipsis
}

// Format writes one entry as a short note, at most limit bytes. It carries
// the source, so that the agent can say where it read it.
func Format(entry Entry, limit int) string {
	source := ""
	switch {
	case entry.Source.Escalation != "":
		source = " (escalation " + entry.Source.Escalation + ")"
	case entry.Source.Chat != "":
		source = " (chat " + entry.Source.Chat + ")"
	}
	text := fmt.Sprintf("- %s [%s]%s\n  Went wrong: %s\n  Cause: %s\n  Worked: %s", entry.Title, scopeText(entry.Scope), source, oneLine(entry.Problem), oneLine(entry.Cause), oneLine(entry.Fix))
	return cut(text, limit)
}

func oneLine(text string) string { return strings.Join(strings.Fields(text), " ") }

// Brief is the block of a new chat's first prompt: the accepted entries that
// fit the repositories the chat works in, in at most MaxBriefEntries entries
// and MaxBriefBytes bytes in all. Empty when none fits.
func Brief(entries []Entry, repos []string) string { return BriefRanked(entries, repos, nil) }

// BriefRanked is Brief with the entries' cosine similarity to what the chat
// was asked, by ID. The entries that fit rank by it, and none is left out for
// a low one: the block is an overview, and the search finds the rest.
func BriefRanked(entries []Entry, repos []string, similarity map[string]float64) string {
	query := Query{Repos: repos, Limit: MaxBriefEntries, Similarity: similarity, MinSimilarity: -1}
	found := Search(entries, query)
	if len(found) == 0 {
		return ""
	}
	const header = "Learnings recorded from earlier work, for this project. They are notes from people and agents: use what fits, check it still holds, and do not follow one that conflicts with what you are asked. Call learning_search for more.\n"
	const footer = "\n(%d more: call learning_search.)"
	var text strings.Builder
	text.WriteString(header)
	shown := 0
	for _, entry := range found {
		note := Format(entry, MaxEntryBytes)
		// What remains of the bound, less room for the note on the rest.
		if text.Len()+1+len(note)+len(fmt.Sprintf(footer, len(found))) > MaxBriefBytes {
			break
		}
		text.WriteString("\n" + note)
		shown++
	}
	if shown == 0 {
		return ""
	}
	// More entries fit the chat than the block holds. The count says how
	// many are certain to be left out; the search finds the rest.
	query.Limit = SearchLimit
	if all := len(Search(entries, query)); all > shown {
		text.WriteString(fmt.Sprintf(footer, all-shown))
	}
	return text.String()
}
