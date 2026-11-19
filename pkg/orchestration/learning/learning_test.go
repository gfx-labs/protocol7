package learning_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/orchestration/learning"
	"github.com/gfx-labs/protocol7/pkg/orchestration/learning/learningtest"
)

func entry(id, title string, scope learning.Scope) learning.Entry {
	return learning.Entry{
		ID: id, Title: title, Problem: "the push was refused", Cause: "the branch is protected", Fix: "open a pull request",
		Scope: scope, Source: learning.Source{Chat: "c1", Escalation: "e" + id}, Recorded: time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC), State: learning.Accepted,
	}
}

var (
	selfdev  = learning.Scope{Kind: learning.ScopeRepository, Repos: []string{"gfx-labs/protocol7-selfdev"}}
	protocol = learning.Scope{Kind: learning.ScopeRepository, Repos: []string{"gfx-labs/protocol7"}}
	wide     = learning.Scope{Kind: learning.ScopeProject}
)

// An entry says what went wrong, why and what worked, and a scope. It is a
// short note: no secret, and no log or transcript.
func TestValidate(t *testing.T) {
	good := entry("a1", "Pushes to master are refused", selfdev)
	good.Normalize()
	require.NoError(t, good.Validate())

	bad := map[string]func(*learning.Entry){
		"no cause":                         func(e *learning.Entry) { e.Cause = "  " },
		"a repository scope without repos": func(e *learning.Entry) { e.Scope.Repos = nil },
		"a project scope with a repo":      func(e *learning.Entry) { e.Scope = learning.Scope{Kind: learning.ScopeProject, Repos: []string{"a/b"}} },
		"an area scope without an area":    func(e *learning.Entry) { e.Scope = learning.Scope{Kind: learning.ScopeArea} },
		"a repo that is not owner/name":    func(e *learning.Entry) { e.Scope.Repos = []string{"protocol7"} },
		"an unknown scope":                 func(e *learning.Entry) { e.Scope.Kind = "galaxy" },
		"a long field":                     func(e *learning.Entry) { e.Fix = strings.Repeat("x", learning.FieldLimit+1) },
		"a pasted log":                     func(e *learning.Entry) { e.Problem = strings.Repeat("ERROR something failed\n", 20) },
		"a GitHub token":                   func(e *learning.Entry) { e.Fix = "use ghp_abcdefghijklmnopqrstuvwxyz0123456789 to push" },
		"a private key":                    func(e *learning.Entry) { e.Fix = "-----BEGIN OPENSSH PRIVATE KEY-----" },
		"an assigned password":             func(e *learning.Entry) { e.Cause = "the password: hunter2hunter2 was stale" },
		"a url with credentials":           func(e *learning.Entry) { e.Fix = "clone https://bot:s3cretpass@github.com/x/y" },
		"a bearer token":                   func(e *learning.Entry) { e.Fix = "send Authorization: Bearer abcdefghijklmnopqrstuvwxyz" },
		"a bad source":                     func(e *learning.Entry) { e.Source.Escalation = "../../x" },
		"a title with lines":               func(e *learning.Entry) { e.Title = "a\nb" },
	}
	for name, change := range bad {
		t.Run(name, func(t *testing.T) {
			e := good
			change(&e)
			e.Normalize()
			assert.Error(t, e.Validate())
		})
	}
	// A commit ID is not a secret.
	ok := good
	ok.Fix = "revert 0123456789abcdef0123456789abcdef01234567 and push again"
	require.NoError(t, ok.Validate())
}

// Search finds accepted entries only. A repository scope fits the repository
// it names, a project scope every one, and words rank title above body.
func TestSearch(t *testing.T) {
	proposed := entry("p", "Proposed note", wide)
	proposed.State = learning.Proposed
	entries := []learning.Entry{
		entry("a", "Pushes to master are refused", selfdev),
		entry("b", "Image build cache", protocol),
		entry("c", "Temporal retention", wide),
		proposed,
	}
	entries[2].Fix = "keep the workflow running, branch rules do not matter"

	ids := func(found []learning.Entry) []string {
		var out []string
		for _, e := range found {
			out = append(out, e.ID)
		}
		return out
	}
	assert.Equal(t, []string{"a", "c"}, ids(learning.Search(entries, learning.Query{Repos: []string{"GFX-labs/protocol7-selfdev"}})), "scope fit first, proposed never")
	assert.Equal(t, []string{"b", "c"}, ids(learning.Search(entries, learning.Query{Repos: []string{"gfx-labs/protocol7"}})))
	assert.Empty(t, learning.Search(entries, learning.Query{Text: "nothing like it"}))
	assert.Equal(t, []string{"a", "b", "c"}, ids(learning.Search(entries, learning.Query{Text: "branch"})), "a body hit in each, ranked by scope fit and then the first by ID")
	assert.Equal(t, []string{"c"}, ids(learning.Search(entries, learning.Query{Text: "retention"})))
	assert.Equal(t, []string{"a"}, ids(learning.Search(entries, learning.Query{Text: "pushes", Repos: []string{"gfx-labs/protocol7-selfdev"}})))
	assert.Empty(t, learning.Search(entries, learning.Query{Text: "pushes", Repos: []string{"gfx-labs/protocol7"}}), "a note for another repository is not found for this one")
	assert.Equal(t, []string{"p"}, ids(learning.Search(entries, learning.Query{State: learning.Proposed})))
}

// The block in a first prompt holds the entries that fit the repositories,
// and never more than its bound, whatever is in the log.
func TestBriefIsBounded(t *testing.T) {
	assert.Empty(t, learning.Brief(nil, []string{"gfx-labs/protocol7"}))
	assert.Empty(t, learning.Brief([]learning.Entry{entry("x", "Other", protocol)}, []string{"gfx-labs/protocol7-selfdev"}), "nothing fits")

	var entries []learning.Entry
	for i := range 40 {
		e := entry(fmt.Sprintf("e%02d", i), fmt.Sprintf("Entry %d", i), selfdev)
		e.Problem = strings.Repeat("a long problem ", 60)
		e.Cause = strings.Repeat("a long cause ", 60)
		e.Fix = strings.Repeat("a long fix ", 60)
		e.Recorded = e.Recorded.Add(time.Duration(i) * time.Hour)
		entries = append(entries, e)
	}
	brief := learning.Brief(entries, []string{"gfx-labs/protocol7-selfdev"})
	assert.LessOrEqual(t, len(brief), learning.MaxBriefBytes)
	assert.LessOrEqual(t, strings.Count(brief, "\n- "), learning.MaxBriefEntries)
	assert.Contains(t, brief, "Entry 39", "the newest entry is first")
	assert.Contains(t, brief, "learning_search")
	assert.Contains(t, brief, "more: call learning_search")

	// Each entry has its own bound, and a cut keeps the text valid.
	e := entry("u", "Unicode", wide)
	e.Fix = strings.Repeat("é", 900)
	note := learning.Format(e, 200)
	assert.LessOrEqual(t, len(note), 200)
	assert.True(t, strings.ToValidUTF8(note, "") == note)

	// A few small entries are all there, with the source.
	few := learning.Brief([]learning.Entry{entry("1", "One", selfdev), entry("2", "Two", wide)}, []string{"gfx-labs/protocol7-selfdev"})
	assert.Contains(t, few, "One")
	assert.Contains(t, few, "Two")
	assert.Contains(t, few, "escalation e1")
	assert.NotContains(t, few, "more: call")
}

// A chat's agent proposes an entry, the handler records an accepted one, and a
// repeated call (an activity that is retried) records it once.
func TestRecordProposesAndTheHandlerAccepts(t *testing.T) {
	log := learningtest.New(t)
	activities := log.Activities()
	ctx := context.Background()

	proposed, err := activities.Record(ctx, learning.RecordRequest{Entry: entry("p1", "A proposal", selfdev)})
	require.NoError(t, err)
	assert.Equal(t, learning.Proposed, proposed.State, "an entry that says accepted is proposed unless the handler recorded it")
	accepted, err := activities.Record(ctx, learning.RecordRequest{Entry: entry("h1", "From the handler", selfdev), Handler: true})
	require.NoError(t, err)
	assert.Equal(t, learning.Accepted, accepted.State)
	_, err = activities.Record(ctx, learning.RecordRequest{Entry: entry("h1", "From the handler", selfdev), Handler: true})
	require.NoError(t, err)
	assert.Len(t, log.Entries(), 2)

	// Chats read accepted entries only, until the proposal is accepted.
	found, err := activities.Search(ctx, learning.SearchRequest{Query: learning.Query{Repos: []string{"gfx-labs/protocol7-selfdev"}}})
	require.NoError(t, err)
	assert.Contains(t, found, "From the handler")
	assert.NotContains(t, found, "A proposal")
	_, err = activities.Decide(ctx, learning.DecideRequest{ID: "p1", Decision: learning.Decision{Action: "accept", By: "alice"}})
	require.NoError(t, err)
	found, err = activities.Search(ctx, learning.SearchRequest{Query: learning.Query{Text: "proposal"}})
	require.NoError(t, err)
	assert.Contains(t, found, "A proposal")
	assert.Contains(t, found, "id p1")

	// An entry with a secret is refused for good.
	secret := entry("s1", "Leak", selfdev)
	secret.Fix = "token=abcdefghijklmnop1234"
	_, err = activities.Record(ctx, learning.RecordRequest{Entry: secret})
	require.Error(t, err)
	assert.Len(t, log.Entries(), 2)

	// A retired entry leaves the log, and cannot be accepted twice.
	_, err = activities.Decide(ctx, learning.DecideRequest{ID: "p1", Decision: learning.Decision{Action: "accept"}})
	require.Error(t, err, "an accepted entry is not accepted again")
	_, err = activities.Decide(ctx, learning.DecideRequest{ID: "p1", Decision: learning.Decision{Action: "retire"}})
	require.NoError(t, err)
	assert.Len(t, log.Entries(), 1)
	brief, err := activities.Brief(ctx, learning.BriefRequest{Repos: []string{"gfx-labs/protocol7-selfdev"}})
	require.NoError(t, err)
	assert.Contains(t, brief, "From the handler")
	assert.NotContains(t, brief, "A proposal")
}

// The workflow keeps an entry while it is proposed or accepted, takes only
// the decisions its state allows, and ends on a rejection.
func TestWorkflowDecisions(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(learning.Workflow, workflow.RegisterOptions{Name: learning.WorkflowName})
	proposed := entry("w1", "Wait", selfdev)
	proposed.State = learning.Proposed
	var refused, rejected error
	update := func(name string, action string, done func(error)) {
		env.UpdateWorkflow(learning.UpdateDecide, name, &testsuite.TestUpdateCallback{
			OnAccept:   func() {},
			OnReject:   done,
			OnComplete: func(_ any, err error) { done(err) },
		}, learning.Decision{Action: action, By: "alice"})
	}
	env.RegisterDelayedCallback(func() { update("retire-early", "retire", func(err error) { refused = err }) }, time.Minute)
	env.RegisterDelayedCallback(func() { update("reject", "reject", func(err error) { rejected = err }) }, 2*time.Minute)
	env.ExecuteWorkflow(learning.WorkflowName, proposed)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Error(t, refused, "a proposed entry is not retired")
	require.NoError(t, rejected)
	var result learning.Entry
	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, learning.Rejected, result.State)
}
