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
	"github.com/gfx-labs/protocol7/pkg/support/embedding"
)

func entry(id, title string, scope learning.Scope) learning.Entry {
	return learning.Entry{
		ID: id, Title: title, Problem: "the push was refused", Cause: "the branch is protected", Fix: "open a pull request",
		Scope: scope, Source: learning.Source{Chat: "c1", Escalation: "e" + id}, Recorded: time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC), State: learning.Open,
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

// Every entry is recorded open, whoever records it: nobody accepts it, and
// the curator settles it. A repeated call (an activity that is retried)
// records it once. The SDK stores the entry's vector as it is and computes
// none.
func TestRecordOpensAndDecisionsSettle(t *testing.T) {
	log := learningtest.New(t)
	activities := log.Activities()
	ctx := context.Background()

	proposed, err := activities.Record(ctx, learning.RecordRequest{Entry: entry("p1", "A proposal", selfdev)})
	require.NoError(t, err)
	assert.Equal(t, learning.Open, proposed.State, "an entry that says accepted is open all the same")
	withVector := entry("h1", "From the handler", selfdev)
	withVector.Vector, withVector.VectorModel = embedding.Vector{0.5, -1}, "model"
	withVector.PR, withVector.Note = "https://example.com/pull/1", "set by the caller"
	opened, err := activities.Record(ctx, learning.RecordRequest{Entry: withVector, Handler: true})
	require.NoError(t, err)
	assert.Equal(t, learning.Open, opened.State, "the handler's entry is open too")
	_, err = activities.Record(ctx, learning.RecordRequest{Entry: withVector, Handler: true})
	require.NoError(t, err)
	require.Len(t, log.Entries(), 2)
	assert.Equal(t, embedding.Vector{0.5, -1}, log.Entries()[1].Vector)
	assert.Equal(t, "model", log.Entries()[1].VectorModel)
	assert.Empty(t, log.Entries()[1].PR, "a new entry carries no settlement")
	assert.Empty(t, log.Entries()[1].Note)

	store := log.Store()
	got, err := store.Get(ctx, "p1")
	require.NoError(t, err)
	assert.Equal(t, "A proposal", got.Title)
	assert.Equal(t, learning.Open, got.State)
	_, err = store.Get(ctx, "nobody")
	assert.ErrorIs(t, err, learning.ErrNotFound)
	_, err = store.Get(ctx, "A!")
	assert.ErrorIs(t, err, learning.ErrNotFound)

	// An entry with a secret is refused for good.
	secret := entry("s1", "Leak", selfdev)
	secret.Fix = "token=abcdefghijklmnop1234"
	_, err = activities.Record(ctx, learning.RecordRequest{Entry: secret})
	require.Error(t, err)
	assert.Len(t, log.Entries(), 2)

	// An apply links its PR and stays in the log, readable. A decision that
	// lacks what it needs is refused for good, before the workflow.
	_, err = activities.Decide(ctx, learning.DecideRequest{ID: "p1", Decision: learning.Decision{Action: "apply"}})
	require.Error(t, err)
	assert.ErrorContains(t, err, "pull request")
	_, err = activities.Decide(ctx, learning.DecideRequest{ID: "p1", Decision: learning.Decision{Action: "apply", PR: "javascript:alert(1)"}})
	require.Error(t, err)
	applied, err := activities.Decide(ctx, learning.DecideRequest{ID: "p1", Decision: learning.Decision{Action: "apply", PR: "https://github.com/gfx-labs/protocol7/pull/9", Note: "a skill line", By: "curator", Pass: "learning-curate-1"}})
	require.NoError(t, err)
	assert.Equal(t, learning.Applied, applied.State)
	got, err = store.Get(ctx, "p1")
	require.NoError(t, err)
	assert.Equal(t, learning.Applied, got.State)
	assert.Equal(t, "https://github.com/gfx-labs/protocol7/pull/9", got.PR)
	assert.Equal(t, "curator", got.SettledBy)
	assert.False(t, got.Settled.IsZero())
	assert.True(t, got.State.Readable())
	_, err = activities.Decide(ctx, learning.DecideRequest{ID: "p1", Decision: learning.Decision{Action: "reject", Note: "again"}})
	require.Error(t, err, "an applied entry is not rejected")

	// A cover needs its note and ends the entry, which Get still reads.
	_, err = activities.Decide(ctx, learning.DecideRequest{ID: "h1", Decision: learning.Decision{Action: "cover"}})
	require.Error(t, err)
	_, err = activities.Decide(ctx, learning.DecideRequest{ID: "h1", Decision: learning.Decision{Action: "cover", Note: "AGENTS.md line 3 says it"}})
	require.NoError(t, err)
	got, err = store.Get(ctx, "h1")
	require.NoError(t, err)
	assert.Equal(t, learning.Covered, got.State)
	assert.Equal(t, "AGENTS.md line 3 says it", got.Note)
	assert.False(t, got.State.Readable())
	require.Len(t, log.Entries(), 1)
	assert.Len(t, log.Closed(), 1)

	// A person retires an applied entry, and it leaves the log.
	_, err = activities.Decide(ctx, learning.DecideRequest{ID: "p1", Decision: learning.Decision{Action: "retire"}})
	require.NoError(t, err)
	listed, err := store.List(ctx)
	require.NoError(t, err)
	assert.Empty(t, listed)
	all, err := store.ListAll(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 2, "the settled entries are read until Temporal's retention ends")
}

// Only an open entry is settled, in the states and with the details its action
// needs.
func TestSettle(t *testing.T) {
	now := time.Date(2026, 11, 22, 9, 0, 0, 0, time.UTC)
	open := entry("o1", "Open", selfdev)
	open.State = learning.Open
	legacy := open
	legacy.State = learning.Proposed

	for _, start := range []learning.Entry{open, legacy} {
		applied, err := start.Settle(learning.Decision{Action: "apply", PR: "https://github.com/a/b/pull/1", By: "c", Pass: "p"}, now)
		require.NoError(t, err)
		assert.Equal(t, learning.Applied, applied.State)
		assert.Equal(t, "https://github.com/a/b/pull/1", applied.PR)
		assert.Equal(t, now, applied.Settled)
		covered, err := start.Settle(learning.Decision{Action: "cover", Note: " says it "}, now)
		require.NoError(t, err)
		assert.Equal(t, learning.Covered, covered.State)
		assert.Equal(t, "says it", covered.Note)
		rejected, err := start.Settle(learning.Decision{Action: "reject", Note: "not true any more"}, now)
		require.NoError(t, err)
		assert.Equal(t, learning.Rejected, rejected.State)
		accepted, err := start.Settle(learning.Decision{Action: "accept"}, now)
		require.NoError(t, err)
		assert.Equal(t, learning.Accepted, accepted.State)
		assert.True(t, accepted.Settled.IsZero(), "an accept does not settle")
	}

	applied, _ := open.Settle(learning.Decision{Action: "apply", PR: "https://github.com/a/b/pull/1"}, now)
	for name, decision := range map[string]learning.Decision{
		"apply again":      {Action: "apply", PR: "https://github.com/a/b/pull/2"},
		"cover":            {Action: "cover", Note: "x"},
		"reject":           {Action: "reject"},
		"an unknown":       {Action: "delete"},
		"retire with a pr": {Action: "retire", PR: "https://github.com/a/b/pull/3"},
	} {
		_, err := applied.Settle(decision, now)
		assert.Error(t, err, name)
	}
	_, err := open.Settle(learning.Decision{Action: "retire"}, now)
	assert.Error(t, err, "an open entry is rejected, not retired")
	_, err = open.Settle(learning.Decision{Action: "apply", PR: "http://insecure.example/pull/1"}, now)
	assert.Error(t, err, "a link is https")
	_, err = open.Settle(learning.Decision{Action: "cover", Note: "token=abcdefghijklmnop1234"}, now)
	assert.Error(t, err, "a note holds no secret")
	_, err = open.Settle(learning.Decision{Action: "reject", PR: "https://github.com/a/b/pull/1"}, now)
	assert.Error(t, err, "only an apply links a pull request")
	retired, err := applied.Settle(learning.Decision{Action: "retire"}, now)
	require.NoError(t, err)
	assert.Equal(t, learning.Retired, retired.State)
	assert.Equal(t, "https://github.com/a/b/pull/1", retired.PR, "a retired entry keeps its link")

	for state, want := range map[learning.State]struct{ open, readable, ended bool }{
		learning.Open: {true, true, false}, learning.Proposed: {true, true, false}, learning.Accepted: {false, true, false},
		learning.Applied: {false, true, false}, learning.Covered: {false, false, true}, learning.Rejected: {false, false, true}, learning.Retired: {false, false, true},
	} {
		assert.Equal(t, want.open, state.IsOpen(), state)
		assert.Equal(t, want.readable, state.Readable(), state)
		assert.Equal(t, want.ended, state.Ended(), state)
	}
}

// The log's activities and the workflow register under the names that running
// histories hold.
func TestRegisterNames(t *testing.T) {
	r := &names{}
	require.NoError(t, learning.Register(r, &learning.Activities{}))
	assert.Equal(t, []string{"selfdev.Learning", "LearningRecord", "LearningDecide"}, r.names)
	r = &names{}
	require.NoError(t, learning.RegisterWorkflow(r))
	assert.Equal(t, []string{"selfdev.Learning"}, r.names)
	assert.Error(t, learning.Register(nil, &learning.Activities{}))
	assert.Error(t, learning.Register(r, nil))
}

type names struct{ names []string }

func (n *names) RegisterWorkflow(name string, _ any) { n.names = append(n.names, name) }
func (n *names) RegisterActivity(name string, _ any) { n.names = append(n.names, name) }

// The workflow keeps an entry while it is open, accepted or applied, takes
// only the decisions its state allows, rewrites its memo when one settles it,
// and ends on a cover, a rejection or a retirement.
func TestWorkflowDecisions(t *testing.T) {
	run := func(t *testing.T, decisions ...learning.Decision) (learning.Entry, []error) {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		env.RegisterWorkflowWithOptions(learning.Workflow, workflow.RegisterOptions{Name: learning.WorkflowName})
		open := entry("w1", "Wait", selfdev)
		open.State = learning.Open
		errs := make([]error, len(decisions))
		for i, decision := range decisions {
			env.RegisterDelayedCallback(func() {
				env.UpdateWorkflow(learning.UpdateDecide, fmt.Sprint("update-", i), &testsuite.TestUpdateCallback{
					OnAccept:   func() {},
					OnReject:   func(err error) { errs[i] = err },
					OnComplete: func(_ any, err error) { errs[i] = err },
				}, decision)
			}, time.Duration(i+1)*time.Minute)
		}
		env.ExecuteWorkflow(learning.WorkflowName, open)
		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())
		var result learning.Entry
		require.NoError(t, env.GetWorkflowResult(&result))
		return result, errs
	}

	t.Run("a rejection ends it", func(t *testing.T) {
		result, errs := run(t, learning.Decision{Action: "retire"}, learning.Decision{Action: "reject", Note: "no longer true", By: "alice"})
		require.Error(t, errs[0], "an open entry is not retired")
		require.NoError(t, errs[1])
		assert.Equal(t, learning.Rejected, result.State)
		assert.Equal(t, "no longer true", result.Note)
	})
	t.Run("a cover ends it", func(t *testing.T) {
		result, errs := run(t, learning.Decision{Action: "cover"}, learning.Decision{Action: "cover", Note: "skill says it"})
		require.Error(t, errs[0], "a cover names its line")
		require.NoError(t, errs[1])
		assert.Equal(t, learning.Covered, result.State)
	})
	t.Run("an apply keeps it until a retirement", func(t *testing.T) {
		result, errs := run(t,
			learning.Decision{Action: "apply", PR: "https://github.com/gfx-labs/protocol7/pull/9", By: "curator", Pass: "learning-curate-1"},
			learning.Decision{Action: "reject", Note: "x"},
			learning.Decision{Action: "retire", By: "alice"})
		require.NoError(t, errs[0])
		require.Error(t, errs[1], "an applied entry is not rejected")
		require.NoError(t, errs[2])
		assert.Equal(t, learning.Retired, result.State)
		assert.Equal(t, "https://github.com/gfx-labs/protocol7/pull/9", result.PR)
		assert.Equal(t, "learning-curate-1", result.Pass)
	})
}
