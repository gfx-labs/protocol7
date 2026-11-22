package learning_test

import (
	"context"
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

// A chat's agent proposes an entry, the handler records an accepted one, and a
// repeated call (an activity that is retried) records it once. The SDK stores
// the entry's vector as it is and computes none.
func TestRecordProposesAndTheHandlerAccepts(t *testing.T) {
	log := learningtest.New(t)
	activities := log.Activities()
	ctx := context.Background()

	proposed, err := activities.Record(ctx, learning.RecordRequest{Entry: entry("p1", "A proposal", selfdev)})
	require.NoError(t, err)
	assert.Equal(t, learning.Proposed, proposed.State, "an entry that says accepted is proposed unless the handler recorded it")
	withVector := entry("h1", "From the handler", selfdev)
	withVector.Vector, withVector.VectorModel = embedding.Vector{0.5, -1}, "model"
	accepted, err := activities.Record(ctx, learning.RecordRequest{Entry: withVector, Handler: true})
	require.NoError(t, err)
	assert.Equal(t, learning.Accepted, accepted.State)
	_, err = activities.Record(ctx, learning.RecordRequest{Entry: withVector, Handler: true})
	require.NoError(t, err)
	assert.Len(t, log.Entries(), 2)
	assert.Equal(t, embedding.Vector{0.5, -1}, log.Entries()[1].Vector)
	assert.Equal(t, "model", log.Entries()[1].VectorModel)

	store := log.Store()
	got, err := store.Get(ctx, "p1")
	require.NoError(t, err)
	assert.Equal(t, "A proposal", got.Title)
	assert.Equal(t, learning.Proposed, got.State)
	_, err = store.Get(ctx, "nobody")
	assert.ErrorIs(t, err, learning.ErrNotFound)
	_, err = store.Get(ctx, "A!")
	assert.ErrorIs(t, err, learning.ErrNotFound)

	_, err = activities.Decide(ctx, learning.DecideRequest{ID: "p1", Decision: learning.Decision{Action: "accept", By: "alice"}})
	require.NoError(t, err)
	got, err = store.Get(ctx, "p1")
	require.NoError(t, err)
	assert.Equal(t, learning.Accepted, got.State)

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
	listed, err := store.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	assert.Equal(t, "h1", listed[0].ID)
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
