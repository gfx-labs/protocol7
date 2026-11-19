package chat_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/orchestration/learning"
	"github.com/gfx-labs/protocol7/pkg/orchestration/learning/learningtest"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

func TestLearningWorkflowRunsOnTheChatQueue(t *testing.T) {
	assert.Equal(t, chat.TaskQueue, learning.TaskQueue)
}

func learningChat(first string) chat.State {
	state := newChat(first)
	state.Request.Repositories = []chat.Repository{{Name: "protocol7-selfdev", URL: "git@github.com:gfx-labs/protocol7-selfdev.git"}}
	return state
}

func registerLearnings(env *testsuite.TestWorkflowEnvironment, log *learningtest.Log) {
	activities := log.Activities()
	env.RegisterActivityWithOptions(activities.Record, activity.RegisterOptions{Name: learning.ActivityRecord})
	env.RegisterActivityWithOptions(activities.Search, activity.RegisterOptions{Name: learning.ActivitySearch})
	env.RegisterActivityWithOptions(activities.Brief, activity.RegisterOptions{Name: learning.ActivityBrief})
	env.RegisterActivityWithOptions(activities.Decide, activity.RegisterOptions{Name: learning.ActivityDecide})
}

// A learning that an ordinary chat records is proposed: it reaches the page and
// not the next chat. Once accepted, a new chat in the same repository finds it
// with learning_search and has it in its first prompt, and only in the first.
func TestARecordedLearningReachesANewChat(t *testing.T) {
	log := learningtest.New(t)

	// The first chat proposes a learning.
	env, fake := newEnvironment(t)
	registerLearnings(env, log)
	var proposed, refused string
	fake.onSlow = func() {
		fake.mu.Lock()
		session := fake.opened[0].ID
		fake.mu.Unlock()
		toolCall(t, env, session, "learning_record", map[string]any{
			"title": "Pushes to master are refused", "problem": "git push to master failed", "cause": "the branch is protected",
			"fix": "push the chat branch and open a pull request", "scope": "repository", "repos": []string{"gfx-labs/protocol7-selfdev"}, "escalation": "esc1",
		}, func(result p7.ToolResult) { proposed = result.Content[0].Text })
		// A secret is refused, and nothing is recorded.
		raw := []byte(`{"title":"Leak","problem":"p","cause":"c","fix":"use token=abcdefghijklmnop1234","scope":"project"}`)
		env.UpdateWorkflow(domain.UpdateToolCall, "leak", &testsuite.TestUpdateCallback{
			OnAccept: func() {}, OnReject: func(err error) { refused = err.Error() },
			OnComplete: func(value any, err error) {
				if err != nil {
					refused = err.Error()
				} else if result := value.(p7.ToolResult); result.IsError {
					refused = result.Content[0].Text
				}
			},
		}, p7.ToolCall{Session: session, Tool: "learning_record", Arguments: raw})
		close(fake.release)
	}
	env.ExecuteWorkflow(chat.WorkflowName, learningChat("slow"))
	_ = env.GetWorkflowError()
	require.Len(t, log.Entries(), 1)
	assert.Contains(t, refused, "secret")
	recorded := log.Entries()[0]
	assert.Equal(t, learning.Proposed, recorded.State)
	assert.Equal(t, "esc1", recorded.Source.Escalation)
	assert.Equal(t, "c1", recorded.Source.Chat)
	assert.Equal(t, []string{"gfx-labs/protocol7-selfdev"}, recorded.Scope.Repos)
	assert.Contains(t, proposed, "is proposed")

	// A new chat sees nothing yet.
	empty, _ := runFirstTurn(t, log, learningChat("hello"))
	assert.NotContains(t, empty, "Pushes to master")

	// A person accepts it.
	_, err := log.Store().Decide(t.Context(), recorded.ID, learning.Decision{Action: "accept", By: "alice"})
	require.NoError(t, err)

	// A new chat in the same repository has it in its first prompt, within the bound.
	first, searched := runFirstTurn(t, log, learningChat("hello"), func(env *testsuite.TestWorkflowEnvironment, session string, out *string) {
		toolCall(t, env, session, "learning_search", map[string]any{"query": "protected branch", "repo": "gfx-labs/protocol7-selfdev"}, func(result p7.ToolResult) { *out = result.Content[0].Text })
	})
	assert.Contains(t, first, "Pushes to master are refused")
	assert.Contains(t, first, "the branch is protected")
	assert.Contains(t, first, "escalation esc1")
	assert.LessOrEqual(t, len(first), 12000)
	assert.Contains(t, searched, "Pushes to master are refused")

	// A chat in another repository does not.
	other := learningChat("hello")
	other.Request.Repositories = []chat.Repository{{Name: "protocol7", URL: "git@github.com:gfx-labs/protocol7.git"}}
	elsewhere, _ := runFirstTurn(t, log, other)
	assert.NotContains(t, elsewhere, "Pushes to master")
}

// runFirstTurn runs a chat whose first turn gives the context the agent got, and
// returns it, and what the optional tool call said. The chat's second message
// shows that the block comes with the first turn only.
func runFirstTurn(t *testing.T, log *learningtest.Log, state chat.State, during ...func(*testsuite.TestWorkflowEnvironment, string, *string)) (string, string) {
	t.Helper()
	env, fake := newEnvironment(t)
	registerLearnings(env, log)
	var out string
	env.RegisterDelayedCallback(func() {
		fake.mu.Lock()
		session := fake.opened[0].ID
		fake.mu.Unlock()
		for _, call := range during {
			call(env, session, &out)
		}
		env.SignalWorkflow(chat.SignalMessage, chat.Message{ID: "m2", Text: "again"})
	}, time.Minute)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, 2*time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, state)
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, fake.turns, 2)
	assert.NotContains(t, fake.turns[1].Context, "Learnings recorded")
	return fake.turns[0].Context, out
}

// The handler's chat records accepted entries and decides on proposals. No
// other chat has learning_decide.
func TestTheHandlerRecordsAndDecides(t *testing.T) {
	log := learningtest.New(t)
	proposal := learning.Entry{ID: "p1", Title: "Proposal", Problem: "p", Cause: "c", Fix: "f", Scope: learning.Scope{Kind: learning.ScopeProject}, State: learning.Proposed, Recorded: time.Now()}
	require.NoError(t, log.Store().Record(t.Context(), proposal))

	env, fake := newEnvironment(t)
	registerLearnings(env, log)
	var recorded, listed, decided string
	fake.onSlow = func() {
		fake.mu.Lock()
		session := fake.opened[0].ID
		fake.mu.Unlock()
		toolCall(t, env, session, "learning_record", map[string]any{"title": "From the handler", "problem": "p", "cause": "c", "fix": "f", "scope": "area", "area": "CI"}, func(result p7.ToolResult) { recorded = result.Content[0].Text })
		// One after the other: a decision changes what the search lists.
		toolCall(t, env, session, "learning_search", map[string]any{"state": "proposed"}, func(result p7.ToolResult) {
			listed = result.Content[0].Text
			toolCall(t, env, session, "learning_decide", map[string]any{"id": "p1", "action": "accept"}, func(result p7.ToolResult) {
				decided = result.Content[0].Text
				close(fake.release)
			})
		})
	}
	state := newChat("slow")
	state.Request.Handler = true
	env.ExecuteWorkflow(chat.WorkflowName, state)
	_ = env.GetWorkflowError()
	assert.Contains(t, recorded, "accepted")
	assert.Contains(t, listed, "id p1")
	assert.Contains(t, decided, "accepted")
	assert.Len(t, log.Entries(), 2)
	for _, entry := range log.Entries() {
		assert.Equal(t, learning.Accepted, entry.State, entry.Title)
		assert.Contains(t, []string{"ci", ""}, entry.Scope.Area)
	}
}
