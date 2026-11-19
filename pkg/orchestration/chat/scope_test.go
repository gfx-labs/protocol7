package chat_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/domain/scope"
	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

var (
	tradeRepository  = chat.Repository{Name: "trade", URL: "https://github.com/oku-trade/trade.git"}
	retailRepository = chat.Repository{Name: "retail", URL: "https://github.com/oku-trade/retail.git"}
	otherRepository  = chat.Repository{Name: "other", URL: "https://github.com/someone/other.git"}
	// parentScope is what the person's chat holds: one repository, read and
	// write of contents, read of checks.
	parentScope = scope.NewScope("oku", []string{"oku-trade/trade"}, map[string]string{"contents": "write", "checks": "read"})
)

func scopedChat(first string) chat.State {
	state := newChat(first)
	scope := parentScope
	// The request lists more repositories than the scope reaches, as one built
	// by hand might: only the scope's are cloned.
	state.Request.Repositories = []chat.Repository{tradeRepository, retailRepository, otherRepository}
	state.Request.Project, state.Request.Restricted, state.Request.Scope = "oku", true, &scope
	return state
}

func assertWithin(t *testing.T, what string, got *scope.Scope, parent scope.Scope) {
	t.Helper()
	if got == nil {
		t.Errorf("%s has no scope", what)
		return
	}
	if !got.Within(parent) {
		t.Errorf("%s holds %s, which is more than its parent's %s", what, got.Key(), parent.Key())
	}
}

// The session clones only what the scope reaches, and its first prompt says what
// the scope is.
func TestAChatClonesOnlyItsScope(t *testing.T) {
	env, fake := newEnvironment(t)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, scopedChat("hello"))
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, fake.opened, 1)
	var cloned []string
	for _, input := range fake.opened[0].Inputs {
		cloned = append(cloned, input.Destination)
	}
	assert.Equal(t, []string{"/trade"}, cloned)
	require.NotEmpty(t, fake.turns)
	assert.Contains(t, fake.turns[0].Context, "oku-trade/trade")
	assert.Contains(t, fake.turns[0].Context, "contents: write")
	assert.NotContains(t, fake.turns[0].Context, "someone/other")
}

// A child chat, and the escalation a chat raises, take the parent's scope. What
// the agent writes in the tool's arguments is not read, so it cannot ask for a
// wider one, and the repositories of the child are those of the scope.
func TestChildrenAndEscalationsNeverExceedTheirParent(t *testing.T) {
	env, fake := newEnvironment(t)
	var children []chat.State
	var escalations []chat.EscalationRequest
	env.SetOnChildWorkflowStartedListener(func(_ *workflow.Info, _ workflow.Context, args converter.EncodedValues) {
		var child chat.State
		if args.Get(&child) == nil && child.Request.Parent != "" {
			children = append(children, child)
			return
		}
		var escalation chat.EscalationState
		if args.Get(&escalation) == nil {
			escalations = append(escalations, escalation.Request)
		}
	})
	env.OnWorkflow(chat.EscalationWorkflowName, mock.Anything, mock.Anything).Return(chat.EscalationState{}, nil)
	env.OnRequestCancelExternalWorkflow(mock.Anything, mock.Anything, "").Return(nil)
	fake.onSlow = func() {
		fake.mu.Lock()
		session := fake.opened[0].ID
		fake.mu.Unlock()
		wider := map[string]any{"project": "selfdev", "repositories": []string{"gfx-labs/protocol7"}, "permissions": map[string]string{"administration": "write"}}
		toolCall(t, env, session, "chat_spawn", map[string]any{"task": "look", "scope": wider, "repositories": []string{"someone/other"}}, func(res p7.ToolResult) {
			if res.IsError {
				t.Errorf("chat_spawn: %+v", res)
			}
			toolCall(t, env, session, "escalate", map[string]any{"need": "n", "why": "w", "scope": wider}, func(p7.ToolResult) {
				close(fake.release)
			})
		})
	}
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, 3*time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, scopedChat("slow"))
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, children, 1)
	assertWithin(t, "the child", children[0].Request.Scope, parentScope)
	assert.True(t, children[0].Request.Scope.Equal(parentScope), "the child holds %s", children[0].Request.Scope.Key())
	assert.Equal(t, []chat.Repository{tradeRepository}, children[0].Request.Repositories)
	assert.Equal(t, "oku", children[0].Request.Project)
	require.Len(t, escalations, 1)
	require.NotNil(t, escalations[0].Session)
	assertWithin(t, "the escalation", escalations[0].Session.Scope, parentScope)
}

// The handler's chat holds the scope of the chat it serves, and an escalation
// that carries none, from before scopes, gives it none to widen.
func TestTheHandlerHoldsTheScopeOfTheChatItServes(t *testing.T) {
	h := newHandlerEnvironment(t)
	request := intake("e1", "c1", "need")
	scope := parentScope
	request.Request.Session.Scope = &scope
	request.Request.Session.Repositories = []chat.Repository{tradeRepository, retailRepository, otherRepository}
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(chat.SignalEscalationIntake, request) }, time.Minute)
	h.env.RegisterDelayedCallback(func() {
		require.Len(t, h.started, 1)
		h.endTurn(t, 0, "")
	}, 2*time.Minute)
	h.env.RegisterDelayedCallback(func() { h.env.CancelWorkflow() }, 3*time.Minute)
	h.env.ExecuteWorkflow(chat.HandlerWorkflowName, chat.HandlerState{})
	require.Len(t, h.started, 1)
	got := h.started[0].Request
	assertWithin(t, "the handler's chat", got.Scope, parentScope)
	assert.Equal(t, []chat.Repository{tradeRepository}, got.Repositories)
	assert.True(t, got.Handler)
	assert.Equal(t, "tenant", got.Project)
}

// Chats with different scopes never share a pod, so a token that is fixed by
// the pod's pool cannot reach across them. A chat with no recorded scope keeps
// the name it had.
func TestPoolKeyDiffersAcrossScopes(t *testing.T) {
	base := baseRequest()
	none := chat.PoolKey(base, tokenScope)
	scoped := func(scope scope.Scope) string {
		request := base
		request.Scope = &scope
		return chat.PoolKey(request, tokenScope)
	}
	full := scope.NewScope("oku", []string{"oku-trade/trade", "oku-trade/retail"}, map[string]string{"contents": "write", "checks": "read"})
	keys := map[string]string{
		"none":          none,
		"full":          scoped(full),
		"one repo":      scoped(scope.NewScope("oku", []string{"oku-trade/trade"}, full.Permissions)),
		"read only":     scoped(full.ReadOnly()),
		"fewer perms":   scoped(scope.NewScope("oku", full.Repositories, map[string]string{"contents": "write"})),
		"other project": scoped(scope.NewScope("website", full.Repositories, full.Permissions)),
	}
	seen := map[string]string{}
	for name, key := range keys {
		if other, ok := seen[key]; ok {
			t.Errorf("%s and %s share the pool %s", name, other, key)
		}
		seen[key] = name
		assert.NoError(t, domain.ValidPool(key), name)
	}
	// The same scope, however it was written down, names the same pool.
	assert.Equal(t, keys["full"], scoped(scope.NewScope("oku", []string{"oku-trade/retail", "OKU-trade/trade"}, full.Permissions)))
}
