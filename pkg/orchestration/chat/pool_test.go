package chat_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

var coreRepositories = []chat.Repository{
	{Name: "protocol7", URL: "git@github.com:gfx-labs/protocol7.git"},
	{Name: "protocol7-selfdev", URL: "git@github.com:gfx-labs/protocol7-selfdev.git"},
}

const tokenScope = "protocol7,protocol7-selfdev;contents=write"

func baseRequest() chat.Request {
	return chat.Request{ID: "root", Repositories: coreRepositories, Models: []string{"claude:work/claude-opus-5-5#medium"}, Backend: "kubernetes"}
}

// The name is the same for the same permissions and changes with each thing
// that decides them, so two sessions share a pool only when every one matches.
func TestPoolKeyChangesWithEveryPermission(t *testing.T) {
	base := baseRequest()
	key := chat.PoolKey(base, tokenScope)
	if domain.ValidPool(key) != nil {
		t.Fatalf("the name %q is not one protocol7 takes", key)
	}
	if again := chat.PoolKey(base, tokenScope); again != key {
		t.Errorf("the name is not stable: %q and %q", key, again)
	}
	reordered := base
	reordered.Repositories = []chat.Repository{coreRepositories[1], coreRepositories[0]}
	reordered.Models = []string{"claude:work/claude-opus-5-5#low"}
	reordered.ID, reordered.Parent = "another chat", "its parent"
	reordered.Title, reordered.StartedBy, reordered.Model, reordered.Host = "another title", &chat.Person{Login: "x"}, "other name", "other"
	if got := chat.PoolKey(reordered, tokenScope); got != key {
		t.Errorf("a repository order, a model of the same account, a chat ID or a title changed the name")
	}

	for name, change := range map[string]func(*chat.Request) string{
		"project":         func(r *chat.Request) string { r.Project = "tenant"; return tokenScope },
		"repository set":  func(r *chat.Request) string { r.Repositories = r.Repositories[:1]; return tokenScope },
		"repository url":  func(r *chat.Request) string { r.Repositories[0].URL += "x"; return tokenScope },
		"repository name": func(r *chat.Request) string { r.Repositories[0].Name += "x"; return tokenScope },
		"restricted":      func(r *chat.Request) string { r.Restricted = true; return tokenScope },
		"issue": func(r *chat.Request) string {
			r.Issue = &chat.Issue{Repo: "a/b", Number: 1}
			return tokenScope
		},
		"another issue": func(r *chat.Request) string {
			r.Issue = &chat.Issue{Repo: "a/b", Number: 2}
			return tokenScope
		},
		"escalation":    func(r *chat.Request) string { r.Escalation = &chat.Escalation{}; return tokenScope },
		"handler":       func(r *chat.Request) string { r.Handler = true; return tokenScope },
		"model account": func(r *chat.Request) string { r.Models = []string{"codex-1/gpt"}; return tokenScope },
		"fallback account": func(r *chat.Request) string {
			r.Models = []string{"claude:work/claude-opus-5-5,openrouter/x"}
			return tokenScope
		},
		"no models":   func(r *chat.Request) string { r.Models = nil; return tokenScope },
		"token scope": func(r *chat.Request) string { return tokenScope + ",more" },
	} {
		request := base
		request.Repositories = append([]chat.Repository(nil), base.Repositories...)
		scope := change(&request)
		if got := chat.PoolKey(request, scope); got == key || domain.ValidPool(got) != nil {
			t.Errorf("a different %s gave the name %q, want another valid one than %q", name, got, key)
		}
	}
	// Handlers of two escalations do not share a pool.
	h1, h2 := base, base
	h1.Handler, h1.Serves = true, &chat.Serving{Escalation: "e1", Chat: "c1"}
	h2.Handler, h2.Serves = true, &chat.Serving{Escalation: "e2", Chat: "c1"}
	if chat.PoolKey(h1, tokenScope) == chat.PoolKey(h2, tokenScope) {
		t.Error("handlers of two escalations have one name")
	}
	// A name and a URL that run together are not another set's.
	a, b := base, base
	a.Repositories = []chat.Repository{{Name: "a", URL: "b=c"}}
	b.Repositories = []chat.Repository{{Name: "a=b", URL: "c"}}
	if chat.PoolKey(a, tokenScope) == chat.PoolKey(b, tokenScope) {
		t.Error("a name and a URL that run together gave the name of another set")
	}
	if got := chat.PoolKey(base, ""); got != "" {
		t.Errorf("with no scope pools are off, got %q", got)
	}
}

// poolRequests stand in for the kubernetes backend's activities that place
// sessions in a pool.
type poolRequests struct {
	mu       sync.Mutex
	placed   []domain.PlaceRequest
	released []domain.ReleaseRequest
	deleted  []domain.PoolSnapshotsRequest
}

func (p *poolRequests) place(_ context.Context, request domain.PlaceRequest) (domain.PoolPlacement, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.placed = append(p.placed, request)
	return domain.PoolPlacement{Worker: 1, Pod: domain.PoolPodName(request.Pool.Name, 1), TaskQueue: domain.PoolQueue(request.Pool.Name, 1)}, nil
}

func (p *poolRequests) release(_ context.Context, request domain.ReleaseRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.released = append(p.released, request)
	return nil
}

func (p *poolRequests) deleteSnapshots(_ context.Context, request domain.PoolSnapshotsRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deleted = append(p.deleted, request)
	return nil
}

func (p *poolRequests) register(env interface {
	RegisterActivityWithOptions(any, activity.RegisterOptions)
}) {
	env.RegisterActivityWithOptions(p.place, activity.RegisterOptions{Name: domain.ActivityPlaceSession})
	env.RegisterActivityWithOptions(p.release, activity.RegisterOptions{Name: domain.ActivityReleaseSession})
	env.RegisterActivityWithOptions(p.deleteSnapshots, activity.RegisterOptions{Name: domain.ActivityDeletePoolSnapshots})
}

// A parent chat and the two children it spawns run their three sessions in
// one pool: the name comes from the parent's authority, and each session asks
// that pool for its place, with the sizes this package chooses.
func TestAParentAndItsChildrenShareAPool(t *testing.T) {
	env, fake := newEnvironment(t)
	pools := &poolRequests{}
	pools.register(env)
	var started []chat.Request
	env.SetOnChildWorkflowStartedListener(func(_ *workflow.Info, _ workflow.Context, args converter.EncodedValues) {
		var state chat.State
		if err := args.Get(&state); err != nil {
			t.Error(err)
		}
		started = append(started, state.Request)
	})
	fake.onSlow = func() {
		fake.mu.Lock()
		session := fake.opened[0].ID
		fake.mu.Unlock()
		count := 0
		for _, task := range []string{"first child", "second child"} {
			toolCall(t, env, session, "chat_spawn", map[string]any{"task": task, "title": task}, func(result p7.ToolResult) {
				if result.IsError {
					t.Errorf("spawn: %+v", result)
					return
				}
				count++
				if count == 2 {
					toolCall(t, env, session, "chat_await", map[string]any{}, func(p7.ToolResult) { env.SignalWorkflow(chat.SignalClose, nil) })
				}
			})
		}
	}
	state := newChat("slow")
	state.Request.Backend = "kubernetes"
	state.Request.PoolScope = tokenScope
	env.ExecuteWorkflow(chat.WorkflowName, state)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}

	want := chat.PoolKey(state.Request, tokenScope)
	if want == "" || len(pools.placed) != 3 || len(pools.released) != 3 {
		t.Fatalf("%d placements and %d releases for three sessions in pool %q, want 3 and 3", len(pools.placed), len(pools.released), want)
	}
	sessions := map[string]bool{}
	for _, request := range pools.placed {
		sessions[request.Session] = true
		if request.Pool.Name != want || request.Pool.MaxWorkers != chat.PoolMaxWorkers || request.Pool.SessionsPerWorker != chat.PoolSessionsPerWorker || request.Pool.IdleTimeout != chat.PoolIdleTimeout {
			t.Errorf("the placement %+v is not in pool %q with the chosen sizes", request, want)
		}
		if err := request.Pool.Validate(); err != nil {
			t.Errorf("protocol7 refuses the pool: %v", err)
		}
	}
	if len(sessions) != 3 {
		t.Errorf("the three sessions have %d IDs", len(sessions))
	}
	for _, request := range pools.released {
		if request.Pool.Name != want {
			t.Errorf("the release %+v is not in pool %q", request, want)
		}
	}
	if len(started) != 2 {
		t.Fatalf("%d children started, want 2", len(started))
	}
	for _, child := range started {
		// The child's pool is named from its own request. It is the parent's,
		// as the three placements above show, because it holds the same
		// permissions.
		if child.Parent != state.Request.ID || child.PoolScope != tokenScope || chat.PoolKey(child, tokenScope) != want {
			t.Errorf("a child's request %+v does not name pool %q", child, want)
		}
	}
}

// Without a scope, on the host backend, and for a chat that was started
// before pools, there is no pool: each session has a pod of its own.
func TestAChatWithoutPoolsGetsAPodOfItsOwn(t *testing.T) {
	for name, edit := range map[string]func(*chat.Request){
		"no scope":     func(r *chat.Request) { r.PoolScope = "" },
		"host backend": func(r *chat.Request) { r.Backend = "host" },
	} {
		t.Run(name, func(t *testing.T) {
			env, fake := newEnvironment(t)
			pools := &poolRequests{}
			pools.register(env)
			env.RegisterActivityWithOptions(func(context.Context, domain.PodRequest) (domain.PodHandle, error) {
				return domain.PodHandle{Name: "p7-session-x", TaskQueue: "p7-session-x"}, nil
			}, activity.RegisterOptions{Name: domain.ActivityStartSessionPod})
			env.RegisterActivityWithOptions(func(context.Context, domain.PodRequest) error { return nil }, activity.RegisterOptions{Name: domain.ActivityDeleteSessionPod})
			state := newChat("first")
			state.Request.Backend, state.Request.PoolScope = "kubernetes", tokenScope
			edit(&state.Request)
			env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, 1)
			env.ExecuteWorkflow(chat.WorkflowName, state)
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			if len(fake.opened) != 1 || len(pools.placed) != 0 {
				t.Fatalf("opened %d sessions and placed %d in pools, want 1 and 0", len(fake.opened), len(pools.placed))
			}
		})
	}
}

// A chat that opens its session again takes the pool that its permissions name
// today: a pool from an earlier opening is not kept when the deployment turned
// pools off, or changed the token's scope.
func TestAStalePoolIsNotKept(t *testing.T) {
	for name, edit := range map[string]func(*chat.Request){
		"pools turned off": func(r *chat.Request) { r.PoolScope = "" },
		"scope changed":    func(r *chat.Request) { r.PoolScope = tokenScope + ",more" },
	} {
		t.Run(name, func(t *testing.T) {
			env, _ := newEnvironment(t)
			pools := &poolRequests{}
			pools.register(env)
			env.RegisterActivityWithOptions(func(context.Context, domain.PodRequest) (domain.PodHandle, error) {
				return domain.PodHandle{Name: "p7-session-x", TaskQueue: "p7-session-x"}, nil
			}, activity.RegisterOptions{Name: domain.ActivityStartSessionPod})
			env.RegisterActivityWithOptions(func(context.Context, domain.PodRequest) error { return nil }, activity.RegisterOptions{Name: domain.ActivityDeleteSessionPod})
			state := newChat("first")
			state.Request.Backend, state.Request.PoolScope = "kubernetes", tokenScope
			state.Request.Pool = chat.PoolKey(state.Request, tokenScope)
			stale := state.Request.Pool
			edit(&state.Request)
			env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, 1)
			env.ExecuteWorkflow(chat.WorkflowName, state)
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			for _, request := range pools.placed {
				if request.Pool.Name == stale {
					t.Errorf("the session was placed in the stale pool %s", stale)
				}
			}
			if name == "pools turned off" && len(pools.placed) != 0 {
				t.Errorf("placed %d sessions in pools with pools off", len(pools.placed))
			}
			if name == "scope changed" && (len(pools.placed) != 1 || pools.placed[0].Pool.Name != chat.PoolKey(state.Request, state.Request.PoolScope)) {
				t.Errorf("placements %+v, want one in the pool of the new scope", pools.placed)
			}
		})
	}
}

// The pool and its scope are in the chat's request, which Continue-As-New and
// the page's view carry as JSON: they survive a round trip, and a request from
// before pools, which has neither, encodes without them.
func TestThePoolIsPartOfTheRequestState(t *testing.T) {
	request := chat.Request{ID: "x", Backend: "kubernetes", PoolScope: tokenScope, Pool: "0123456789abcdef01234567"}
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var back chat.Request
	if err := json.Unmarshal(raw, &back); err != nil || back.Pool != request.Pool || back.PoolScope != tokenScope {
		t.Errorf("the request came back as %+v (%v) from %s", back, err, raw)
	}
	raw, err = json.Marshal(chat.Request{ID: "x"})
	if err != nil || strings.Contains(string(raw), "pool") {
		t.Errorf("a request with no pool encodes as %s, %v", raw, err)
	}
}
