package kubernetes_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/orchestration/kubernetes"
)

// poolAPI is the part of the pod API that pools use: create, read, list by
// label and delete.
type poolAPI struct {
	mu      sync.Mutex
	pods    map[string]map[string]any
	created int
	// gate, when set, holds the creation of a pod until it is closed.
	gate atomic.Pointer[chan struct{}]
}

func (a *poolAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if gate := a.gate.Load(); gate != nil && r.Method == http.MethodPost {
		<-*gate
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	prefix := "/api/v1/namespaces/p7/pods"
	fail := func(code int) {
		w.WriteHeader(code)
		_, _ = w.Write([]byte(`{"message":"no"}`))
	}
	name := strings.TrimPrefix(r.URL.Path, prefix+"/")
	switch {
	case r.Method == http.MethodPost && r.URL.Path == prefix:
		var pod map[string]any
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &pod)
		metadata := pod["metadata"].(map[string]any)
		podName := metadata["name"].(string)
		if _, exists := a.pods[podName]; exists {
			fail(http.StatusConflict)
			return
		}
		a.created++
		metadata["uid"] = "uid-" + strconv.Itoa(a.created)
		metadata["creationTimestamp"] = time.Now().UTC().Format(time.RFC3339)
		a.pods[podName] = pod
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	case r.Method == http.MethodGet && r.URL.Path == prefix:
		key, value, _ := strings.Cut(r.URL.Query().Get("labelSelector"), "=")
		var items []any
		for _, pod := range a.pods {
			if pod["metadata"].(map[string]any)["labels"].(map[string]any)[key] == value {
				items = append(items, pod)
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, prefix+"/"):
		pod, ok := a.pods[name]
		if !ok {
			fail(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(pod)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, prefix+"/"):
		if _, ok := a.pods[name]; !ok {
			fail(http.StatusNotFound)
			return
		}
		delete(a.pods, name)
		_, _ = w.Write([]byte(`{}`))
	default:
		http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusNotImplemented)
	}
}

func (a *poolAPI) names() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var names []string
	for name := range a.pods {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func (a *poolAPI) pod(name string) map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.pods[name]
}

func (a *poolAPI) remove(name string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.pods, name)
}

const (
	poolA = "0123456789abcdef"
	poolB = "fedcba9876543210"
)

// poolRig is a Temporal development server, the backend's worker over a pod
// API stand-in, and a driver that asks the pool as a workflow that opens a
// session would.
type poolRig struct {
	t        *testing.T
	api      *poolAPI
	temporal client.Client
	closed   chan string
	mu       sync.Mutex
	closes   []string
}

func startPoolRig(t *testing.T) *poolRig {
	t.Helper()
	temporalCLI, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("the temporal CLI is not on PATH")
	}
	ctx := t.Context()
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: temporalCLI, LogLevel: "error", Stdout: os.Stderr, Stderr: os.Stderr,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop() })
	rig := &poolRig{t: t, api: &poolAPI{pods: map[string]map[string]any{}}, temporal: server.Client()}
	apiServer := httptest.NewServer(rig.api)
	t.Cleanup(apiServer.Close)
	dir := t.TempDir()
	token, template := filepath.Join(dir, "token"), filepath.Join(dir, "pod.json")
	if err := os.WriteFile(token, []byte("tok"), 0o600); err != nil {
		t.Fatal(err)
	}
	podTemplate := `{"metadata":{"labels":{"app":"p7-session","p7.gfx-labs.io/session":"stale"}},"spec":{"containers":[{"name":"forward"},
		{"name":"session","image":"img","resources":{"requests":{"cpu":"250m"}},"env":[{"name":"P7_SESSION","value":"stale"},{"name":"KEEP","value":"1"}]}]}}`
	if err := os.WriteFile(template, []byte(podTemplate), 0o600); err != nil {
		t.Fatal(err)
	}
	pods, err := kubernetes.New(kubernetes.Config{
		Namespace: "p7", TemplateFile: template, SessionContainer: "session", APIServer: apiServer.URL, TokenFile: token,
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	backend := kubernetes.NewWorker(rig.temporal, pods)
	if err := backend.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(backend.Stop)

	driver := worker.New(rig.temporal, "driver", worker.Options{})
	driver.RegisterWorkflowWithOptions(placeDriver, workflow.RegisterOptions{Name: "place"})
	driver.RegisterWorkflowWithOptions(releaseDriver, workflow.RegisterOptions{Name: "release"})
	driver.RegisterWorkflowWithOptions(waitingOwner, workflow.RegisterOptions{Name: "owner"})
	if err := driver.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(driver.Stop)
	return rig
}

// placeDriver does what a workflow that opens a session does: it asks the
// pool, with the retries and the long timeout of the real call.
func placeDriver(ctx workflow.Context, request domain.PlaceRequest) (domain.PoolPlacement, error) {
	var placement domain.PoolPlacement
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue: domain.KubernetesQueue, StartToCloseTimeout: time.Hour, HeartbeatTimeout: time.Minute,
	})
	err := workflow.ExecuteActivity(ctx, domain.ActivityPlaceSession, request).Get(ctx, &placement)
	return placement, err
}

func releaseDriver(ctx workflow.Context, request domain.ReleaseRequest) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{TaskQueue: domain.KubernetesQueue, StartToCloseTimeout: time.Minute})
	return workflow.ExecuteActivity(ctx, domain.ActivityReleaseSession, request).Get(ctx, nil)
}

// place starts a placement and returns its run.
func (r *poolRig) place(pool domain.PoolSpec, session, owner string) client.WorkflowRun {
	r.t.Helper()
	run, err := r.temporal.ExecuteWorkflow(r.t.Context(), client.StartWorkflowOptions{ID: "place-" + session + "-" + strconv.FormatInt(time.Now().UnixNano(), 36), TaskQueue: "driver"},
		"place", domain.PlaceRequest{Pool: pool, Session: session, Owner: owner, Attempt: "attempt-" + session + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)})
	if err != nil {
		r.t.Fatal(err)
	}
	return run
}

// placed places a session and waits for the answer.
func (r *poolRig) placed(pool domain.PoolSpec, session string) domain.PoolPlacement {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(r.t.Context(), 30*time.Second)
	defer cancel()
	var placement domain.PoolPlacement
	if err := r.place(pool, session, "").Get(ctx, &placement); err != nil {
		r.t.Fatalf("place %s: %v", session, err)
	}
	return placement
}

func (r *poolRig) release(pool domain.PoolSpec, session string) {
	r.t.Helper()
	run, err := r.temporal.ExecuteWorkflow(r.t.Context(), client.StartWorkflowOptions{ID: "release-" + session + "-" + strconv.FormatInt(time.Now().UnixNano(), 36), TaskQueue: "driver"},
		"release", domain.ReleaseRequest{Pool: pool, Session: session})
	if err != nil {
		r.t.Fatal(err)
	}
	if err := run.Get(r.t.Context(), nil); err != nil {
		r.t.Fatalf("release %s: %v", session, err)
	}
}

func (r *poolRig) state(pool string) domain.PoolState {
	r.t.Helper()
	value, err := r.temporal.QueryWorkflow(r.t.Context(), domain.PoolWorkflowID(pool), "", domain.QueryPoolState)
	if err != nil {
		r.t.Fatal(err)
	}
	var state domain.PoolState
	if err := value.Get(&state); err != nil {
		r.t.Fatal(err)
	}
	return state
}

func (r *poolRig) podCount(pool string) int {
	count := 0
	for _, name := range r.api.names() {
		if strings.HasPrefix(name, "p7-pool-"+pool+"-") {
			count++
		}
	}
	return count
}

// A pool of 3 workers with 3 sessions each runs 9 sessions in 3 pods, the 10th
// waits until a session is released, and the pod of a worker is the pool's:
// labelled with the pool and not a session, serving the queue of its slot.
func TestAPoolScalesUpPlacesAndQueues(t *testing.T) {
	rig := startPoolRig(t)
	spec := domain.PoolSpec{Name: poolA, MaxWorkers: 3, SessionsPerWorker: 3, IdleTimeout: time.Minute, Pod: domain.PodSpec{Image: "other", Limits: map[string]string{"memory": "2Gi"}}}
	perSlot := map[int][]string{}
	for index := 1; index <= 9; index++ {
		session := "s" + strconv.Itoa(index)
		placement := rig.placed(spec, session)
		perSlot[placement.Worker] = append(perSlot[placement.Worker], session)
		if placement.Pod != domain.PoolPodName(poolA, placement.Worker) || placement.TaskQueue != domain.PoolQueue(poolA, placement.Worker) {
			t.Errorf("placement %+v", placement)
		}
		// A session that asks again gets its place.
		if again := rig.placed(spec, session); again != placement {
			t.Errorf("session %s was placed on %+v, and then on %+v", session, placement, again)
		}
	}
	if len(perSlot) != 3 {
		t.Fatalf("9 sessions are on %d workers: %v", len(perSlot), perSlot)
	}
	for slot, sessions := range perSlot {
		if len(sessions) != 3 {
			t.Errorf("worker %d has %v, want 3 sessions", slot, sessions)
		}
	}
	if got := rig.api.names(); len(got) != 3 {
		t.Fatalf("pods %v, want 3", got)
	}

	pod := rig.api.pod(domain.PoolPodName(poolA, 1))
	metadata := pod["metadata"].(map[string]any)
	labels := metadata["labels"].(map[string]any)
	if labels[kubernetes.LabelPool] != poolA || labels[kubernetes.LabelWorker] != "1" || labels[kubernetes.LabelManaged] != "true" || labels["app"] != "p7-session" {
		t.Errorf("labels %v", labels)
	}
	if _, has := labels[kubernetes.LabelSession]; has {
		t.Errorf("a pool pod has the session label, which would give it a session's identity: %v", labels)
	}
	container := pod["spec"].(map[string]any)["containers"].([]any)[1].(map[string]any)
	env := map[string]string{}
	for _, variable := range container["env"].([]any) {
		named := variable.(map[string]any)
		env[named["name"].(string)] = named["value"].(string)
	}
	if _, has := env[kubernetes.EnvSession]; has || env[kubernetes.EnvPool] != poolA || env[kubernetes.EnvWorker] != "1" || env[kubernetes.EnvTaskQueue] != domain.PoolQueue(poolA, 1) || env["KEEP"] != "1" {
		t.Errorf("env %v", env)
	}
	resources := container["resources"].(map[string]any)
	if container["image"] != "other" || resources["requests"].(map[string]any)["cpu"] != "250m" || resources["limits"].(map[string]any)["memory"] != "2Gi" {
		t.Errorf("the pool's pod changes were not applied: %v", container)
	}

	// The 10th waits.
	waiting := rig.place(spec, "s10", "")
	select {
	case <-time.After(2 * time.Second):
	case <-func() chan struct{} {
		done := make(chan struct{})
		go func() { _ = waiting.Get(t.Context(), nil); close(done) }()
		return done
	}():
		t.Fatal("the 10th session was placed in a full pool")
	}
	if state := rig.state(poolA); len(state.Waiting) != 1 || state.Waiting[0] != "s10" || len(state.Workers) != 3 {
		t.Errorf("state %+v, want s10 waiting for 3 full workers", state)
	}
	if got := rig.api.names(); len(got) != 3 {
		t.Errorf("a pod was made for a session that waits: %v", got)
	}
	rig.release(spec, "s4")
	var placement domain.PoolPlacement
	if err := waiting.Get(t.Context(), &placement); err != nil {
		t.Fatal(err)
	}
	slot, _ := func() (int, bool) {
		for slot, sessions := range perSlot {
			if slices.Contains(sessions, "s4") {
				return slot, true
			}
		}
		return 0, false
	}()
	if placement.Worker != slot {
		t.Errorf("the 10th session was placed on worker %d, want worker %d, where s4 was", placement.Worker, slot)
	}
	if got := rig.api.names(); len(got) != 3 {
		t.Errorf("pods %v, want 3", got)
	}
}

// An empty worker is removed after the idle timeout, and the others stay.
// When the pool holds nothing its workflow ends, and the next session starts
// it again.
func TestAnEmptyWorkerIsRemovedAfterTheIdleTimeout(t *testing.T) {
	rig := startPoolRig(t)
	spec := domain.PoolSpec{Name: poolA, MaxWorkers: 2, SessionsPerWorker: 2, IdleTimeout: 2 * time.Second}
	slots := map[string]int{}
	for _, session := range []string{"a", "b", "c"} {
		slots[session] = rig.placed(spec, session).Worker
	}
	if slots["a"] != slots["b"] || slots["c"] == slots["a"] || rig.podCount(poolA) != 2 {
		t.Fatalf("slots %v, pods %v", slots, rig.api.names())
	}
	// Worker 2 becomes empty and goes after the timeout, not before.
	rig.release(spec, "c")
	time.Sleep(time.Second)
	if rig.podCount(poolA) != 2 {
		t.Errorf("the empty worker went before its idle timeout")
	}
	waitFor(t, "the empty worker to be removed", 20*time.Second, func() bool { return rig.podCount(poolA) == 1 })
	if got := rig.api.names(); got[0] != domain.PoolPodName(poolA, slots["a"]) {
		t.Errorf("the pod of the worker with sessions went: %v", got)
	}
	rig.release(spec, "a")
	rig.release(spec, "b")
	waitFor(t, "the last worker to be removed", 20*time.Second, func() bool { return rig.podCount(poolA) == 0 })
	waitFor(t, "the pool's workflow to end", 20*time.Second, func() bool {
		described, err := rig.temporal.DescribeWorkflowExecution(t.Context(), domain.PoolWorkflowID(poolA), "")
		return err == nil && described.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	})
	// A new session starts the pool again.
	if placement := rig.placed(spec, "d"); placement.Worker != 1 || rig.podCount(poolA) != 1 {
		t.Errorf("placement %+v, pods %v", placement, rig.api.names())
	}
}

// A worker whose pod was deleted is lost: its sessions are placed again when
// they ask, on a new worker, which is another pod of the pool.
func TestSessionsOfADeletedWorkerArePlacedAgain(t *testing.T) {
	restore := kubernetes.SetPoolTimings(0, time.Hour, time.Hour)
	defer restore()
	rig := startPoolRig(t)
	spec := domain.PoolSpec{Name: poolA, MaxWorkers: 2, SessionsPerWorker: 2, IdleTimeout: time.Minute}
	first, second := rig.placed(spec, "s1"), rig.placed(spec, "s2")
	third := rig.placed(spec, "s3")
	if first.Worker != second.Worker || third.Worker == first.Worker {
		t.Fatalf("placements %+v %+v %+v", first, second, third)
	}
	oldUID := rig.api.pod(first.Pod)["metadata"].(map[string]any)["uid"]
	rig.api.remove(first.Pod)

	// s1 asks again: its worker is gone, so it is placed anew, on the worker
	// that has room, and then s2 too.
	again := rig.placed(spec, "s1")
	if again.Worker != third.Worker {
		t.Errorf("s1 was placed on worker %d, want the worker %d that has room", again.Worker, third.Worker)
	}
	// Worker 2 is full again, so s2 gets a new worker in the free slot: the
	// pod of that slot is made anew.
	after := rig.placed(spec, "s2")
	if after.Worker != first.Worker {
		t.Errorf("s2 was placed on worker %d, want a new worker in slot %d", after.Worker, first.Worker)
	}
	if uid := rig.api.pod(first.Pod)["metadata"].(map[string]any)["uid"]; uid == oldUID {
		t.Errorf("the pod of slot %d is the one that was deleted", first.Worker)
	}
	if state := rig.state(poolA); len(state.Workers) != 2 {
		t.Errorf("state %+v, want 2 workers", state)
	}
}

// Pools with other names have workers of their own, with their own labels and
// queues.
func TestPoolsDoNotShareWorkers(t *testing.T) {
	rig := startPoolRig(t)
	a := domain.PoolSpec{Name: poolA, MaxWorkers: 2, SessionsPerWorker: 3, IdleTimeout: time.Minute}
	b := domain.PoolSpec{Name: poolB, MaxWorkers: 2, SessionsPerWorker: 3, IdleTimeout: time.Minute}
	x, y := rig.placed(a, "x"), rig.placed(b, "y")
	if x.Pod == y.Pod || x.TaskQueue == y.TaskQueue || rig.podCount(poolA) != 1 || rig.podCount(poolB) != 1 {
		t.Errorf("placements %+v %+v, pods %v", x, y, rig.api.names())
	}
	if rig.api.pod(y.Pod)["metadata"].(map[string]any)["labels"].(map[string]any)[kubernetes.LabelPool] != poolB {
		t.Errorf("the pod of pool b is not labelled with it")
	}
}

// A session whose owner workflow is not running is closed on its worker, and
// its place goes, so that its worker can go.
func TestASessionWhoseOwnerIsGoneIsReleased(t *testing.T) {
	restore := kubernetes.SetPoolTimings(time.Minute, time.Hour, time.Second)
	defer restore()
	// The callers ask before the pool's check closes the session of the owner
	// that is gone, as they would: a caller whose owner is gone does not ask.
	defer kubernetes.SetPoolWaiting(50*time.Millisecond, time.Minute)()
	rig := startPoolRig(t)
	spec := domain.PoolSpec{Name: poolA, MaxWorkers: 1, SessionsPerWorker: 2, IdleTimeout: time.Second}
	// The worker's queue answers the close the pool sends.
	closes := make(chan string, 4)
	fake := worker.New(rig.temporal, domain.PoolQueue(poolA, 1), worker.Options{})
	fake.RegisterActivityWithOptions(func(_ context.Context, request domain.CloseRequest) error {
		closes <- request.Session
		return nil
	}, activity.RegisterOptions{Name: domain.ActivityCloseSession})
	if err := fake.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fake.Stop)
	// "live" has an owner that runs, "gone" has one that never did.
	if _, err := rig.temporal.ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{ID: "owner-live", TaskQueue: "driver"}, "owner"); err != nil {
		t.Fatal(err)
	}
	for session, owner := range map[string]string{"live": "owner-live", "gone": "owner-never-started"} {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		if err := rig.place(spec, session, owner).Get(ctx, nil); err != nil {
			t.Fatal(err)
		}
		cancel()
	}
	select {
	case session := <-closes:
		if session != "gone" {
			t.Errorf("closed %s", session)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the session of the owner that is gone was not closed")
	}
	waitFor(t, "the session to be released", 10*time.Second, func() bool {
		state := rig.state(poolA)
		return len(state.Workers) == 1 && len(state.Workers[0].Sessions) == 1 && state.Workers[0].Sessions[0] == "live"
	})
}

// A spec that does not validate is refused for good, and a pool is asked
// only with its own name.
func TestAPoolSpecIsChecked(t *testing.T) {
	rig := startPoolRig(t)
	for name, spec := range map[string]domain.PoolSpec{
		"no workers":  {Name: poolA, MaxWorkers: 0, SessionsPerWorker: 1},
		"too many":    {Name: poolA, MaxWorkers: domain.MaxPoolWorkers + 1, SessionsPerWorker: 1},
		"no sessions": {Name: poolA, MaxWorkers: 1, SessionsPerWorker: 0},
		"bad name":    {Name: "Not A Pool", MaxWorkers: 1, SessionsPerWorker: 1},
	} {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		if err := rig.place(spec, "s", "").Get(ctx, nil); err == nil {
			t.Errorf("%s: accepted", name)
		}
		cancel()
	}
	if got := rig.api.names(); len(got) != 0 {
		t.Errorf("pods %v for specs that were refused", got)
	}
}

// A session that is released while the pool starts the worker it waits for is
// no longer waiting when the worker is there: the pool goes on, keeps the
// worker for its idle timeout, and places the next session on it.
func TestASessionReleasedWhileAWorkerStartsIsNotPlaced(t *testing.T) {
	// The caller that was released does not ask again, as one whose activity
	// was cancelled does not.
	defer kubernetes.SetPoolWaiting(time.Hour, time.Minute)()
	rig := startPoolRig(t)
	spec := domain.PoolSpec{Name: poolA, MaxWorkers: 1, SessionsPerWorker: 1, IdleTimeout: time.Hour}
	gate := make(chan struct{})
	rig.api.gate.Store(&gate)
	rig.place(spec, "a", "")
	waitFor(t, "the session to wait for its worker", 20*time.Second, func() bool {
		value, err := rig.temporal.QueryWorkflow(t.Context(), domain.PoolWorkflowID(poolA), "", domain.QueryPoolState)
		if err != nil {
			return false
		}
		var state domain.PoolState
		return value.Get(&state) == nil && len(state.Waiting) == 1
	})
	rig.release(spec, "a")
	close(gate)
	// The pool is not stuck: it holds the worker, empty, and places the next.
	waitFor(t, "the worker", 20*time.Second, func() bool { return rig.podCount(poolA) == 1 })
	// A workflow task that panics is retried only after seconds. The caller
	// that was released stays asleep: only new ones ask quickly.
	kubernetes.SetPoolAsk(200 * time.Millisecond)
	started := time.Now()
	if placement := rig.placed(spec, "b"); placement.Worker != 1 {
		t.Errorf("placement %+v", placement)
	}
	if took := time.Since(started); took > 4*time.Second {
		t.Errorf("the pool took %s to place the next session: its workflow task failed", took)
	}
	if state := rig.state(poolA); len(state.Waiting) != 0 || len(state.Workers) != 1 || len(state.Workers[0].Sessions) != 1 {
		t.Errorf("state %+v", state)
	}
}

// Many sessions may wait: each holds no update open (Temporal allows ten in a
// workflow), and they are placed in the order they asked.
func TestManySessionsWaitInOrder(t *testing.T) {
	defer kubernetes.SetPoolWaiting(300*time.Millisecond, time.Minute)()
	rig := startPoolRig(t)
	spec := domain.PoolSpec{Name: poolA, MaxWorkers: 1, SessionsPerWorker: 1, IdleTimeout: time.Minute}
	const waiters = 15
	rig.placed(spec, "first")
	runs := map[string]client.WorkflowRun{}
	var order []string
	for index := 1; index <= waiters; index++ {
		session := "w" + strconv.Itoa(index)
		runs[session] = rig.place(spec, session, "")
		order = append(order, session)
		want := index
		waitFor(t, session+" to be queued", 20*time.Second, func() bool {
			value, err := rig.temporal.QueryWorkflow(t.Context(), domain.PoolWorkflowID(poolA), "", domain.QueryPoolState)
			var state domain.PoolState
			return err == nil && value.Get(&state) == nil && len(state.Waiting) == want
		})
	}
	if state := rig.state(poolA); !slices.Equal(state.Waiting, order) {
		t.Fatalf("queue %v, want %v", state.Waiting, order)
	}
	// Each release places the next in line, and no other.
	previous := "first"
	for _, session := range order {
		rig.release(spec, previous)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		var placement domain.PoolPlacement
		if err := runs[session].Get(ctx, &placement); err != nil {
			cancel()
			t.Fatalf("%s was not placed after %s was released: %v", session, previous, err)
		}
		cancel()
		if placement.Worker != 1 {
			t.Errorf("%s: %+v", session, placement)
		}
		previous = session
	}
	if state := rig.state(poolA); len(state.Waiting) != 0 || len(state.Workers) != 1 || !slices.Equal(state.Workers[0].Sessions, []string{previous}) {
		t.Errorf("state %+v", state)
	}
}

// A caller that stops asking is dropped from the queue, and does not take the
// place that frees up.
func TestACallerThatStopsAskingLeavesTheQueue(t *testing.T) {
	defer kubernetes.SetPoolWaiting(200*time.Millisecond, 2*time.Second)()
	rig := startPoolRig(t)
	spec := domain.PoolSpec{Name: poolA, MaxWorkers: 1, SessionsPerWorker: 1, IdleTimeout: time.Minute}
	rig.placed(spec, "first")
	gone := rig.place(spec, "gone", "")
	waitFor(t, "gone to be queued", 20*time.Second, func() bool { return len(rig.state(poolA).Waiting) == 1 })
	// Its activity is cancelled with its workflow.
	if err := rig.temporal.TerminateWorkflow(t.Context(), gone.GetID(), gone.GetRunID(), "test"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "gone to leave the queue", 20*time.Second, func() bool { return len(rig.state(poolA).Waiting) == 0 })
	rig.release(spec, "first")
	if placement := rig.placed(spec, "next"); placement.Worker != 1 {
		t.Errorf("placement %+v", placement)
	}
	if state := rig.state(poolA); len(state.Workers[0].Sessions) != 1 || state.Workers[0].Sessions[0] != "next" {
		t.Errorf("state %+v", state)
	}
}
