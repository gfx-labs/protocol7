package p7_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/acpshim"
	"github.com/gfx-labs/protocol7/pkg/execution/snapshot"
	"github.com/gfx-labs/protocol7/pkg/execution/snapshot/snapshottest"
	orchestration "github.com/gfx-labs/protocol7/pkg/orchestration/activity"
	"github.com/gfx-labs/protocol7/pkg/orchestration/coreworker"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

const testPool = "0123456789abcdef"

func testPoolSpec() p7.SessionPool {
	return p7.SessionPool{Name: testPool, MaxWorkers: 2, SessionsPerWorker: 2, IdleTimeout: time.Minute}
}

// fakePool stands in for the kubernetes backend's pool: placing a session
// starts a core worker on the queue of a slot, with a state directory of its
// own, as `p7 session --pool` does in a pod, and a worker that is lost takes
// its disk with it. The snapshots are under the pool's prefix.
type fakePool struct {
	t         *testing.T
	temporal  client.Client
	binary    string
	snapshots p7.SnapshotStore

	mu      sync.Mutex
	workers map[int]*fakeWorker
	// dead are the slots whose machine is gone: nothing runs there again.
	dead    map[int]bool
	placed  map[string]int
	history []string
	freed   []string
}

type fakeWorker struct {
	core *coreworker.Worker
	root string
}

func (f *fakePool) load(slot int) int {
	count := 0
	for _, placed := range f.placed {
		if placed == slot {
			count++
		}
	}
	return count
}

func (f *fakePool) place(_ context.Context, request domain.PlaceRequest) (domain.PoolPlacement, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if slot, ok := f.placed[request.Session]; ok && f.workers[slot] != nil {
		return domain.PoolPlacement{Worker: slot, Pod: domain.PoolPodName(testPool, slot), TaskQueue: domain.PoolQueue(testPool, slot)}, nil
	}
	delete(f.placed, request.Session)
	chosen := 0
	for slot := 1; slot <= request.Pool.MaxWorkers && chosen == 0; slot++ {
		if f.workers[slot] != nil && f.load(slot) < request.Pool.SessionsPerWorker {
			chosen = slot
		}
	}
	for slot := 1; slot <= request.Pool.MaxWorkers && chosen == 0; slot++ {
		if f.workers[slot] == nil && !f.dead[slot] {
			chosen = slot
			root, err := os.MkdirTemp("", "p7")
			if err != nil {
				return domain.PoolPlacement{}, err
			}
			manager, err := newMemoryManager(f.binary, root, f.temporal)
			if err != nil {
				return domain.PoolPlacement{}, err
			}
			config := f.snapshots
			config.Prefix = domain.PoolSnapshotPrefix(config.Prefix, testPool)
			core, err := coreworker.New(coreworker.Config{TaskQueue: domain.PoolQueue(testPool, slot), Snapshots: config},
				f.temporal, orchestration.NewSessions(manager), slog.New(slog.DiscardHandler))
			if err != nil {
				return domain.PoolPlacement{}, err
			}
			if err := core.Start(); err != nil {
				return domain.PoolPlacement{}, err
			}
			f.workers[slot] = &fakeWorker{core: core, root: root}
		}
	}
	if chosen == 0 {
		return domain.PoolPlacement{}, fmt.Errorf("the pool is full")
	}
	f.placed[request.Session] = chosen
	f.history = append(f.history, fmt.Sprintf("%s=%d", request.Session, chosen))
	return domain.PoolPlacement{Worker: chosen, Pod: domain.PoolPodName(testPool, chosen), TaskQueue: domain.PoolQueue(testPool, chosen)}, nil
}

func (f *fakePool) release(_ context.Context, request domain.ReleaseRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.placed, request.Session)
	f.freed = append(f.freed, request.Session)
	return nil
}

func (f *fakePool) deleteSnapshots(ctx context.Context, request domain.PoolSnapshotsRequest) error {
	config := f.snapshots
	config.Prefix = domain.PoolSnapshotPrefix(config.Prefix, request.Pool)
	store, err := snapshot.New(config, nil)
	if err != nil {
		return err
	}
	return store.Delete(ctx, request.Session)
}

// lose stops a worker and takes its disk, as a pod that was deleted.
func (f *fakePool) lose(slot int) {
	f.mu.Lock()
	worker := f.workers[slot]
	delete(f.workers, slot)
	f.dead[slot] = true
	f.mu.Unlock()
	worker.core.Stop()
	sessions, _ := os.ReadDir(filepath.Join(worker.root, "sessions"))
	for _, entry := range sessions {
		_ = acpshim.Stop(f.t.Context(), filepath.Join(worker.root, "sessions", entry.Name(), "shim"))
	}
	_ = os.RemoveAll(worker.root)
}

// poolMemoryWorkflow opens a session in the pool, writes a file with a turn,
// waits for the signal "go", asks what the agent remembers, and closes.
func poolMemoryWorkflow(ctx workflow.Context, content string) (string, error) {
	var turns int
	if err := workflow.SetQueryHandler(ctx, "turns", func() (int, error) { return turns, nil }); err != nil {
		return "", err
	}
	pool := testPoolSpec()
	session, err := p7.OpenSession(ctx, p7.SessionSpec{Placement: p7.Placement{Backend: p7.BackendKubernetes}, Pool: &pool})
	if err != nil {
		return "", err
	}
	if _, err := p7.PromptText(ctx, session, p7.TextPrompt{Text: "write note.txt " + content}); err != nil {
		return "", err
	}
	turns++
	workflow.GetSignalChannel(ctx, "go").Receive(ctx, nil)
	turn, err := p7.PromptText(ctx, session, p7.TextPrompt{Text: "recall"})
	if err != nil {
		return "", err
	}
	return turn.Text + " queue=" + session.TaskQueue, session.Close(ctx)
}

// Two sessions of a pool run on one worker. When the worker is lost, with its
// disk, each session's next turn asks the pool again, is placed on another
// worker, and finds its own files and conversation there, from its own
// snapshot, and not the other session's. Closing gives back the place and
// deletes the snapshot.
func TestSessionsOfAPoolAreRecoveredOnAnotherWorker(t *testing.T) {
	temporalCLI, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("the temporal CLI is not on PATH")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	defer p7.SetPodLostAfter(3 * time.Second)()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: temporalCLI, LogLevel: "error", Stdout: os.Stderr, Stderr: os.Stderr,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop() })
	t.Setenv("AWS_ACCESS_KEY_ID", "test-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	s3 := snapshottest.Start(t)
	temporalClient := server.Client()

	pool := &fakePool{
		t: t, temporal: temporalClient, binary: binary, workers: map[int]*fakeWorker{}, dead: map[int]bool{}, placed: map[string]int{},
		snapshots: p7.SnapshotStore{Endpoint: s3.URL, Bucket: snapshottest.Bucket, Prefix: "p7/sessions", PathStyle: true},
	}
	t.Cleanup(func() {
		for slot := range pool.workers {
			pool.workers[slot].core.Stop()
			_ = os.RemoveAll(pool.workers[slot].root)
		}
	})
	backend := worker.New(temporalClient, domain.KubernetesQueue, worker.Options{})
	backend.RegisterActivityWithOptions(pool.place, activity.RegisterOptions{Name: domain.ActivityPlaceSession})
	backend.RegisterActivityWithOptions(pool.release, activity.RegisterOptions{Name: domain.ActivityReleaseSession})
	backend.RegisterActivityWithOptions(pool.deleteSnapshots, activity.RegisterOptions{Name: domain.ActivityDeletePoolSnapshots})
	if err := backend.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(backend.Stop)
	consumer, err := p7.NewWorker(p7.WorkerConfig{TaskQueue: "consumer", TemporalAddress: server.FrontendHostPort()})
	if err != nil {
		t.Fatal(err)
	}
	consumer.RegisterWorkflow("pool-memory", poolMemoryWorkflow)
	if err := consumer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(consumer.Stop)

	runs := map[string]client.WorkflowRun{}
	for _, content := range []string{"alpha", "beta"} {
		runs[content], err = temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "pool-" + content, TaskQueue: "consumer"}, "pool-memory", content)
		if err != nil {
			t.Fatal(err)
		}
	}
	for content := range runs {
		for turns := 0; turns < 1; {
			response, err := temporalClient.QueryWorkflow(ctx, "pool-"+content, "", "turns")
			if err == nil {
				_ = response.Get(&turns)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	pool.mu.Lock()
	if len(pool.workers) != 1 || len(pool.placed) != 2 || len(pool.history) != 2 {
		t.Fatalf("workers %d, placed %v: two sessions of a pool of two per worker belong on one", len(pool.workers), pool.placed)
	}
	pool.mu.Unlock()

	pool.lose(1)
	for content := range runs {
		if err := temporalClient.SignalWorkflow(ctx, "pool-"+content, "", "go", nil); err != nil {
			t.Fatal(err)
		}
	}
	for content, run := range runs {
		var reply string
		if err := run.Get(ctx, &reply); err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"loaded=true", "history=write note.txt " + content + "|", "note.txt=" + content, "queue=" + domain.PoolQueue(testPool, 2)} {
			if !strings.Contains(reply, want) {
				t.Errorf("session %s: the agent said %q, want it to contain %q", content, reply, want)
			}
		}
		other := map[string]string{"alpha": "beta", "beta": "alpha"}[content]
		if strings.Contains(reply, "note.txt="+other) {
			t.Errorf("session %s found the file of session %s: %q", content, other, reply)
		}
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if len(pool.workers) != 1 || pool.workers[2] == nil || len(pool.freed) != 2 {
		t.Errorf("workers %v, released %v, want both sessions on slot 2 and released once closed", pool.workers, pool.freed)
	}
	// Closed sessions have no snapshot left.
	config := pool.snapshots
	config.Prefix = domain.PoolSnapshotPrefix(config.Prefix, testPool)
	store, err := snapshot.New(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, session := range []string{pool.history[0], pool.history[1]} {
		id, _, _ := strings.Cut(session, "=")
		if found, err := store.Restore(ctx, id, filepath.Join(t.TempDir(), "x"), nil); err != nil || found {
			t.Errorf("the snapshot of the closed session %s is still there: %v, %v", id, found, err)
		}
	}
}

// A pool needs the kubernetes backend and a spec that validates.
func TestAPoolNeedsTheKubernetesBackendAndASpec(t *testing.T) {
	suite := &testsuite.WorkflowTestSuite{}
	for name, spec := range map[string]p7.SessionSpec{
		"host":          {Placement: p7.Placement{Backend: p7.BackendHost, Host: "h"}, Pool: &p7.SessionPool{Name: testPool, MaxWorkers: 1, SessionsPerWorker: 1}},
		"no workers":    {Placement: p7.Placement{Backend: p7.BackendKubernetes}, Pool: &p7.SessionPool{Name: testPool, SessionsPerWorker: 1}},
		"a bad name":    {Placement: p7.Placement{Backend: p7.BackendKubernetes}, Pool: &p7.SessionPool{Name: "BAD", MaxWorkers: 1, SessionsPerWorker: 1}},
		"no per-worker": {Placement: p7.Placement{Backend: p7.BackendKubernetes}, Pool: &p7.SessionPool{Name: testPool, MaxWorkers: 1}},
	} {
		env := suite.NewTestWorkflowEnvironment()
		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			_, err := p7.OpenSession(ctx, spec)
			return err
		})
		if err := env.GetWorkflowError(); err == nil || !strings.Contains(err.Error(), "open session") {
			t.Errorf("%s: error %v", name, err)
		}
	}
}
