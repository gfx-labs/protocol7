package kubernetes_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/session"
	"github.com/gfx-labs/protocol7/pkg/execution/snapshot"
	"github.com/gfx-labs/protocol7/pkg/execution/snapshot/snapshottest"
	orchestration "github.com/gfx-labs/protocol7/pkg/orchestration/activity"
	"github.com/gfx-labs/protocol7/pkg/orchestration/kubernetes"
)

// fakeAPI keeps pods in memory and answers the two calls reconciliation
// makes: list by label, and delete with a UID precondition.
type fakeAPI struct {
	mu      sync.Mutex
	pods    map[string]map[string]any
	deleted []string
}

func (f *fakeAPI) add(name string, labels map[string]string, annotations map[string]string, created time.Time, ready *bool, readySince time.Time) {
	metadata := map[string]any{
		"name": name, "uid": "uid-" + name, "labels": labels, "annotations": annotations,
		"creationTimestamp": created.UTC().Format(time.RFC3339),
	}
	status := map[string]any{}
	if ready != nil {
		condition := "False"
		if *ready {
			condition = "True"
		}
		status["conditions"] = []any{map[string]any{"type": "Ready", "status": condition, "lastTransitionTime": readySince.UTC().Format(time.RFC3339)}}
	}
	f.pods[name] = map[string]any{"metadata": metadata, "status": status}
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := "/api/v1/namespaces/p7/pods"
	switch {
	case r.Method == http.MethodGet && r.URL.Path == prefix:
		selector := r.URL.Query().Get("labelSelector")
		key, value, _ := strings.Cut(selector, "=")
		var items []any
		for _, pod := range f.pods {
			if pod["metadata"].(map[string]any)["labels"].(map[string]string)[key] == value {
				items = append(items, pod)
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, prefix+"/"):
		pod, ok := f.pods[strings.TrimPrefix(r.URL.Path, prefix+"/")]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"not found"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(pod)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, prefix+"/"):
		name := strings.TrimPrefix(r.URL.Path, prefix+"/")
		pod, ok := f.pods[name]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"not found"}`))
			return
		}
		var options struct {
			Preconditions struct {
				UID string `json:"uid"`
			} `json:"preconditions"`
		}
		_ = json.NewDecoder(r.Body).Decode(&options)
		if uid := options.Preconditions.UID; uid != "" && uid != pod["metadata"].(map[string]any)["uid"] {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"message":"uid"}`))
			return
		}
		delete(f.pods, name)
		f.deleted = append(f.deleted, name)
		_, _ = w.Write([]byte(`{}`))
	default:
		http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusNotImplemented)
	}
}

func (f *fakeAPI) names() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var names []string
	for name := range f.pods {
		names = append(names, name)
	}
	return names
}

// waitingOwner runs until it is signalled, failingOwner fails at once.
func waitingOwner(ctx workflow.Context) error {
	workflow.GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	return nil
}

func failingOwner(workflow.Context) error { return fmt.Errorf("the owner failed") }

// logLines collects what the reconciler logs.
type logLines struct {
	mu    sync.Mutex
	lines bytes.Buffer
}

func (l *logLines) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lines.Write(p)
}

func (l *logLines) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lines.String()
}

// Reconciliation deletes the pod of a session whose owner workflow failed,
// was terminated or does not exist, whose session is closed, or that is not
// ready after its open timeout, and keeps every other pod. It runs against a
// Temporal development server, with the worker's real session activities
// answering for the pods' sessions.
func TestReconcileDeletesSessionPodsThatNothingOwns(t *testing.T) {
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
	temporalClient := server.Client()

	// The kubernetes worker, with the probe workflow.
	dir := t.TempDir()
	token, template := filepath.Join(dir, "token"), filepath.Join(dir, "pod.json")
	if err := os.WriteFile(token, []byte("tok"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(template, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	api := &fakeAPI{pods: map[string]map[string]any{}}
	apiServer := httptest.NewServer(api)
	t.Cleanup(apiServer.Close)
	logs := &logLines{}
	// Snapshots of some sessions, in a fake store.
	s3 := snapshottest.Start(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	snapshots := snapshot.Config{Endpoint: s3.URL, Bucket: snapshottest.Bucket, PathStyle: true}
	store, err := snapshot.New(snapshots, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"keeps", "failed", "closed", "stuck"} {
		tree := filepath.Join(t.TempDir(), id)
		if err := os.MkdirAll(tree, 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Save(ctx, id, tree); err != nil {
			t.Fatal(err)
		}
	}
	pods, err := kubernetes.New(kubernetes.Config{
		Namespace: "p7", TemplateFile: template, SessionContainer: "session", APIServer: apiServer.URL, TokenFile: token,
		ReconcileInterval: time.Hour, OpenTimeout: 15 * time.Minute, Snapshots: snapshots,
	}, slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	backend := kubernetes.NewWorker(temporalClient, pods)
	if err := backend.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(backend.Stop)

	// Owners: running, failed and terminated workflows.
	owners := worker.New(temporalClient, "owners", worker.Options{})
	owners.RegisterWorkflowWithOptions(waitingOwner, workflow.RegisterOptions{Name: "waiting"})
	owners.RegisterWorkflowWithOptions(failingOwner, workflow.RegisterOptions{Name: "failing"})
	if err := owners.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(owners.Stop)
	start := func(id, kind string) client.WorkflowRun {
		run, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: "owners"}, kind)
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	start("owner-running", "waiting")
	if err := start("owner-failed", "failing").Get(ctx, nil); err == nil {
		t.Fatal("the failing owner did not fail")
	}
	start("owner-terminated", "waiting")
	if err := temporalClient.TerminateWorkflow(ctx, "owner-terminated", "", "test"); err != nil {
		t.Fatal(err)
	}

	// The sessions of the pods, on their own queues as in a pod. The
	// record is the manager's own file.
	root := t.TempDir()
	manager, err := session.NewManager(session.Config{Root: root, ShimBinary: "unused", Harness: session.Harness{Command: "unused"}})
	if err != nil {
		t.Fatal(err)
	}
	sessions := orchestration.NewSessions(manager)
	record := func(id string, closed bool) {
		directory := filepath.Join(root, "sessions", id)
		if err := os.MkdirAll(directory, 0o755); err != nil {
			t.Fatal(err)
		}
		content, _ := json.Marshal(map[string]any{"id": id, "closed": closed})
		if err := os.WriteFile(filepath.Join(directory, "session.json"), content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"keeps", "closed"} {
		record(id, id == "closed")
		pod := worker.New(temporalClient, domain.SessionQueue(id), worker.Options{})
		sessions.Register(pod)
		if err := pod.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pod.Stop)
	}
	// "unopened" has a pod whose worker never opened its session.
	pod := worker.New(temporalClient, domain.SessionQueue("unopened"), worker.Options{})
	sessions.Register(pod)
	if err := pod.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pod.Stop)

	now := time.Now()
	yes, no := true, false
	managed := func(session string) map[string]string {
		return map[string]string{kubernetes.LabelManaged: "true", kubernetes.LabelSession: session}
	}
	owned := func(owner string) map[string]string { return map[string]string{kubernetes.AnnotationOwner: owner} }
	// Pods that stay.
	api.add("p7-session-keeps", managed("keeps"), owned("owner-running"), now.Add(-time.Hour), &yes, now.Add(-time.Hour))
	api.add("p7-session-unopened", managed("unopened"), owned("owner-running"), now.Add(-time.Hour), &yes, now.Add(-time.Hour))
	api.add("p7-session-starting", managed("starting"), owned("owner-running"), now.Add(-time.Minute), &no, now.Add(-time.Minute))
	api.add("p7-session-nostatus", managed("nostatus"), owned("owner-running"), now.Add(-time.Minute), nil, time.Time{})
	api.add("p7-session-slowok", managed("slowok"), map[string]string{
		kubernetes.AnnotationOwner: "owner-running", kubernetes.AnnotationOpenTimeout: "1h",
	}, now.Add(-30*time.Minute), &no, now.Add(-30*time.Minute))
	api.add("not-ours", map[string]string{kubernetes.LabelSession: "x"}, owned("owner-failed"), now.Add(-time.Hour), &no, now.Add(-time.Hour))
	// Pods that go.
	api.add("p7-session-failed", managed("failed"), owned("owner-failed"), now.Add(-time.Minute), &yes, now)
	api.add("p7-session-terminated", managed("terminated"), owned("owner-terminated"), now.Add(-time.Minute), &yes, now)
	api.add("p7-session-nobody", managed("nobody"), owned("owner-never-started"), now.Add(-time.Minute), &yes, now)
	api.add("p7-session-closed", managed("closed"), owned("owner-running"), now.Add(-time.Hour), &yes, now.Add(-time.Hour))
	api.add("p7-session-stuck", managed("stuck"), owned("owner-running"), now.Add(-20*time.Minute), &no, now.Add(-20*time.Minute))
	api.add("p7-session-stuckfor", managed("stuckfor"), map[string]string{
		kubernetes.AnnotationOwner: "owner-running", kubernetes.AnnotationOpenTimeout: "2m",
	}, now.Add(-time.Hour), &no, now.Add(-5*time.Minute))

	reconciler := kubernetes.NewReconciler(pods, temporalClient)
	reconcileCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for pass := range 2 {
		if err := reconciler.Reconcile(reconcileCtx); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
	}

	want := map[string]bool{
		"p7-session-keeps": true, "p7-session-unopened": true, "p7-session-starting": true, "p7-session-nostatus": true,
		"p7-session-slowok": true, "not-ours": true,
	}
	got := map[string]bool{}
	for _, name := range api.names() {
		got[name] = true
	}
	for name := range want {
		if !got[name] {
			t.Errorf("pod %s was deleted, want it kept", name)
		}
	}
	for name := range got {
		if !want[name] {
			t.Errorf("pod %s was kept, want it deleted", name)
		}
	}
	// Each deletion is logged once, with its session and its reason.
	logged := logs.String()
	for session, reason := range map[string]string{
		"failed": kubernetes.ReasonOwnerGone, "terminated": kubernetes.ReasonOwnerGone, "nobody": kubernetes.ReasonOwnerGone,
		"closed": kubernetes.ReasonSessionClose, "stuck": kubernetes.ReasonNotReady, "stuckfor": kubernetes.ReasonNotReady,
	} {
		line := fmt.Sprintf("session=%s pod=p7-session-%s reason=%q", session, session, reason)
		if strings.Count(logged, line) != 1 {
			t.Errorf("the log has %d lines with %s, want one:\n%s", strings.Count(logged, line), line, logged)
		}
	}
	if len(api.deleted) != 6 {
		t.Errorf("deleted %v, want six pods", api.deleted)
	}
	// The snapshots of sessions that are abandoned or closed went with
	// their pods. One of a pod that was only slow to start stays, with that
	// of a pod that stays.
	for id, wantKept := range map[string]bool{"keeps": true, "failed": false, "closed": false, "stuck": true} {
		found, err := store.Restore(ctx, id, filepath.Join(t.TempDir(), "x"), nil)
		if err != nil || found != wantKept {
			t.Errorf("the snapshot of %s: found %v, %v, want %v", id, found, err, wantKept)
		}
	}
}

// The reconciler's loop reconciles when it starts and again on each tick,
// and stops.
func TestReconcilerRunsAtStartAndOnEachInterval(t *testing.T) {
	temporalCLI, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("the temporal CLI is not on PATH")
	}
	server, err := testsuite.StartDevServer(t.Context(), testsuite.DevServerOptions{
		ExistingPath: temporalCLI, LogLevel: "error", Stdout: os.Stderr, Stderr: os.Stderr,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop() })
	dir := t.TempDir()
	token, template := filepath.Join(dir, "token"), filepath.Join(dir, "pod.json")
	for file, content := range map[string]string{token: "tok", template: "{}"} {
		if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	api := &fakeAPI{pods: map[string]map[string]any{}}
	apiServer := httptest.NewServer(api)
	t.Cleanup(apiServer.Close)
	pods, err := kubernetes.New(kubernetes.Config{
		Namespace: "p7", TemplateFile: template, SessionContainer: "session", APIServer: apiServer.URL, TokenFile: token,
		ReconcileInterval: 100 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	orphan := func(session string) {
		api.mu.Lock()
		defer api.mu.Unlock()
		api.add("p7-session-"+session, map[string]string{kubernetes.LabelManaged: "true", kubernetes.LabelSession: session},
			map[string]string{kubernetes.AnnotationOwner: "nobody"}, time.Now(), nil, time.Time{})
	}
	orphan("before")
	reconciler := kubernetes.NewReconciler(pods, server.Client())
	reconciler.Start()
	reconciler.Start() // a second start does nothing
	defer reconciler.Stop()
	gone := func(name string) {
		t.Helper()
		for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
			if len(api.names()) == 0 {
				return
			}
		}
		t.Fatalf("pod %s was not deleted", name)
	}
	gone("before")
	orphan("after")
	gone("after")
	reconciler.Stop()
	orphan("stopped")
	time.Sleep(400 * time.Millisecond)
	if len(api.names()) != 1 {
		t.Errorf("a pod was deleted after Stop: %v", api.names())
	}
}
