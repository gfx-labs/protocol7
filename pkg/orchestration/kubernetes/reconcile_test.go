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
	// leases are the coordination.k8s.io leases by name, and version counts
	// their writes.
	leases  map[string]map[string]any
	version int
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
	leasePrefix := "/apis/coordination.k8s.io/v1/namespaces/p7/leases"
	switch {
	case strings.HasPrefix(r.URL.Path, leasePrefix):
		f.serveLease(w, r, strings.Trim(strings.TrimPrefix(r.URL.Path, leasePrefix), "/"))
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

// serveLease serves get, create and update of a lease the way the API server
// does: an update that does not carry the current resourceVersion is a 409.
func (f *fakeAPI) serveLease(w http.ResponseWriter, r *http.Request, name string) {
	if f.leases == nil {
		f.leases = map[string]map[string]any{}
	}
	fail := func(code int, message string) {
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]any{"message": message})
	}
	write := func(lease map[string]any) {
		_ = json.NewEncoder(w).Encode(lease)
	}
	store := func(lease map[string]any) {
		f.version++
		lease["metadata"].(map[string]any)["resourceVersion"] = fmt.Sprint(f.version)
		f.leases[lease["metadata"].(map[string]any)["name"].(string)] = lease
	}
	var lease map[string]any
	if r.Method == http.MethodPost || r.Method == http.MethodPut {
		if err := json.NewDecoder(r.Body).Decode(&lease); err != nil {
			fail(http.StatusBadRequest, err.Error())
			return
		}
		// The API server refuses a time that is not a MicroTime.
		spec, _ := lease["spec"].(map[string]any)
		for _, field := range []string{"acquireTime", "renewTime"} {
			if value, ok := spec[field].(string); ok {
				if _, err := time.Parse("2006-01-02T15:04:05.000000Z07:00", value); err != nil || len(value) != 27 {
					fail(http.StatusUnprocessableEntity, field+" is not a MicroTime: "+value)
					return
				}
			}
		}
	}
	existing, found := f.leases[name]
	switch {
	case r.Method == http.MethodGet && name != "":
		if !found {
			fail(http.StatusNotFound, "not found")
			return
		}
		write(existing)
	case r.Method == http.MethodPost && name == "":
		metadata, _ := lease["metadata"].(map[string]any)
		created, _ := metadata["name"].(string)
		if _, exists := f.leases[created]; exists {
			fail(http.StatusConflict, "exists")
			return
		}
		store(lease)
		write(lease)
	case r.Method == http.MethodPut && found:
		sent, _ := lease["metadata"].(map[string]any)["resourceVersion"].(string)
		if sent != existing["metadata"].(map[string]any)["resourceVersion"] {
			fail(http.StatusConflict, "the object has been modified")
			return
		}
		store(lease)
		write(lease)
	default:
		fail(http.StatusNotImplemented, "unexpected "+r.Method+" "+r.URL.Path)
	}
}

// holder is the identity that holds the lease, or "".
func (f *fakeAPI) holder(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	spec, _ := f.leases[name]["spec"].(map[string]any)
	holder, _ := spec["holderIdentity"].(string)
	return holder
}

// replica is one `p7 pods` replica: its own view of the API server, which
// counts the pod lists it makes and can be cut off from the API server.
type replica struct {
	pods       *kubernetes.Pods
	reconciler *kubernetes.Reconciler
	logs       *logLines
	mu         sync.Mutex
	lists      []time.Time
	cut        bool
}

func (r *replica) listed() []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Time(nil), r.lists...)
}

func (r *replica) partition() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cut = true
}

func newReplica(t *testing.T, api *fakeAPI, identity string, interval, lease time.Duration) *replica {
	t.Helper()
	dir := t.TempDir()
	token, template := filepath.Join(dir, "token"), filepath.Join(dir, "pod.json")
	for file, content := range map[string]string{token: "tok", template: "{}"} {
		if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	r := &replica{logs: &logLines{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		cut := r.cut
		if !cut && req.Method == http.MethodGet && req.URL.Path == "/api/v1/namespaces/p7/pods" {
			r.lists = append(r.lists, time.Now())
		}
		r.mu.Unlock()
		if cut {
			http.Error(w, "unreachable", http.StatusServiceUnavailable)
			return
		}
		api.ServeHTTP(w, req)
	}))
	t.Cleanup(server.Close)
	var err error
	r.pods, err = kubernetes.New(kubernetes.Config{
		Namespace: "p7", TemplateFile: template, SessionContainer: "session", APIServer: server.URL, TokenFile: token,
		ReconcileInterval: interval, LeaseDuration: lease, Identity: identity,
	}, slog.New(slog.NewTextHandler(r.logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	// With no pods, a pass makes no call to Temporal.
	r.reconciler = kubernetes.NewReconciler(r.pods, nil)
	t.Cleanup(r.reconciler.Stop)
	return r
}

func waitFor(t *testing.T, what string, within time.Duration, condition func() bool) time.Duration {
	t.Helper()
	start := time.Now()
	for time.Now().Before(start.Add(within)) {
		if condition() {
			return time.Since(start)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("after %v: %s", within, what)
	return 0
}

// Two reconcilers over one API server make one pass per interval between
// them, and the other takes over when the leader stops, whether it releases
// the lease or is cut off from the API server and cannot.
func TestTwoReconcilersMakeOnePassPerInterval(t *testing.T) {
	const interval, lease = 100 * time.Millisecond, time.Second
	api := &fakeAPI{pods: map[string]map[string]any{}}
	a := newReplica(t, api, "replica-a", interval, lease)
	b := newReplica(t, api, "replica-b", interval, lease)
	replicas := map[string]*replica{"replica-a": a, "replica-b": b}
	a.reconciler.Start()
	b.reconciler.Start()

	// One leads and makes the passes. The other makes none.
	waitFor(t, "no replica holds the lease", 5*time.Second, func() bool { return api.holder("p7-pods-reconciler") != "" })
	window := 1500 * time.Millisecond
	time.Sleep(window)
	leader := replicas[api.holder("p7-pods-reconciler")]
	follower := a
	if leader == a {
		follower = b
	}
	if got := len(follower.listed()); got != 0 {
		t.Errorf("the replica that does not hold the lease listed pods %d times", got)
	}
	// A pass at once and one for each interval: about window/interval.
	if got, most := len(leader.listed()), int(window/interval)+3; got < 5 || got > most {
		t.Errorf("the leader made %d passes in %v at an interval of %v, want 5 to %d", got, window, interval, most)
	}
	if strings.Count(a.logs.String()+b.logs.String(), "session pod reconciler elected") != 1 {
		t.Errorf("want one election:\na: %s\nb: %s", a.logs.String(), b.logs.String())
	}

	// The leader stops, which releases the lease, and the other takes over
	// within a retry period, long before the lease would expire.
	leader.reconciler.Stop()
	stopped, before := len(leader.listed()), len(follower.listed())
	took := waitFor(t, "the other did not take over", lease, func() bool { return len(follower.listed()) > before })
	t.Logf("took over %v after the leader stopped", took)
	if holder := api.holder("p7-pods-reconciler"); holder != map[*replica]string{a: "replica-a", b: "replica-b"}[follower] {
		t.Errorf("the lease is held by %q, want the other replica", holder)
	}
	if got := len(leader.listed()); got != stopped {
		t.Errorf("the stopped replica went on listing: %d lists, %d at its stop", got, stopped)
	}

	// A leader that cannot reach the API server cannot release the lease.
	// It stops leading by itself, and the other takes over once the lease
	// has expired.
	follower.partition()
	leader.reconciler.Start()
	waited := waitFor(t, "the restarted replica did not take over", 4*lease, func() bool { return len(leader.listed()) > stopped })
	if waited < lease/2 {
		t.Errorf("took the lease after %v, before it could have expired (%v)", waited, lease)
	}
	if !strings.Contains(follower.logs.String(), "stopped leading") {
		t.Errorf("the cut off leader did not stop leading:\n%s", follower.logs.String())
	}
	// It will not make another pass.
	count := len(follower.listed())
	time.Sleep(3 * interval)
	if len(follower.listed()) != count {
		t.Errorf("the cut off replica went on listing")
	}
}
