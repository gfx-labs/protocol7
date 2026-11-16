package kubernetes_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/snapshot"
	"github.com/gfx-labs/protocol7/pkg/execution/snapshot/snapshottest"
	"github.com/gfx-labs/protocol7/pkg/orchestration/kubernetes"
)

const testGroup = "0123456789abcdef"

// groupAPI is the part of the pod API that group pods use, with the
// optimistic concurrency that they rely on: every write bumps the pod's
// resourceVersion, and a patch or a delete that names another one is a 409.
type groupAPI struct {
	mu      sync.Mutex
	pods    map[string]map[string]any
	version int
	// terminating are the pods that a delete marks and does not remove, so
	// that a test ends them when it chooses.
	terminating bool
	created     int
	// beforePatch runs once before the next patch is judged, to race it.
	beforePatch func()
	patches     int
}

func newGroupAPI() *groupAPI { return &groupAPI{pods: map[string]map[string]any{}} }

func (g *groupAPI) bump(pod map[string]any) {
	g.version++
	pod["metadata"].(map[string]any)["resourceVersion"] = strconv.Itoa(g.version)
}

func (g *groupAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	defer g.mu.Unlock()
	prefix := "/api/v1/namespaces/p7/pods"
	fail := func(code int, message string) {
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]any{"message": message})
	}
	name := strings.TrimPrefix(r.URL.Path, prefix+"/")
	body, _ := io.ReadAll(r.Body)
	switch {
	case r.Method == http.MethodPost && r.URL.Path == prefix:
		var pod map[string]any
		_ = json.Unmarshal(body, &pod)
		name := pod["metadata"].(map[string]any)["name"].(string)
		if _, exists := g.pods[name]; exists {
			fail(http.StatusConflict, "exists")
			return
		}
		pod["metadata"].(map[string]any)["uid"] = "uid-" + name + "-" + strconv.Itoa(g.created)
		g.created++
		g.bump(pod)
		g.pods[name] = pod
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, prefix+"/"):
		pod, ok := g.pods[name]
		if !ok {
			fail(http.StatusNotFound, "not found")
			return
		}
		_ = json.NewEncoder(w).Encode(pod)
	case r.Method == http.MethodPatch && strings.HasPrefix(r.URL.Path, prefix+"/"):
		if r.Header.Get("Content-Type") != "application/merge-patch+json" {
			fail(http.StatusUnsupportedMediaType, "content type "+r.Header.Get("Content-Type"))
			return
		}
		if hook := g.beforePatch; hook != nil {
			g.beforePatch = nil
			g.mu.Unlock()
			hook()
			g.mu.Lock()
		}
		g.patches++
		pod, ok := g.pods[name]
		if !ok {
			fail(http.StatusNotFound, "not found")
			return
		}
		var patch struct {
			Metadata struct {
				ResourceVersion string         `json:"resourceVersion"`
				Annotations     map[string]any `json:"annotations"`
			} `json:"metadata"`
		}
		_ = json.Unmarshal(body, &patch)
		metadata := pod["metadata"].(map[string]any)
		if patch.Metadata.ResourceVersion != metadata["resourceVersion"] {
			fail(http.StatusConflict, "resourceVersion")
			return
		}
		annotations, _ := metadata["annotations"].(map[string]any)
		if annotations == nil {
			annotations = map[string]any{}
			metadata["annotations"] = annotations
		}
		for key, value := range patch.Metadata.Annotations {
			if value == nil {
				delete(annotations, key)
			} else {
				annotations[key] = value
			}
		}
		g.bump(pod)
		_ = json.NewEncoder(w).Encode(pod)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, prefix+"/"):
		pod, ok := g.pods[name]
		if !ok {
			fail(http.StatusNotFound, "not found")
			return
		}
		var options struct {
			Preconditions struct {
				UID             string `json:"uid"`
				ResourceVersion string `json:"resourceVersion"`
			} `json:"preconditions"`
		}
		_ = json.Unmarshal(body, &options)
		metadata := pod["metadata"].(map[string]any)
		if (options.Preconditions.UID != "" && options.Preconditions.UID != metadata["uid"]) ||
			(options.Preconditions.ResourceVersion != "" && options.Preconditions.ResourceVersion != metadata["resourceVersion"]) {
			fail(http.StatusConflict, "precondition")
			return
		}
		if g.terminating {
			metadata["deletionTimestamp"] = time.Now().UTC().Format(time.RFC3339)
			g.bump(pod)
		} else {
			delete(g.pods, name)
		}
		_, _ = w.Write([]byte(`{}`))
	case r.Method == http.MethodGet && r.URL.Path == prefix:
		var items []any
		for _, pod := range g.pods {
			items = append(items, pod)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
	default:
		http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusNotImplemented)
	}
}

// pod returns a copy-free view of a pod, or nil.
func (g *groupAPI) pod(name string) map[string]any {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.pods[name]
}

// members are the sessions on a pod's member annotations.
func (g *groupAPI) members(name string) map[string]string {
	g.mu.Lock()
	defer g.mu.Unlock()
	members := map[string]string{}
	pod, ok := g.pods[name]
	if !ok {
		return nil
	}
	annotations, _ := pod["metadata"].(map[string]any)["annotations"].(map[string]any)
	for key, value := range annotations {
		if session, ok := strings.CutPrefix(key, kubernetes.AnnotationMemberPrefix); ok {
			members[session], _ = value.(string)
		}
	}
	return members
}

func (g *groupAPI) end(name string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.pods, name)
}

func newGroupPods(t *testing.T, api http.Handler, snapshots snapshot.Config) *kubernetes.Pods {
	t.Helper()
	server := httptest.NewServer(api)
	t.Cleanup(server.Close)
	dir := t.TempDir()
	token, template := filepath.Join(dir, "token"), filepath.Join(dir, "pod.json")
	if err := os.WriteFile(token, []byte("tok"), 0o600); err != nil {
		t.Fatal(err)
	}
	podTemplate := `{"metadata":{"labels":{"app":"p7-session","p7.gfx-labs.io/session":"stale"}},"spec":{"containers":[{"name":"forward"},
		{"name":"session","env":[{"name":"P7_SESSION","value":"stale"},{"name":"KEEP","value":"1"}]}]}}`
	if err := os.WriteFile(template, []byte(podTemplate), 0o600); err != nil {
		t.Fatal(err)
	}
	pods, err := kubernetes.New(kubernetes.Config{
		Namespace: "p7", TemplateFile: template, SessionContainer: "session", APIServer: server.URL, TokenFile: token,
		Snapshots: snapshots, OpenTimeout: 15 * time.Minute,
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	return pods
}

// The first session of a group makes its pod: named for the group, labelled
// with the group and not with a session, serving the group's queue, and
// holding the session as a member. The next sessions join it and make no
// pod. The pod goes with its last member, and a session that leaves while
// others stay does not take the pod down.
func TestSessionsOfAGroupShareOnePod(t *testing.T) {
	api := newGroupAPI()
	pods := newGroupPods(t, api, snapshot.Config{})
	ctx := context.Background()
	podName := domain.GroupPodName(testGroup)
	join := func(session, owner string) domain.PodHandle {
		handle, err := pods.Start(ctx, domain.PodRequest{Session: session, Group: testGroup, Owner: owner})
		if err != nil {
			t.Fatal(err)
		}
		return handle
	}

	parent := join("parent", "chat-parent")
	if parent.Name != podName || parent.TaskQueue != domain.GroupQueue(testGroup) {
		t.Fatalf("handle %+v", parent)
	}
	metadata := api.pod(podName)["metadata"].(map[string]any)
	labels := metadata["labels"].(map[string]any)
	if labels[kubernetes.LabelGroup] != testGroup || labels[kubernetes.LabelManaged] != "true" || labels["app"] != "p7-session" {
		t.Errorf("labels %v", labels)
	}
	if _, has := labels[kubernetes.LabelSession]; has {
		t.Errorf("a group pod carries the session label, which would give it a session's identity: %v", labels)
	}
	env := map[string]string{}
	for _, variable := range api.pod(podName)["spec"].(map[string]any)["containers"].([]any)[1].(map[string]any)["env"].([]any) {
		named := variable.(map[string]any)
		env[named["name"].(string)] = named["value"].(string)
	}
	if _, has := env[kubernetes.EnvSession]; has || env[kubernetes.EnvGroup] != testGroup || env[kubernetes.EnvTaskQueue] != domain.GroupQueue(testGroup) || env["KEEP"] != "1" {
		t.Errorf("env %v", env)
	}

	if first, second := join("child-a", "chat-a"), join("child-b", "chat-b"); first != parent || second != parent {
		t.Errorf("handles %+v %+v, want the pod of the parent %+v", first, second, parent)
	}
	join("child-b", "chat-b") // a repeated join is not an error and changes nothing
	if api.created != 1 {
		t.Errorf("%d pods were made for three sessions, want 1", api.created)
	}
	want := map[string]string{"parent": "chat-parent", "child-a": "chat-a", "child-b": "chat-b"}
	if got := api.members(podName); len(got) != 3 || got["parent"] != want["parent"] || got["child-a"] != want["child-a"] || got["child-b"] != want["child-b"] {
		t.Errorf("members %v, want %v", got, want)
	}

	leave := func(session string) {
		t.Helper()
		if err := pods.Delete(ctx, domain.PodRequest{Session: session, Group: testGroup}); err != nil {
			t.Fatal(err)
		}
	}
	leave("child-a")
	leave("child-a") // leaving twice is not an error
	leave("child-b")
	if api.pod(podName) == nil || len(api.members(podName)) != 1 {
		t.Fatalf("the pod must stay for the parent, members %v", api.members(podName))
	}
	leave("parent")
	if api.pod(podName) != nil {
		t.Errorf("the pod is still there after its last session left")
	}
	leave("parent") // the pod is gone
}

// Another group, and a session with no group, get pods of their own.
func TestAnotherGroupGetsAnotherPod(t *testing.T) {
	api := newGroupAPI()
	pods := newGroupPods(t, api, snapshot.Config{})
	ctx := context.Background()
	other := "fedcba9876543210"
	a, err := pods.Start(ctx, domain.PodRequest{Session: "a", Group: testGroup})
	if err != nil {
		t.Fatal(err)
	}
	b, err := pods.Start(ctx, domain.PodRequest{Session: "b", Group: other})
	if err != nil {
		t.Fatal(err)
	}
	c, err := pods.Start(ctx, domain.PodRequest{Session: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if a.Name == b.Name || a.TaskQueue == b.TaskQueue || c.Name != "p7-session-c" || c.TaskQueue != "p7-session-c" {
		t.Errorf("handles %+v %+v %+v", a, b, c)
	}
	if len(api.pods) != 3 {
		t.Errorf("pods %d, want 3", len(api.pods))
	}
	if _, err := pods.Start(ctx, domain.PodRequest{Session: "d", Group: "Not A Key"}); err == nil {
		t.Error("a malformed group key was accepted")
	}
	if _, err := pods.Start(ctx, domain.PodRequest{Session: "../x", Group: testGroup}); err == nil {
		t.Error("a session ID that is not a name was accepted")
	}
}

// A join that reads the pod and then loses a race with the leave of the last
// member must not add a member to a pod that is being deleted: the delete
// names the version it read, so one of the two fails and tries again.
func TestAJoinRacingTheLastLeaveIsNotLost(t *testing.T) {
	api := newGroupAPI()
	pods := newGroupPods(t, api, snapshot.Config{})
	ctx := context.Background()
	podName := domain.GroupPodName(testGroup)
	if _, err := pods.Start(ctx, domain.PodRequest{Session: "old", Group: testGroup}); err != nil {
		t.Fatal(err)
	}
	// Between the joiner's read and its patch, "old" leaves and the pod
	// goes. The joiner's patch is stale and must find the pod gone, and make
	// a new one.
	api.beforePatch = func() {
		if err := pods.Delete(ctx, domain.PodRequest{Session: "old", Group: testGroup}); err != nil {
			t.Error(err)
		}
	}
	if _, err := pods.Start(ctx, domain.PodRequest{Session: "new", Group: testGroup, Owner: "chat-new"}); err != nil {
		t.Fatal(err)
	}
	if got := api.members(podName); len(got) != 1 || got["new"] != "chat-new" {
		t.Errorf("members %v, want only new", got)
	}
	if api.created != 2 {
		t.Errorf("%d pods made, want a second for the session that joined as the first ended", api.created)
	}
}

// A join finds the pod of the group ending, waits for it to be gone, and
// makes the next. The leave that ended the pod waits for it as well.
func TestAJoinWaitsForAnEndingPod(t *testing.T) {
	api := newGroupAPI()
	api.terminating = true
	pods := newGroupPods(t, api, snapshot.Config{})
	ctx := context.Background()
	podName := domain.GroupPodName(testGroup)
	if _, err := pods.Start(ctx, domain.PodRequest{Session: "old", Group: testGroup}); err != nil {
		t.Fatal(err)
	}
	left, joined := make(chan error, 1), make(chan error, 1)
	go func() { left <- pods.Delete(ctx, domain.PodRequest{Session: "old", Group: testGroup}) }()
	waitFor(t, "the pod to be marked for deletion", 5*time.Second, func() bool {
		pod := api.pod(podName)
		api.mu.Lock()
		defer api.mu.Unlock()
		return pod != nil && pod["metadata"].(map[string]any)["deletionTimestamp"] != nil
	})
	go func() {
		_, err := pods.Start(ctx, domain.PodRequest{Session: "new", Group: testGroup, Owner: "chat-new"})
		joined <- err
	}()
	select {
	case err := <-joined:
		t.Fatalf("the join returned while the pod was ending: %v", err)
	case err := <-left:
		t.Fatalf("the leave returned while the pod was ending: %v", err)
	case <-time.After(1500 * time.Millisecond):
	}
	api.end(podName)
	for _, result := range []chan error{left, joined} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("no answer after the pod ended")
		}
	}
	if got := api.members(podName); len(got) != 1 || got["new"] != "chat-new" {
		t.Errorf("members %v, want only new in the next pod", got)
	}
}

// With snapshots a session's snapshot, under its group's prefix, is deleted
// when it leaves, and no other session's is.
func TestLeavingAGroupDeletesTheSessionsSnapshot(t *testing.T) {
	s3 := snapshottest.Start(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	config := snapshot.Config{Endpoint: s3.URL, Bucket: snapshottest.Bucket, Prefix: "sessions", PathStyle: true}
	grouped := config
	grouped.Prefix = domain.GroupSnapshotPrefix("sessions", testGroup)
	if grouped.Prefix != "sessions/groups/"+testGroup {
		t.Fatalf("prefix %q", grouped.Prefix)
	}
	store, err := snapshot.New(grouped, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		tree := filepath.Join(t.TempDir(), id)
		if err := os.MkdirAll(tree, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(tree, "file"), []byte(id), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Save(ctx, id, tree); err != nil {
			t.Fatal(err)
		}
	}
	api := newGroupAPI()
	pods := newGroupPods(t, api, config)
	for _, id := range []string{"a", "b"} {
		if _, err := pods.Start(ctx, domain.PodRequest{Session: id, Group: testGroup}); err != nil {
			t.Fatal(err)
		}
	}
	if err := pods.Delete(ctx, domain.PodRequest{Session: "a", Group: testGroup}); err != nil {
		t.Fatal(err)
	}
	restores := func(id string) bool {
		found, err := store.Restore(ctx, id, filepath.Join(t.TempDir(), "x"), nil)
		if err != nil {
			t.Fatal(err)
		}
		return found
	}
	if restores("a") {
		t.Error("the snapshot of the session that left is still there")
	}
	if !restores("b") {
		t.Error("the snapshot of the session that stayed was deleted")
	}
}

// fakeTemporal answers the two things reconciliation asks of Temporal.
type fakeTemporal struct {
	client.Client
	running map[string]bool
	closed  map[string]bool
}

func (f *fakeTemporal) DescribeWorkflowExecution(_ context.Context, id, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	running, known := f.running[id]
	if !known {
		return nil, &serviceerror.NotFound{}
	}
	status := enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	if running {
		status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: status}}, nil
}

type fakeRun struct {
	client.WorkflowRun
	state domain.SessionState
}

func (r fakeRun) Get(_ context.Context, valuePtr any) error {
	*(valuePtr.(*domain.SessionState)) = r.state
	return nil
}

func (f *fakeTemporal) ExecuteWorkflow(_ context.Context, _ client.StartWorkflowOptions, workflow any, args ...any) (client.WorkflowRun, error) {
	request := args[0].(kubernetes.ProbeGroupRequest)
	return fakeRun{state: domain.SessionState{Session: request.Session, Exists: true, Closed: f.closed[request.Session]}}, nil
}

// Reconciliation takes out the members whose owner is gone or whose session
// is closed, deletes the pod with the last of them, and deletes a group pod
// that has no member or never became ready. It leaves the others.
func TestReconcileRemovesOrphanedMembersOfAGroupPod(t *testing.T) {
	api := newGroupAPI()
	pods := newGroupPods(t, api, snapshot.Config{})
	ctx := context.Background()
	reconciler := kubernetes.NewReconciler(pods, &fakeTemporal{
		running: map[string]bool{"chat-live": true, "chat-done": false},
		closed:  map[string]bool{"closed": true},
	})
	ready := func(name string, since time.Time) {
		pod := api.pods[name]
		metadata := pod["metadata"].(map[string]any)
		metadata["creationTimestamp"] = since.UTC().Format(time.RFC3339)
		pod["status"] = map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True", "lastTransitionTime": since.UTC().Format(time.RFC3339)}}}
	}
	other := "fedcba9876543210"
	third := "aaaaaaaabbbbbbbb"
	for session, owner := range map[string]string{"live": "chat-live", "orphan": "chat-done", "closed": "chat-live", "lost": "chat-never-started"} {
		if _, err := pods.Start(ctx, domain.PodRequest{Session: session, Group: testGroup, Owner: owner}); err != nil {
			t.Fatal(err)
		}
	}
	// A group whose only session is orphaned, and one that is stuck.
	if _, err := pods.Start(ctx, domain.PodRequest{Session: "solo", Group: other, Owner: "chat-done"}); err != nil {
		t.Fatal(err)
	}
	if _, err := pods.Start(ctx, domain.PodRequest{Session: "stuck", Group: third, Owner: "chat-live"}); err != nil {
		t.Fatal(err)
	}
	ready(domain.GroupPodName(testGroup), time.Now().Add(-time.Hour))
	ready(domain.GroupPodName(other), time.Now().Add(-time.Hour))
	api.pods[domain.GroupPodName(third)]["metadata"].(map[string]any)["creationTimestamp"] = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)

	if err := reconciler.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if got := api.members(domain.GroupPodName(testGroup)); len(got) != 1 || got["live"] != "chat-live" {
		t.Errorf("members after reconciliation %v, want only live", got)
	}
	if api.pod(domain.GroupPodName(other)) != nil {
		t.Errorf("the pod whose last member was orphaned is still there")
	}
	if api.pod(domain.GroupPodName(third)) != nil {
		t.Errorf("the pod that never became ready is still there")
	}
}
