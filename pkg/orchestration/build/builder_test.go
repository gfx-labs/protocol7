package build_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/orchestration/build"
)

const (
	repository = "oku-trade/oku-review"
	pkg        = "oku-trade/oku-review"
)

type fakeSource struct{ tree map[string]string }

func (s fakeSource) Tree(context.Context, string, string) (map[string]string, error) {
	return s.tree, nil
}
func (s fakeSource) Checkout(_ context.Context, _, _, directory string) error {
	return os.WriteFile(filepath.Join(directory, "Dockerfile"), []byte("FROM scratch\n"), 0o600)
}

type fakeRegistry struct {
	mu   sync.Mutex
	tags map[string]string
}

func (r *fakeRegistry) Digest(_ context.Context, p, tag string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tags[p+":"+tag], nil
}

func (r *fakeRegistry) Tag(_ context.Context, p, digest, tag string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tags[p+":"+tag] = digest
	return nil
}

// fakeSolver pushes into the fake registry as buildkitd would push into zot.
type fakeSolver struct {
	registry *fakeRegistry
	fail     bool
	solves   int
}

func (s *fakeSolver) Solve(_ context.Context, job build.SolveJob, _ func(string)) (string, error) {
	s.solves++
	if s.fail {
		return "", &build.StepError{Log: "RUN go build: exit 1", Err: errors.New("process did not complete")}
	}
	if _, err := os.Stat(filepath.Join(job.Context, "Dockerfile")); err != nil {
		return "", err
	}
	digest := "sha256:" + strings.Repeat("b", 64)
	for _, ref := range job.Refs {
		host, rest, _ := strings.Cut(ref, "/")
		if host != "zot.registry.svc:5000" {
			return "", errors.New("ref not in zot: " + ref)
		}
		name, tag, _ := strings.Cut(rest, ":")
		s.registry.tags[name+":"+tag] = digest
	}
	return digest, nil
}

type harness struct {
	registry *fakeRegistry
	solver   *fakeSolver
}

func newHarness() *harness {
	registry := &fakeRegistry{tags: map[string]string{}}
	return &harness{registry: registry, solver: &fakeSolver{registry: registry}}
}

type envRegistrar struct {
	env *testsuite.TestWorkflowEnvironment
}

func (r envRegistrar) RegisterActivity(name string, function any) {
	r.env.RegisterActivityWithOptions(function, activity.RegisterOptions{Name: name})
}

// prMaker is the caller: it requests a build of its branch head and reports
// the error type it sees.
func prMaker(ctx workflow.Context, request build.Request) (build.Result, error) {
	result, err := build.Build(ctx, request, build.BuildOptions{Timeout: time.Hour})
	var applicationError *temporal.ApplicationError
	if errors.As(err, &applicationError) {
		return build.Result{}, temporal.NewNonRetryableApplicationError(err.Error(), applicationError.Type(), nil)
	}
	return result, err
}

func (h *harness) run(t *testing.T, request build.Request) (build.Result, error) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	builder := &build.Builder{Repository: repository, Images: []build.ImageSpec{{Name: "reviewer", Package: pkg, Dockerfile: "Dockerfile"}}}
	activities := &build.Activities{
		Repository: repository, Inputs: []string{"cmd", "Dockerfile"}, RegistryHost: "zot.registry.svc:5000",
		Packages: []string{pkg}, WorkDir: t.TempDir(),
		Source:   fakeSource{tree: map[string]string{"cmd/main.go": "aaa", "Dockerfile": "bbb", "README.md": "ccc"}},
		Registry: h.registry, Solver: h.solver,
	}
	env.RegisterWorkflow(prMaker)
	env.RegisterWorkflowWithOptions(builder.Workflow, workflow.RegisterOptions{Name: build.WorkflowName})
	activities.RegisterOn(envRegistrar{env})
	env.ExecuteWorkflow(prMaker, request)
	if !env.IsWorkflowCompleted() {
		t.Fatal("caller did not complete")
	}
	var result build.Result
	err := env.GetWorkflowError()
	if err == nil {
		if decodeErr := env.GetWorkflowResult(&result); decodeErr != nil {
			t.Fatal(decodeErr)
		}
	}
	return result, err
}

func pullRequest(commit string) build.Request {
	return build.Request{Repository: repository, Commit: commit, Trigger: build.TriggerPullRequest, PullRequest: 7}
}

func errorType(err error) string {
	var applicationError *temporal.ApplicationError
	if errors.As(err, &applicationError) {
		return applicationError.Type()
	}
	return ""
}

// A PR maker gets the pushed digest back. A second request with the same
// inputs is answered from zot without building.
func TestBuildPushesAndReuses(t *testing.T) {
	h := newHarness()
	commit := strings.Repeat("1", 40)
	result, err := h.run(t, pullRequest(commit))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Images) != 1 || result.Images[0].Digest != "sha256:"+strings.Repeat("b", 64) || result.Reused {
		t.Fatalf("first build %+v", result)
	}
	if got := result.Images[0].Tag; got != "pr-7-111111111111" {
		t.Fatalf("tag %q", got)
	}
	if h.registry.tags[pkg+":"+build.InputsTag(result.Inputs)] == "" {
		t.Fatal("no inputs tag pushed")
	}
	again, err := h.run(t, pullRequest(strings.Repeat("2", 40)))
	if err != nil {
		t.Fatal(err)
	}
	if !again.Reused || h.solver.solves != 1 || again.Images[0].Digest != result.Images[0].Digest {
		t.Fatalf("second build %+v after %d solves", again, h.solver.solves)
	}
	if h.registry.tags[pkg+":pr-7-222222222222"] == "" {
		t.Fatal("reused images did not get the request's tag")
	}
}

func TestBuildFailedReachesCaller(t *testing.T) {
	h := newHarness()
	h.solver.fail = true
	_, err := h.run(t, pullRequest(strings.Repeat("3", 40)))
	if errorType(err) != build.ErrorBuildFailed {
		t.Fatalf("error %v, want %s", err, build.ErrorBuildFailed)
	}
	if h.solver.solves != 1 {
		t.Fatalf("a failed Dockerfile step was retried: %d solves", h.solver.solves)
	}
}

// A malformed request fails in the caller without starting the builder.
func TestMalformedRequestNotAllowed(t *testing.T) {
	h := newHarness()
	_, err := h.run(t, pullRequest("HEAD"))
	if errorType(err) != build.ErrorNotAllowed || h.solver.solves != 0 {
		t.Fatalf("error %v, want %s", err, build.ErrorNotAllowed)
	}
}

func TestOtherRepositoryNotAllowed(t *testing.T) {
	h := newHarness()
	request := pullRequest(strings.Repeat("4", 40))
	request.Repository = "oku-trade/trade"
	_, err := h.run(t, request)
	if errorType(err) != build.ErrorNotAllowed || h.solver.solves != 0 {
		t.Fatalf("error %v after %d solves, want %s", err, h.solver.solves, build.ErrorNotAllowed)
	}
}

func TestInputsKeyIgnoresOtherPaths(t *testing.T) {
	inputs := []string{"cmd", "Dockerfile"}
	base := map[string]string{"cmd/main.go": "a", "Dockerfile": "b", "README.md": "c"}
	readme := map[string]string{"cmd/main.go": "a", "Dockerfile": "b", "README.md": "changed"}
	code := map[string]string{"cmd/main.go": "changed", "Dockerfile": "b", "README.md": "c"}
	if build.InputsKey(base, inputs) != build.InputsKey(readme, inputs) {
		t.Fatal("a change outside the inputs changed the key")
	}
	if build.InputsKey(base, inputs) == build.InputsKey(code, inputs) {
		t.Fatal("a change in the inputs kept the key")
	}
	if build.InputsKey(map[string]string{"cmdline/x": "a"}, []string{"cmd"}) != build.InputsKey(nil, []string{"cmd"}) {
		t.Fatal("a sibling path with the same prefix counted as an input")
	}
}

type recordingStarter struct {
	options []client.StartWorkflowOptions
	args    []any
}

func (s *recordingStarter) ExecuteWorkflow(_ context.Context, options client.StartWorkflowOptions, _ any, args ...any) (client.WorkflowRun, error) {
	s.options = append(s.options, options)
	s.args = append(s.args, args...)
	return nil, nil
}

func TestPushStartsMainBuildForInputChanges(t *testing.T) {
	starter := &recordingStarter{}
	handler := build.PushHandler{Repository: repository, Branch: "main", Inputs: []string{"cmd"}, Temporal: starter}
	after := strings.Repeat("5", 40)
	push := func(ref, file string) []byte {
		return []byte(`{"ref":"` + ref + `","after":"` + after + `","repository":{"full_name":"oku-trade/oku-review"},"commits":[{"modified":["` + file + `"]}]}`)
	}
	for _, body := range [][]byte{push("refs/heads/feature", "cmd/x.go"), push("refs/heads/main", "docs/x.md")} {
		if err := handler.Handle(context.Background(), "push", body); err != nil {
			t.Fatal(err)
		}
	}
	if len(starter.options) != 0 {
		t.Fatalf("started %d builds for pushes that change no input on main", len(starter.options))
	}
	if err := handler.Handle(context.Background(), "push", push("refs/heads/main", "cmd/x.go")); err != nil {
		t.Fatal(err)
	}
	if len(starter.options) != 1 || starter.options[0].ID != build.MainWorkflowID(repository, after) || starter.options[0].TaskQueue != build.TaskQueue {
		t.Fatalf("starts %+v", starter.options)
	}
	if request := starter.args[0].(build.Request); request.Trigger != build.TriggerMain || request.Commit != after {
		t.Fatalf("request %+v", request)
	}
}
