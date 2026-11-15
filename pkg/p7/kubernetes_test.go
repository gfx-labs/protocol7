package p7_test

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
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
	"github.com/gfx-labs/protocol7/pkg/execution/session"
	orchestration "github.com/gfx-labs/protocol7/pkg/orchestration/activity"
	"github.com/gfx-labs/protocol7/pkg/orchestration/coreworker"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// fakePods stands in for the kubernetes backend: starting a pod starts a
// core worker on the session's own queue, as `p7 session` does in a
// real pod, and deleting it stops that worker.
type fakePods struct {
	t        *testing.T
	temporal client.Client
	binary   string
	root     string

	mu      sync.Mutex
	workers map[string]*coreworker.Worker
	started []domain.PodRequest
	deleted []string
}

func (f *fakePods) start(_ context.Context, request domain.PodRequest) (domain.PodHandle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	handle := domain.PodHandle{Name: "p7-session-" + request.Session, TaskQueue: domain.SessionQueue(request.Session)}
	f.started = append(f.started, request)
	if f.workers[request.Session] != nil {
		return handle, nil
	}
	manager, err := session.NewManager(session.Config{
		Root: f.root, ShimBinary: f.binary,
		Harness: session.Harness{Command: f.binary, Env: []string{agentEnv + "=1"}},
		Tools:   coreworker.ToolCalls{Temporal: f.temporal},
	})
	if err != nil {
		return handle, err
	}
	core, err := coreworker.New(coreworker.Config{TaskQueue: handle.TaskQueue}, f.temporal, orchestration.NewSessions(manager), slog.New(slog.DiscardHandler))
	if err != nil {
		return handle, err
	}
	if err := core.Start(); err != nil {
		return handle, err
	}
	f.workers[request.Session] = core
	return handle, nil
}

func (f *fakePods) delete(_ context.Context, request domain.PodRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, request.Session)
	if core := f.workers[request.Session]; core != nil {
		// Stopping waits for running activities, among them this close's
		// caller's, so it runs apart.
		go core.Stop()
		delete(f.workers, request.Session)
	}
	return nil
}

func kubernetesWorkflow(ctx workflow.Context, prompt string) (string, error) {
	var opened []string
	tools := []p7.Tool{{Name: "echo", Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
		return p7.TextResult("echo " + string(call.Arguments)), nil
	}}}
	if err := p7.HandleTools(ctx, tools, func() []string { return opened }); err != nil {
		return "", err
	}
	session, err := p7.OpenSession(ctx, p7.SessionSpec{Placement: p7.Placement{Backend: p7.BackendKubernetes}, Tools: tools})
	if err != nil {
		return "", err
	}
	opened = append(opened, session.ID)
	if session.Pod == "" || session.TaskQueue != domain.SessionQueue(session.ID) {
		return "", workflow.NewContinueAsNewError(ctx, "never")
	}
	turn, err := p7.PromptText(ctx, session, p7.TextPrompt{Text: prompt})
	if err != nil {
		return "", err
	}
	if err := session.Close(ctx); err != nil {
		return "", err
	}
	return turn.Text, nil
}

// On the kubernetes backend a session is opened, prompted and closed by the
// worker in its own pod, on the session's own queue, and closing deletes
// the pod. The pod's owner is the workflow that opened it.
func TestAKubernetesSessionRunsInItsOwnPod(t *testing.T) {
	temporalCLI, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("the temporal CLI is not on PATH")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
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
	root, err := os.MkdirTemp("", "p7")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })

	pods := &fakePods{t: t, temporal: temporalClient, binary: binary, root: root, workers: map[string]*coreworker.Worker{}}
	backend := worker.New(temporalClient, domain.KubernetesQueue, worker.Options{})
	backend.RegisterActivityWithOptions(pods.start, activity.RegisterOptions{Name: domain.ActivityStartSessionPod})
	backend.RegisterActivityWithOptions(pods.delete, activity.RegisterOptions{Name: domain.ActivityDeleteSessionPod})
	if err := backend.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(backend.Stop)

	consumer, err := p7.NewWorker(p7.WorkerConfig{TaskQueue: "consumer", TemporalAddress: server.FrontendHostPort()})
	if err != nil {
		t.Fatal(err)
	}
	consumer.RegisterWorkflow("kubernetes", kubernetesWorkflow)
	if err := consumer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(consumer.Stop)

	runCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	run, err := temporalClient.ExecuteWorkflow(runCtx, client.StartWorkflowOptions{ID: "owner-1", TaskQueue: "consumer"}, "kubernetes", "echo hi")
	if err != nil {
		t.Fatal(err)
	}
	var reply string
	if err := run.Get(runCtx, &reply); err != nil {
		t.Fatal(err)
	}
	if want := `echo {\"text\":\"hi\"}`; !strings.Contains(reply, want) {
		t.Errorf("the agent replied %s, want the tool's answer %s", reply, want)
	}
	pods.mu.Lock()
	defer pods.mu.Unlock()
	if len(pods.started) != 1 || pods.started[0].Owner != "owner-1" || pods.started[0].OpenTimeout != 15*time.Minute ||
		len(pods.deleted) != 1 || pods.deleted[0] != pods.started[0].Session {
		t.Errorf("started %+v and deleted %v, want one pod owned by owner-1 with the default open timeout, deleted on close", pods.started, pods.deleted)
	}
}
