package p7_test

import (
	"log/slog"
	"os"
	"os/exec"
	"testing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"

	"github.com/gfx-labs/protocol7/pkg/execution/session"
	"github.com/gfx-labs/protocol7/pkg/orchestration/activity"
	"github.com/gfx-labs/protocol7/pkg/orchestration/coreworker"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// startStack runs a Temporal development server, a core worker on host
// "test" whose harness is this test binary as the scripted agent named by
// agentVar, and a consumer worker on the queue "consumer" with the workflows
// register adds. It skips the test when the temporal CLI is not on PATH.
func startStack(t *testing.T, agentVar string, register func(*p7.Worker)) client.Client {
	t.Helper()
	return startStackWith(t, agentVar, p7.WorkerConfig{}, register)
}

// startStackWith is startStack with the consumer worker's configuration. Its
// task queue and Temporal address are set here.
func startStackWith(t *testing.T, agentVar string, config p7.WorkerConfig, register func(*p7.Worker)) client.Client {
	t.Helper()
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

	// The shim's socket path is under the root and may be at most 107 bytes.
	root, err := os.MkdirTemp("", "p7")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	manager, err := session.NewManager(session.Config{
		Root: root, ShimBinary: binary,
		Harness: session.Harness{Command: binary, Env: []string{agentVar + "=1"}},
		Tools:   coreworker.ToolCalls{Temporal: temporalClient},
	})
	if err != nil {
		t.Fatal(err)
	}
	core, err := coreworker.New(coreworker.Config{Host: "test"}, temporalClient, activity.NewSessions(manager), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if err := core.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(core.Stop)

	config.TaskQueue, config.TemporalAddress = "consumer", server.FrontendHostPort()
	consumer, err := p7.NewWorker(config)
	if err != nil {
		t.Fatal(err)
	}
	register(consumer)
	if err := consumer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(consumer.Stop)
	return temporalClient
}
