package p7_test

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	acp "github.com/gfx-labs/go-acp"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/execution/acpshim"
	"github.com/gfx-labs/protocol7/pkg/execution/session"
	"github.com/gfx-labs/protocol7/pkg/execution/snapshot"
	"github.com/gfx-labs/protocol7/pkg/execution/snapshot/snapshottest"
	"github.com/gfx-labs/protocol7/pkg/orchestration/activity"
	"github.com/gfx-labs/protocol7/pkg/orchestration/coreworker"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// memoryAgentEnv, when set, makes this test binary an ACP agent with a
// conversation that it keeps on disk, in the directory its --store.root flag
// names, as towa does. It can load a conversation only if its log is there.
//
//	write <path> <text>  writes a file in the session's working directory
//	recall               says whether the conversation was loaded, what it was
//	                     asked before, and the files in the working directory
const memoryAgentEnv = "P7_TEST_MEMORY_AGENT"

func storeRoot(args []string) string {
	for _, arg := range args {
		if root, ok := strings.CutPrefix(arg, "--store.root="); ok {
			return root
		}
	}
	return ""
}

type memoryAgent struct {
	*toolAgent
	store  string
	cwd    string
	loaded bool
}

func (a *memoryAgent) log(id string) string { return filepath.Join(a.store, id+".log") }

func (a *memoryAgent) NewSession(_ context.Context, request acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	id := fmt.Sprintf("conversation-%d", time.Now().UnixNano())
	a.cwd = request.Cwd
	if err := os.MkdirAll(a.store, 0o755); err != nil {
		return acp.NewSessionResponse{}, err
	}
	return acp.NewSessionResponse{SessionId: acp.SessionId(id)}, os.WriteFile(a.log(id), nil, 0o644)
}

func (a *memoryAgent) LoadSession(_ context.Context, request acp.LoadSessionRequest) (acp.LoadSessionResponse, error) {
	if _, err := os.Stat(a.log(string(request.SessionId))); err != nil {
		return acp.LoadSessionResponse{}, acp.NewResourceNotFound("no such conversation")
	}
	a.cwd, a.loaded = request.Cwd, true
	return acp.LoadSessionResponse{}, nil
}

func (a *memoryAgent) Prompt(ctx context.Context, request acp.PromptRequest) (acp.PromptResponse, error) {
	text := strings.TrimSpace(request.Prompt[len(request.Prompt)-1].Text.Text)
	log := a.log(string(request.SessionId))
	earlier, _ := os.ReadFile(log)
	history := strings.ReplaceAll(string(earlier), "\n", "|")
	file, err := os.OpenFile(log, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return acp.PromptResponse{}, err
	}
	_, err = file.WriteString(text + "\n")
	_ = file.Close()
	if err != nil {
		return acp.PromptResponse{}, err
	}
	reply := "ok"
	switch command, rest, _ := strings.Cut(text, " "); command {
	case "write":
		name, content, _ := strings.Cut(rest, " ")
		target := filepath.Join(a.cwd, name)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return acp.PromptResponse{}, err
		}
		if err := os.WriteFile(target, []byte(content), 0o755); err != nil {
			return acp.PromptResponse{}, err
		}
	case "recall":
		var files []string
		_ = filepath.WalkDir(a.cwd, func(path string, entry fs.DirEntry, err error) error {
			if err == nil && entry.Type().IsRegular() {
				content, _ := os.ReadFile(path)
				relative, _ := filepath.Rel(a.cwd, path)
				files = append(files, relative+"="+string(content))
			}
			return nil
		})
		reply = fmt.Sprintf("loaded=%v history=%s files=%s", a.loaded, history, strings.Join(files, ","))
	}
	err = a.connection.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: request.SessionId,
		Update: acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
			Content: acp.ContentBlock{Text: &acp.ContentBlockText{Type: "text", Text: reply}},
		}},
	})
	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, err
}

// memoryWorkflow opens a session, runs a first turn that writes files, waits
// for the signal "go", runs a second turn that asks what the agent remembers,
// and closes the session.
func memoryWorkflow(ctx workflow.Context, host string) ([]string, error) {
	turns := 0
	if err := workflow.SetQueryHandler(ctx, "turns", func() (int, error) { return turns, nil }); err != nil {
		return nil, err
	}
	session, err := p7.OpenSession(ctx, p7.SessionSpec{Placement: p7.Placement{Backend: p7.BackendHost, Host: host}})
	if err != nil {
		return nil, err
	}
	var replies []string
	for _, prompt := range []string{"write note.txt first", "write node_modules/dep/index.js cache"} {
		turn, err := p7.PromptText(ctx, session, p7.TextPrompt{Text: prompt})
		if err != nil {
			return nil, err
		}
		replies = append(replies, turn.Text)
		turns++
	}
	workflow.GetSignalChannel(ctx, "go").Receive(ctx, nil)
	turn, err := p7.PromptText(ctx, session, p7.TextPrompt{Text: "recall"})
	if err != nil {
		return nil, err
	}
	replies = append(replies, turn.Text)
	return replies, session.Close(ctx)
}

// snapshotStack is a Temporal development server, a fake S3 and a consumer
// worker, with core workers that come and go.
type snapshotStack struct {
	t         *testing.T
	temporal  client.Client
	binary    string
	snapshots p7.SnapshotStore
}

func startSnapshotStack(t *testing.T) *snapshotStack {
	t.Helper()
	temporalCLI, err := exec.LookPath("temporal")
	if err != nil {
		t.Skip("the temporal CLI is not on PATH")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	server, err := testsuite.StartDevServer(t.Context(), testsuite.DevServerOptions{
		ExistingPath: temporalCLI, LogLevel: "error", Stdout: os.Stderr, Stderr: os.Stderr,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop() })

	// The credentials come from the standard environment.
	t.Setenv("AWS_ACCESS_KEY_ID", "test-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	s3 := snapshottest.Start(t)

	consumer, err := p7.NewWorker(p7.WorkerConfig{TaskQueue: "consumer", TemporalAddress: server.FrontendHostPort()})
	if err != nil {
		t.Fatal(err)
	}
	consumer.RegisterWorkflow("memory", memoryWorkflow)
	if err := consumer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(consumer.Stop)
	return &snapshotStack{
		t: t, temporal: server.Client(), binary: binary,
		snapshots: p7.SnapshotStore{Endpoint: s3.URL, Bucket: snapshottest.Bucket, Prefix: "p7/sessions", PathStyle: true, Exclude: []string{"node_modules"}},
	}
}

// core starts a core worker on host "test" with its own state directory, as
// a new machine or pod would have. A worker for one session restores it
// before it polls.
func (s *snapshotStack) core(root, session string) *coreworker.Worker {
	s.t.Helper()
	manager, err := newMemoryManager(s.binary, root, s.temporal)
	if err != nil {
		s.t.Fatal(err)
	}
	core, err := coreworker.New(coreworker.Config{Host: "test", Snapshots: s.snapshots, Session: session},
		s.temporal, activity.NewSessions(manager), slog.New(slog.DiscardHandler))
	if err != nil {
		s.t.Fatal(err)
	}
	if err := core.Start(); err != nil {
		s.t.Fatal(err)
	}
	return core
}

func newMemoryManager(binary, root string, temporal client.Client) (*session.Manager, error) {
	return session.NewManager(session.Config{
		Root: root, ShimBinary: binary,
		Harness: session.Harness{Command: binary, Env: []string{memoryAgentEnv + "=1"}, StoreFlag: "--store.root"},
		Tools:   coreworker.ToolCalls{Temporal: temporal},
	})
}

func shortDir(t *testing.T) string {
	t.Helper()
	// The shim's socket path is under it and may be at most 107 bytes.
	root, err := os.MkdirTemp("", "p7")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

// A session's worker is stopped in the middle of the session, and its
// machine's disk is gone. A new worker for the same session, on a machine
// with an empty disk, restores the snapshot, and the next turn finds the
// files and the conversation of the earlier turns. The journal goes on from
// its last sequence number.
func TestASessionContinuesOnAnotherWorkerFromItsSnapshot(t *testing.T) {
	for _, test := range []struct {
		name string
		// restoredAtStart: the new worker is told its session and restores it
		// before it polls. Otherwise it restores the session when its first
		// activity comes.
		restoredAtStart bool
	}{
		{"the new worker is started for the session", true},
		{"the new worker restores the session when it is used", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			stack := startSnapshotStack(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			rootA, rootB := shortDir(t), shortDir(t)
			first := stack.core(rootA, "")

			run, err := stack.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "memory-" + fmt.Sprint(test.restoredAtStart), TaskQueue: "consumer"}, "memory", "test")
			if err != nil {
				t.Fatal(err)
			}
			for turns := 0; turns < 2; {
				response, err := stack.temporal.QueryWorkflow(ctx, run.GetID(), "", "turns")
				if err == nil {
					_ = response.Get(&turns)
				}
				time.Sleep(100 * time.Millisecond)
			}
			ids, err := os.ReadDir(filepath.Join(rootA, "sessions"))
			if err != nil || len(ids) != 1 {
				t.Fatalf("sessions in the first worker's directory: %v, %v", ids, err)
			}
			id := ids[0].Name()

			// The worker is stopped, its harness and shim go with it, and the
			// disk is lost.
			first.Stop()
			if err := acpshim.Stop(ctx, filepath.Join(rootA, "sessions", id, "shim")); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(rootA); err != nil {
				t.Fatal(err)
			}

			session := ""
			if test.restoredAtStart {
				session = id
			}
			second := stack.core(rootB, session)
			t.Cleanup(second.Stop)
			if test.restoredAtStart {
				// Restored before the worker was asked anything.
				if _, err := os.Stat(filepath.Join(rootB, "sessions", id, "session.json")); err != nil {
					t.Errorf("the session was not restored when the worker started: %v", err)
				}
			}
			if err := stack.temporal.SignalWorkflow(ctx, run.GetID(), "", "go", nil); err != nil {
				t.Fatal(err)
			}
			var replies []string
			if err := run.Get(ctx, &replies); err != nil {
				t.Fatal(err)
			}
			recall := replies[len(replies)-1]
			for _, want := range []string{
				// The conversation was loaded, with the earlier prompts in it.
				"loaded=true", "history=write note.txt first|write node_modules/dep/index.js cache|",
				// The files of the earlier turns are there, except the excluded ones.
				"note.txt=first",
			} {
				if !strings.Contains(recall, want) {
					t.Errorf("the agent said %q, want it to contain %q", recall, want)
				}
			}
			if _, files, _ := strings.Cut(recall, "files="); strings.Contains(files, "node_modules") {
				t.Errorf("the agent said %q, want the excluded directory to be gone", recall)
			}

			// The journal has every turn, once, in order.
			frames, _, err := acpshim.ReadJournal(filepath.Join(rootB, "sessions", id, "shim"), 0)
			if err != nil {
				t.Fatal(err)
			}
			var prompts []string
			for index, frame := range frames {
				if index > 0 && frame.Seq != frames[index-1].Seq+1 {
					t.Errorf("journal sequence %d follows %d", frame.Seq, frames[index-1].Seq)
				}
				if strings.Contains(string(frame.Msg), `"session/prompt"`) {
					prompts = append(prompts, string(frame.Msg))
				}
			}
			if len(prompts) != 3 {
				t.Errorf("the journal has %d prompts, want 3", len(prompts))
			}

			// Closing the session deletes its snapshot.
			store, err := snapshot.New(stack.snapshots, nil)
			if err != nil {
				t.Fatal(err)
			}
			found, err := store.Restore(ctx, id, filepath.Join(shortDir(t), "x"), nil)
			if err != nil || found {
				t.Errorf("after the session closed its snapshot is still there: %v, %v", found, err)
			}
		})
	}
}
