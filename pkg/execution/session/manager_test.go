package session_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	acp "github.com/gfx-labs/go-acp"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/acpshim"
	"github.com/gfx-labs/protocol7/pkg/execution/journal"
	"github.com/gfx-labs/protocol7/pkg/execution/session"
)

// agentLogEnv, when set, makes this test binary an ACP agent that writes
// what it is asked to the named file. The tests run the real shim, with this
// binary as both the shim command and the harness behind it.
const agentLogEnv = "P7_TEST_AGENT_LOG"

func TestMain(m *testing.M) {
	acpshim.MaybeChild()
	if os.Getenv(agentLogEnv) == "exit" {
		os.Stderr.WriteString("scripted agent: no login\n")
		os.Exit(3)
	}
	if log := os.Getenv(agentLogEnv); log != "" {
		agent := &scriptedAgent{log: log}
		agent.connection = acp.NewAgentSideConnection(agent, os.Stdout, os.Stdin, slog.New(slog.DiscardHandler))
		_ = agent.connection.Serve(context.Background())
		os.Exit(0)
	}
	if len(os.Args) > 1 && os.Args[1] == "start" {
		os.Exit(acpshim.Main(os.Args[1:], os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

// scriptedAgent answers a prompt with "echo: " and its text. The prompt
// "slow" runs until it is cancelled. It can load any conversation.
type scriptedAgent struct {
	connection *acp.AgentSideConnection
	log        string
	mu         sync.Mutex
	cancelled  chan struct{}
}

func (a *scriptedAgent) record(line string) {
	file, err := os.OpenFile(a.log, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	if _, err := file.WriteString(line + "\n"); err != nil {
		panic(err)
	}
}

func (a *scriptedAgent) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{
		ProtocolVersion:   acp.ProtocolVersionNumber,
		AgentCapabilities: acp.AgentCapabilities{LoadSession: true},
	}, nil
}

func (a *scriptedAgent) NewSession(context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	a.record("new")
	return acp.NewSessionResponse{SessionId: "conversation"}, nil
}

func (a *scriptedAgent) LoadSession(context.Context, acp.LoadSessionRequest) (acp.LoadSessionResponse, error) {
	a.record("load")
	return acp.LoadSessionResponse{}, nil
}

func (a *scriptedAgent) Prompt(ctx context.Context, request acp.PromptRequest) (acp.PromptResponse, error) {
	var blocks []string
	for _, block := range request.Prompt {
		blocks = append(blocks, strings.TrimSpace(block.Text.Text))
	}
	text := strings.Join(blocks, " | ")
	if text == "slow" {
		a.mu.Lock()
		a.cancelled = make(chan struct{})
		cancelled := a.cancelled
		a.mu.Unlock()
		a.record("prompt " + text)
		<-cancelled
		return acp.PromptResponse{StopReason: acp.StopReasonCancelled}, nil
	}
	a.record("prompt " + text)
	err := a.connection.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: request.SessionId,
		Update: acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
			Content: acp.ContentBlock{Text: &acp.ContentBlockText{Type: "text", Text: "echo: " + text}},
		}},
	})
	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, err
}

func (a *scriptedAgent) Cancel(context.Context, acp.CancelNotification) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cancelled != nil {
		close(a.cancelled)
		a.cancelled = nil
	}
	return nil
}

var errUnused = errors.New("the scripted agent does not implement this")

func (a *scriptedAgent) Authenticate(context.Context, acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	return acp.AuthenticateResponse{}, errUnused
}

func (a *scriptedAgent) Logout(context.Context, acp.LogoutRequest) (acp.LogoutResponse, error) {
	return acp.LogoutResponse{}, errUnused
}

func (a *scriptedAgent) CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	return acp.CloseSessionResponse{}, errUnused
}

func (a *scriptedAgent) DeleteSession(context.Context, acp.DeleteSessionRequest) (acp.DeleteSessionResponse, error) {
	return acp.DeleteSessionResponse{}, errUnused
}

func (a *scriptedAgent) ListSessions(context.Context, acp.ListSessionsRequest) (acp.ListSessionsResponse, error) {
	return acp.ListSessionsResponse{}, errUnused
}

func (a *scriptedAgent) ResumeSession(context.Context, acp.ResumeSessionRequest) (acp.ResumeSessionResponse, error) {
	return acp.ResumeSessionResponse{}, errUnused
}

func (a *scriptedAgent) SetSessionConfigOption(context.Context, acp.SetSessionConfigOptionRequest) (acp.SetSessionConfigOptionResponse, error) {
	return acp.SetSessionConfigOptionResponse{}, errUnused
}

func (a *scriptedAgent) SetSessionMode(context.Context, acp.SetSessionModeRequest) (acp.SetSessionModeResponse, error) {
	return acp.SetSessionModeResponse{}, errUnused
}

// fixture is a manager with one open session, "s1", whose harness is the
// scripted agent.
type fixture struct {
	t       *testing.T
	manager *session.Manager
	root    string
	log     string
}

func open(t *testing.T) *fixture {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// The shim's socket path is under the root and may be at most 107
	// bytes, which t.TempDir exceeds for a long test name.
	root, err := os.MkdirTemp("", "p7")
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, root: root, log: filepath.Join(root, "agent.log")}
	f.manager, err = session.NewManager(session.Config{
		Root: root, ShimBinary: binary,
		Harness: session.Harness{Command: binary, Env: []string{agentLogEnv + "=" + f.log}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// The shim is a detached process. It must not outlive the test.
		if err := f.manager.Close(context.Background(), domain.CloseRequest{Session: "s1"}); err != nil {
			t.Errorf("close: %v", err)
		}
		_ = os.RemoveAll(root)
	})
	if _, err := f.manager.Open(t.Context(), domain.SessionRequest{ID: "s1"}); err != nil {
		t.Fatal(err)
	}
	return f
}

// asked is what the agent was asked, in order.
func (f *fixture) asked() []string {
	f.t.Helper()
	content, err := os.ReadFile(f.log)
	if err != nil {
		f.t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(content)), "\n")
}

func (f *fixture) turn(ctx context.Context, turn, prompt string, control session.TurnControl) (domain.TurnResult, error) {
	return f.manager.RunTurn(ctx, domain.TurnRequest{Session: "s1", Turn: turn, Prompt: prompt}, control)
}

// leaveRunning starts a slow turn and detaches from it once the agent has
// the prompt, as a worker that shuts down does.
func (f *fixture) leaveRunning(turn string) {
	f.t.Helper()
	detach := make(chan struct{})
	go func() {
		defer close(detach)
		for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if content, _ := os.ReadFile(f.log); strings.Contains(string(content), "prompt slow\n") {
				return
			}
		}
	}()
	if _, err := f.turn(f.t.Context(), turn, "slow", session.TurnControl{Detach: detach}); !errors.Is(err, session.ErrDetached) {
		f.t.Fatalf("a turn whose caller left: got error %v, want ErrDetached", err)
	}
}

func TestAHarnessThatExitsWhileItStartsIsReportedWithItsOutput(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp("", "p7")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	manager, err := session.NewManager(session.Config{
		Root: root, ShimBinary: binary,
		Harness: session.Harness{Command: binary, Env: []string{agentLogEnv + "=exit"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.Open(t.Context(), domain.SessionRequest{ID: "s1"})
	if !errors.Is(err, session.ErrHarnessStopped) || !strings.HasSuffix(err.Error(), "exit status 3). Its last output:\nscripted agent: no login") {
		t.Errorf("got error %v, want ErrHarnessStopped with the exit status and the harness's output", err)
	}
}

func TestARepeatedTurnReturnsItsResultWithoutASecondPrompt(t *testing.T) {
	f := open(t)
	first, err := f.turn(t.Context(), "t1", "hello", session.TurnControl{})
	if err != nil {
		t.Fatal(err)
	}
	again, err := f.turn(t.Context(), "t1", "hello", session.TurnControl{})
	if err != nil {
		t.Fatal(err)
	}
	if first.StopReason != "end_turn" || first.Text != "echo: hello" {
		t.Errorf("got stop %q text %q, want end_turn and the echo", first.StopReason, first.Text)
	}
	if again != first {
		t.Errorf("the repeated turn returned %+v, want %+v", again, first)
	}
	if asked, want := f.asked(), []string{"new", "prompt hello"}; !slices.Equal(asked, want) {
		t.Errorf("the agent was asked %q, want %q", asked, want)
	}
}

// The context a workflow puts in front of a prompt reaches the agent, and a
// viewer of the session sees only what the person wrote.
func TestContextReachesTheAgentAndIsNotShownAsTheMessage(t *testing.T) {
	f := open(t)
	request := domain.TurnRequest{Session: "s1", Turn: "t1", Context: "instructions", Prompt: "hello"}
	if _, err := f.manager.RunTurn(t.Context(), request, session.TurnControl{}); err != nil {
		t.Fatal(err)
	}
	if asked, want := f.asked(), []string{"new", "prompt instructions | hello"}; !slices.Equal(asked, want) {
		t.Errorf("the agent was asked %q, want %q", asked, want)
	}

	file, err := os.Open(acpshim.JournalPath(filepath.Join(f.root, "sessions", "s1", "shim")))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder := journal.NewDecoder("s1")
	var shown []string
	for lines := json.NewDecoder(file); ; {
		var frame domain.Frame
		if err := lines.Decode(&frame); err != nil {
			break
		}
		for _, event := range decoder.Decode(frame) {
			if event.Kind == domain.EventPrompt {
				shown = append(shown, event.Turn+":"+event.Text)
			}
		}
	}
	if want := []string{"t1:hello"}; !slices.Equal(shown, want) {
		t.Errorf("a viewer is shown the prompts %q, want %q", shown, want)
	}
}

func TestATurnLeftRunningIsPickedUpAndCanBeCancelled(t *testing.T) {
	f := open(t)
	f.leaveRunning("t1")

	// The second caller attaches to the running turn, then cancels it.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result, err := f.turn(ctx, "t1", "slow", session.TurnControl{Beat: cancel})
	if err != nil {
		t.Fatal(err)
	}
	if result.StopReason != "cancelled" {
		t.Errorf("got stop %q, want cancelled", result.StopReason)
	}
	if asked, want := f.asked(), []string{"new", "prompt slow"}; !slices.Equal(asked, want) {
		t.Errorf("the agent was asked %q, want %q", asked, want)
	}
}

func TestAShimStoppedDuringATurnInterruptsItAndTheNextTurnLoadsTheConversation(t *testing.T) {
	f := open(t)
	f.leaveRunning("t1")
	if err := acpshim.Stop(t.Context(), filepath.Join(f.root, "sessions", "s1", "shim")); err != nil {
		t.Fatal(err)
	}

	interrupted, err := f.turn(t.Context(), "t1", "slow", session.TurnControl{})
	if err != nil {
		t.Fatal(err)
	}
	if interrupted.StopReason != domain.StopInterrupted || interrupted.Failure == nil {
		t.Errorf("got %+v, want an interrupted turn with a failure", interrupted)
	}
	next, err := f.turn(t.Context(), "t2", "hello", session.TurnControl{})
	if err != nil {
		t.Fatal(err)
	}
	if next.StopReason != "end_turn" || next.Text != "echo: hello" {
		t.Errorf("the turn after: got stop %q text %q, want end_turn and the echo", next.StopReason, next.Text)
	}
	if asked, want := f.asked(), []string{"new", "prompt slow", "load", "prompt hello"}; !slices.Equal(asked, want) {
		t.Errorf("the agent was asked %q, want %q", asked, want)
	}
}
