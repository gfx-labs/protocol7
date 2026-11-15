package p7_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	acp "github.com/gfx-labs/go-acp"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/execution/acpshim"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// agentEnv, when set, makes this test binary an ACP agent that calls the
// tools its client offers. The tests run the real shim, with this binary
// as both the shim command and the harness behind it.
const agentEnv = "P7_TEST_TOOL_AGENT"

func TestMain(m *testing.M) {
	acpshim.MaybeChild()
	if os.Getenv(agentEnv) != "" {
		agent := &toolAgent{}
		agent.connection = acp.NewAgentSideConnection(agent, os.Stdout, os.Stdin, slog.New(slog.DiscardHandler))
		_ = agent.connection.Serve(context.Background())
		os.Exit(0)
	}
	if os.Getenv(reportAgentEnv) != "" {
		base := &toolAgent{}
		agent := &reportAgent{toolAgent: base}
		base.connection = acp.NewAgentSideConnection(agent, os.Stdout, os.Stdin, slog.New(slog.DiscardHandler))
		_ = base.connection.Serve(context.Background())
		os.Exit(0)
	}
	if os.Getenv(memoryAgentEnv) != "" {
		base := &toolAgent{}
		agent := &memoryAgent{toolAgent: base, store: storeRoot(os.Args)}
		base.connection = acp.NewAgentSideConnection(agent, os.Stdout, os.Stdin, slog.New(slog.DiscardHandler))
		_ = base.connection.Serve(context.Background())
		os.Exit(0)
	}
	if len(os.Args) > 1 && os.Args[1] == "start" {
		os.Exit(acpshim.Main(os.Args[1:], os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

// toolAgent answers a prompt "<tool> <text>" by listing the tools of the
// one MCP server its client offered, calling <tool> with {"text": <text>}
// and replying with what it saw.
type toolAgent struct {
	connection *acp.AgentSideConnection
	server     acp.UnstableMcpServerAcpId
	calls      int
}

func (a *toolAgent) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{
		ProtocolVersion:   acp.ProtocolVersionNumber,
		AgentCapabilities: acp.AgentCapabilities{LoadSession: true, McpCapabilities: acp.McpCapabilities{Acp: true}},
	}, nil
}

func (a *toolAgent) NewSession(_ context.Context, request acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	for _, server := range request.McpServers {
		if server.Acp != nil {
			a.server = acp.UnstableMcpServerAcpId(server.Acp.ServerId)
		}
	}
	return acp.NewSessionResponse{SessionId: "conversation"}, nil
}

func (a *toolAgent) LoadSession(context.Context, acp.LoadSessionRequest) (acp.LoadSessionResponse, error) {
	return acp.LoadSessionResponse{}, nil
}

func (a *toolAgent) mcp(ctx context.Context, method string, params map[string]any) (string, error) {
	a.calls++
	response, err := a.connection.UnstableMessageMcp(ctx, acp.UnstableMessageMcpRequest{
		ServerId: a.server, RequestId: acp.UnstableMcpRequestId(fmt.Sprint(a.calls)), Method: method, Params: params,
	})
	if err != nil {
		return "", err
	}
	if response.Error != nil {
		return "", fmt.Errorf("%s: %s", method, response.Error.Error.Message)
	}
	content, err := json.Marshal(response.Result.Result)
	return string(content), err
}

func (a *toolAgent) Prompt(ctx context.Context, request acp.PromptRequest) (acp.PromptResponse, error) {
	tool, text, _ := strings.Cut(strings.TrimSpace(request.Prompt[0].Text.Text), " ")
	reply := "no tools were offered"
	if a.server != "" {
		listed, err := a.mcp(ctx, "tools/list", nil)
		if err != nil {
			return acp.PromptResponse{}, err
		}
		called, err := a.mcp(ctx, "tools/call", map[string]any{"name": tool, "arguments": map[string]any{"text": text}})
		if err != nil {
			return acp.PromptResponse{}, err
		}
		reply = "listed " + listed + " called " + called
	}
	err := a.connection.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: request.SessionId,
		Update: acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
			Content: acp.ContentBlock{Text: &acp.ContentBlockText{Type: "text", Text: reply}},
		}},
	})
	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, err
}

func (a *toolAgent) Cancel(context.Context, acp.CancelNotification) error { return nil }

var errUnused = errors.New("the tool agent does not implement this")

func (a *toolAgent) Authenticate(context.Context, acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	return acp.AuthenticateResponse{}, errUnused
}

func (a *toolAgent) Logout(context.Context, acp.LogoutRequest) (acp.LogoutResponse, error) {
	return acp.LogoutResponse{}, errUnused
}

func (a *toolAgent) CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	return acp.CloseSessionResponse{}, errUnused
}

func (a *toolAgent) DeleteSession(context.Context, acp.DeleteSessionRequest) (acp.DeleteSessionResponse, error) {
	return acp.DeleteSessionResponse{}, errUnused
}

func (a *toolAgent) ListSessions(context.Context, acp.ListSessionsRequest) (acp.ListSessionsResponse, error) {
	return acp.ListSessionsResponse{}, errUnused
}

func (a *toolAgent) ResumeSession(context.Context, acp.ResumeSessionRequest) (acp.ResumeSessionResponse, error) {
	return acp.ResumeSessionResponse{}, errUnused
}

func (a *toolAgent) SetSessionConfigOption(context.Context, acp.SetSessionConfigOptionRequest) (acp.SetSessionConfigOptionResponse, error) {
	return acp.SetSessionConfigOptionResponse{}, errUnused
}

func (a *toolAgent) SetSessionMode(context.Context, acp.SetSessionModeRequest) (acp.SetSessionModeResponse, error) {
	return acp.SetSessionModeResponse{}, errUnused
}

// toolWorkflow opens a session with the tool "echo", sends it the prompt and
// returns the agent's reply. echo answers with its arguments and the
// session that called it.
func toolWorkflow(ctx workflow.Context, host, prompt string) (string, error) {
	var opened []string
	echo := p7.Tool{
		Name:        "echo",
		Description: "Repeats its text.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}`),
		Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
			if call.Session != opened[0] {
				return p7.ToolResult{}, fmt.Errorf("called from session %s", call.Session)
			}
			return p7.TextResult("echo " + string(call.Arguments)), nil
		},
	}
	tools := []p7.Tool{echo}
	if err := p7.HandleTools(ctx, tools, func() []string { return opened }); err != nil {
		return "", err
	}
	session, err := p7.OpenSession(ctx, p7.SessionSpec{
		Placement: p7.Placement{Backend: p7.BackendHost, Host: host},
		Tools:     tools,
	})
	if err != nil {
		return "", err
	}
	opened = append(opened, session.ID)
	turn, err := p7.PromptText(ctx, session, p7.TextPrompt{Text: prompt})
	if err != nil {
		return "", err
	}
	if err := session.Close(ctx); err != nil {
		return "", err
	}
	return turn.Text, nil
}

// A tool the workflow offers is listed to the agent over the ACP connection
// and called through a Temporal update to the workflow. A tool the workflow
// did not offer is refused, and the agent reads the refusal as the tool's
// error.
func TestASessionCallsItsWorkflowsTools(t *testing.T) {
	temporalClient := startStack(t, agentEnv, func(w *p7.Worker) { w.RegisterWorkflow("tools", toolWorkflow) })
	ctx := t.Context()

	for _, test := range []struct{ prompt, want string }{
		{
			prompt: "echo hello",
			want: `listed {"tools":[{"description":"Repeats its text.","inputSchema":{"properties":{"text":{"type":"string"}},"required":["text"],"type":"object"},"name":"echo"}]}` +
				` called {"content":[{"text":"echo {\"text\":\"hello\"}","type":"text"}],"isError":false}`,
		},
		{
			prompt: "delete hello",
			want:   `"isError":true`,
		},
	} {
		runCtx, cancel := context.WithTimeout(ctx, time.Minute)
		run, err := temporalClient.ExecuteWorkflow(runCtx, client.StartWorkflowOptions{TaskQueue: "consumer"}, "tools", "test", test.prompt)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		var reply string
		err = run.Get(runCtx, &reply)
		cancel()
		if err != nil {
			t.Fatalf("prompt %q: %v", test.prompt, err)
		}
		if !strings.Contains(reply, test.want) {
			t.Errorf("prompt %q: the agent replied %s, want it to contain %s", test.prompt, reply, test.want)
		}
		if test.prompt == "delete hello" && !strings.Contains(reply, `no tool \"delete\"`) {
			t.Errorf("prompt %q: the agent replied %s, want the refusal to name the tool", test.prompt, reply)
		}
	}
}
