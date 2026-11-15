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
	if tool == "permission" || tool == "permission-allow-only" {
		reply, err := a.askPermission(ctx, request.SessionId, text, tool == "permission-allow-only")
		if err != nil {
			return acp.PromptResponse{}, err
		}
		return a.reply(ctx, request.SessionId, reply)
	}
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
	return a.reply(ctx, request.SessionId, reply)
}

func (a *toolAgent) reply(ctx context.Context, id acp.SessionId, text string) (acp.PromptResponse, error) {
	err := a.connection.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: id,
		Update: acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
			Content: acp.ContentBlock{Text: &acp.ContentBlockText{Type: "text", Text: text}},
		}},
	})
	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, err
}

// askPermission asks its client to allow running command, as an agent does
// before a tool call it holds back, and says what the client chose.
func (a *toolAgent) askPermission(ctx context.Context, id acp.SessionId, command string, allowOnly bool) (string, error) {
	kind, title := acp.ToolKindExecute, command
	options := []acp.PermissionOption{{OptionId: "allow", Name: "Allow", Kind: acp.PermissionOptionKindAllowOnce}}
	if !allowOnly {
		options = append(options, acp.PermissionOption{OptionId: "reject", Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce})
	}
	response, err := a.connection.RequestPermission(ctx, acp.RequestPermissionRequest{
		SessionId: id,
		ToolCall:  acp.ToolCallUpdate{ToolCallId: "call-1", Kind: &kind, Title: &title, RawInput: map[string]any{"command": command}},
		Options:   options,
	})
	if err != nil {
		return "", err
	}
	if response.Outcome.Selected == nil {
		return "permission cancelled", nil
	}
	return "permission selected " + string(response.Outcome.Selected.OptionId), nil
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

// Ways permissionWorkflow can be set up.
const (
	// permissionsDecided: the function decides, and the workflow answers.
	permissionsDecided = "decided"
	// permissionsNone: the session has no function.
	permissionsNone = "none"
	// permissionsRefused: the workflow answers, but says it opened no session.
	permissionsRefused = "refused"
	// permissionsUnanswered: the session has a function and the workflow
	// never calls HandlePermissions, so the update has no handler.
	permissionsUnanswered = "unanswered"
)

// permissionWorkflow opens a session whose permission requests are handled
// as mode says, sends it the prompt and returns the agent's reply and, in
// brackets, what the function decided each time it ran. The function checks
// what the request says about the call, denies a request whose title has
// "rm", fails on "boom" and allows the rest.
func permissionWorkflow(ctx workflow.Context, host, mode, prompt string) (string, error) {
	var opened, decided []string
	decide := func(ctx workflow.Context, request p7.PermissionRequest) (p7.PermissionDecision, error) {
		if len(opened) != 1 || request.Session != opened[0] || request.ToolCallID != "call-1" || request.Kind != "execute" ||
			string(request.RawInput) != `{"command":"`+request.Title+`"}` || len(request.Options) < 1 {
			decided = append(decided, fmt.Sprintf("unexpected request %+v", request))
			return p7.Deny("unexpected request"), nil
		}
		switch {
		case strings.Contains(request.Title, "boom"):
			decided = append(decided, "error")
			return p7.PermissionDecision{}, errors.New("cannot decide")
		case strings.Contains(request.Title, "rm"):
			decided = append(decided, "deny")
			return p7.Deny("no deleting"), nil
		}
		decided = append(decided, "allow")
		return p7.Allow(), nil
	}
	spec := p7.SessionSpec{Placement: p7.Placement{Backend: p7.BackendHost, Host: host}}
	if mode != permissionsNone {
		spec.Permissions = decide
	}
	sessions := func() []string { return opened }
	if mode == permissionsRefused {
		sessions = func() []string { return nil }
	}
	if mode == permissionsDecided || mode == permissionsRefused {
		if err := p7.HandlePermissions(ctx, decide, sessions); err != nil {
			return "", err
		}
	}
	session, err := p7.OpenSession(ctx, spec)
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
	return turn.Text + " [" + strings.Join(decided, ",") + "]", nil
}

// An agent's permission request is decided by the workflow that opened the
// session, through a Temporal update: allowed, denied, or denied because the
// function failed. A request the workflow cannot take is denied, and a
// session opened with no function has every request allowed. Each case shows
// that the function ran, or did not, with the request it should have got:
// a reject chosen by the shim alone could also be the function's own refusal
// of a wrong request.
func TestAWorkflowDecidesItsSessionsPermissionRequests(t *testing.T) {
	temporalClient := startStack(t, agentEnv, func(w *p7.Worker) { w.RegisterWorkflow("permissions", permissionWorkflow) })
	for _, test := range []struct {
		name, mode, prompt, want string
	}{
		{"allowed", permissionsDecided, "permission ls", "permission selected allow [allow]"},
		{"denied", permissionsDecided, "permission rm -r data", "permission selected reject [deny]"},
		{"decision fails", permissionsDecided, "permission boom", "permission selected reject [error]"},
		{"denied and no reject option", permissionsDecided, "permission-allow-only rm -r data", "permission cancelled [deny]"},
		{"no function", permissionsNone, "permission rm -r data", "permission selected allow []"},
		{"update refused by the validator", permissionsRefused, "permission ls", "permission selected reject []"},
		{"update with no handler", permissionsUnanswered, "permission ls", "permission selected reject []"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			run, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: "consumer"}, "permissions", "test", test.mode, test.prompt)
			if err != nil {
				t.Fatal(err)
			}
			var reply string
			if err := run.Get(ctx, &reply); err != nil {
				t.Fatal(err)
			}
			if reply != test.want {
				t.Errorf("the agent replied %q, want %q", reply, test.want)
			}
		})
	}
}
