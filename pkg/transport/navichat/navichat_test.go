package navichat_test

import (
	"context"
	"encoding/json"
	"iter"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/gfx-labs/protocol7/pkg/transport/navichat"
)

// chatAgent behaves like navi's A2A mapping: the context is the chat, each
// message is one turn, a turn that asks something is input-required, and a
// cancel stops the running turn. The message text picks the behavior.
type chatAgent struct{}

func (chatAgent) Execute(ctx context.Context, execution *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		if !yield(a2a.NewSubmittedTask(execution, execution.Message), nil) {
			return
		}
		if !yield(a2a.NewStatusUpdateEvent(execution, a2a.TaskStateWorking, nil), nil) {
			return
		}
		text := execution.Message.Parts[0].Text()
		switch {
		case strings.HasPrefix(text, "slow"):
			<-ctx.Done()
			return
		case strings.HasPrefix(text, "ask"):
			time.Sleep(100 * time.Millisecond)
			question := a2a.NewMessageForTask(a2a.MessageRoleAgent, execution, a2a.NewTextPart("Which repository?"))
			yield(a2a.NewStatusUpdateEvent(execution, a2a.TaskStateInputRequired, question), nil)
		default:
			time.Sleep(300 * time.Millisecond)
			answer := a2a.NewMessageForTask(a2a.MessageRoleAgent, execution, a2a.NewTextPart("done: "+text))
			yield(a2a.NewStatusUpdateEvent(execution, a2a.TaskStateCompleted, answer), nil)
		}
	}
}

func (chatAgent) Cancel(_ context.Context, execution *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		yield(a2a.NewStatusUpdateEvent(execution, a2a.TaskStateCanceled, nil), nil)
	}
}

// loginUser names the caller by the login header, as navi does.
type loginUser struct {
	a2asrv.PassthroughCallInterceptor
}

func (loginUser) Before(ctx context.Context, call *a2asrv.CallContext, _ *a2asrv.Request) (context.Context, any, error) {
	logins, _ := call.ServiceParams().Get(navichat.LoginHeader)
	if len(logins) == 0 || logins[0] == "" {
		return ctx, nil, a2a.ErrUnauthenticated
	}
	call.User = a2asrv.NewAuthenticatedUser(logins[0], nil)
	return ctx, nil, nil
}

// serveNavi serves a project's agent the way navi lays it out.
func serveNavi(t *testing.T, project string) string {
	t.Helper()
	handler := a2asrv.NewHandler(chatAgent{}, a2asrv.WithCallInterceptors(loginUser{}))
	var base string
	mux := http.NewServeMux()
	prefix := "/a2a/" + project
	mux.Handle(prefix+"/.well-known/agent-card.json", a2asrv.NewAgentCardHandler(a2asrv.AgentCardProducerFn(func(context.Context) (*a2a.AgentCard, error) {
		return &a2a.AgentCard{
			Name:                "navi " + project,
			Description:         "Chats in " + project,
			Version:             "1",
			SupportedInterfaces: []*a2a.AgentInterface{a2a.NewAgentInterface(base+prefix+"/", a2a.TransportProtocolJSONRPC)},
			Capabilities:        a2a.AgentCapabilities{Streaming: true},
			DefaultInputModes:   []string{"text/plain"},
			DefaultOutputModes:  []string{"text/plain"},
		}, nil
	})))
	mux.Handle("POST "+prefix+"/{$}", a2asrv.NewJSONRPCHandler(handler))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	base = server.URL
	return base
}

// call calls a tool and decodes its structured content into output.
func call(t *testing.T, session *mcp.ClientSession, tool string, arguments, output any) string {
	t.Helper()
	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: tool, Arguments: arguments})
	if err != nil {
		t.Fatalf("%s: %v", tool, err)
	}
	text := result.Content[0].(*mcp.TextContent).Text
	if result.IsError {
		t.Fatalf("%s failed: %s", tool, text)
	}
	encoded, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatalf("%s: encode the structured content: %v", tool, err)
	}
	if err := json.Unmarshal(encoded, output); err != nil {
		t.Fatalf("%s: decode the structured content %s: %v", tool, encoded, err)
	}
	return text
}

// TestChatTools drives chats through the MCP tools against an A2A server
// that maps chats and turns the way navi does.
func TestChatTools(t *testing.T) {
	naviURL := serveNavi(t, "selfdev")
	client, err := navichat.New(navichat.Config{NaviURL: naviURL, Project: "selfdev", Projects: []string{"missing"}, Login: "you@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := navichat.NewMCPServer(client, "test").Connect(t.Context(), serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = serverSession.Close() })
	session, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).Connect(t.Context(), clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })

	tools, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tools.Tools) != 7 {
		t.Errorf("%d tools, want 7", len(tools.Tools))
	}
	for _, tool := range tools.Tools {
		if tool.OutputSchema == nil {
			t.Errorf("%s has no output schema", tool.Name)
		}
	}

	var projects navichat.ProjectsOutput
	call(t, session, "chat_projects", map[string]any{}, &projects)
	if len(projects.Projects) != 2 || !projects.Projects[0].Reachable || !projects.Projects[0].Default || projects.Projects[1].Reachable {
		t.Fatalf("projects: %+v", projects.Projects)
	}
	if !strings.Contains(projects.Projects[1].Error, "has no project") {
		t.Errorf("missing project error: %s", projects.Projects[1].Error)
	}

	// Two chats at once: one finishes, one keeps running.
	var quick, slow navichat.Turn
	call(t, session, "chat_start", map[string]any{"text": "fix the build"}, &quick)
	call(t, session, "chat_start", map[string]any{"text": "slow refactor"}, &slow)
	if quick.Chat == "" || quick.Chat == slow.Chat || quick.Done {
		t.Fatalf("started turns: %+v %+v", quick, slow)
	}

	var waited navichat.Waited
	text := call(t, session, "chat_wait", map[string]any{"turns": []string{slow.ID, quick.ID}, "timeout_seconds": 10}, &waited)
	if waited.TimedOut || waited.Turn.ID != quick.ID || waited.Turn.State != "completed" || waited.Turn.Text != "done: fix the build" {
		t.Fatalf("wait: %+v", waited)
	}
	if len(waited.Others) != 1 || waited.Others[0].ID != slow.ID || waited.Others[0].State != "working" {
		t.Fatalf("other turns: %+v", waited.Others)
	}
	if !strings.Contains(text, "done: fix the build") {
		t.Errorf("wait summary: %s", text)
	}

	// Continue the first chat with a turn that waits on a person.
	var asking navichat.Turn
	call(t, session, "chat_send", map[string]any{"chat": quick.Chat, "text": "ask me"}, &asking)
	if asking.Chat != quick.Chat || asking.ID == quick.ID {
		t.Fatalf("continued turn: %+v", asking)
	}
	text = call(t, session, "chat_wait", map[string]any{"turns": []string{slow.ID, asking.ID}, "timeout_seconds": 10}, &waited)
	if waited.Turn == nil || waited.Turn.ID != asking.ID || !waited.Turn.NeedsInput || waited.Turn.Text != "Which repository?" {
		t.Fatalf("input-required wait: %+v", waited)
	}
	if !strings.Contains(text, "needs input") || !strings.Contains(text, "chat_send") {
		t.Errorf("input-required summary: %s", text)
	}

	var turns navichat.TurnList
	call(t, session, "chat_list", map[string]any{"chat": quick.Chat}, &turns)
	if turns.Total != 2 || len(turns.Turns) != 2 {
		t.Fatalf("turns of the chat: %+v", turns)
	}
	call(t, session, "chat_list", map[string]any{"limit": 1}, &turns)
	if turns.Total != 3 || len(turns.Turns) != 1 || turns.NextPageToken == "" {
		t.Fatalf("one page of turns: %+v", turns)
	}

	var canceled navichat.Turn
	call(t, session, "chat_cancel", map[string]any{"turn": slow.ID}, &canceled)
	if canceled.State != "canceled" || !canceled.Done {
		t.Fatalf("cancel: %+v", canceled)
	}
	var read navichat.Turn
	call(t, session, "chat_get", map[string]any{"turn": slow.ID}, &read)
	if read.State != "canceled" {
		t.Fatalf("canceled turn reads %+v", read)
	}

	// Errors say what to do next.
	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "chat_get", Arguments: map[string]any{"turn": "nope"}})
	if err != nil {
		t.Fatal(err)
	}
	if message := result.Content[0].(*mcp.TextContent).Text; !result.IsError || !strings.Contains(message, "chat_list") {
		t.Errorf("unknown turn: %v %s", result.IsError, message)
	}
	result, err = session.CallTool(t.Context(), &mcp.CallToolParams{Name: "chat_cancel", Arguments: map[string]any{"turn": quick.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if message := result.Content[0].(*mcp.TextContent).Text; !result.IsError || !strings.Contains(message, "already ended") {
		t.Errorf("cancel of an ended turn: %v %s", result.IsError, message)
	}
}
