package session_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/session"
)

// toolCaller is a ToolCaller that cannot send permission requests.
type toolCaller struct{}

func (toolCaller) CallTool(context.Context, string, string, domain.ToolCall) (domain.ToolResult, error) {
	return domain.ToolResult{}, errors.New("no tools in this test")
}

// permissionCaller also decides permission requests, with decide, and keeps
// what it was asked.
type permissionCaller struct {
	toolCaller
	decide func(domain.PermissionRequest) (domain.PermissionDecision, error)
	asked  []domain.PermissionRequest
	owners []string
}

func (p *permissionCaller) CallPermission(_ context.Context, owner, _ string, request domain.PermissionRequest) (domain.PermissionDecision, error) {
	p.asked = append(p.asked, request)
	p.owners = append(p.owners, owner)
	return p.decide(request)
}

// permissionAnswer opens session "s1" with the request on a manager whose
// tool caller is caller, has the scripted agent ask to run command, and
// returns what the agent says it was answered. A nil caller is a manager
// with none.
func permissionAnswer(t *testing.T, caller session.ToolCaller, request domain.SessionRequest, command string) string {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp("", "p7")
	if err != nil {
		t.Fatal(err)
	}
	manager, err := session.NewManager(session.Config{
		Root: root, ShimBinary: binary, Tools: caller,
		Harness: session.Harness{Command: binary, Env: []string{agentLogEnv + "=" + root + "/agent.log"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.Close(context.Background(), domain.CloseRequest{Session: "s1"}); err != nil {
			t.Errorf("close: %v", err)
		}
		_ = os.RemoveAll(root)
	})
	request.ID = "s1"
	if _, err := manager.Open(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	result, err := manager.RunTurn(t.Context(), domain.TurnRequest{Session: "s1", Turn: "t1", Prompt: "ask " + command}, session.TurnControl{})
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(result.Text)
}

// The owner's decision picks the agent's allow or reject option, and a
// decision that cannot be had is a denial, never an allow.
func TestPermissionRequestsAreDecidedByTheOwnerAndDeniedWhenItCannotBeAsked(t *testing.T) {
	owned := domain.SessionRequest{Owner: "workflow", Permissions: true}
	decide := func(allow bool, err error) *permissionCaller {
		return &permissionCaller{decide: func(domain.PermissionRequest) (domain.PermissionDecision, error) {
			return domain.PermissionDecision{Allow: allow}, err
		}}
	}
	for _, test := range []struct {
		name    string
		caller  session.ToolCaller
		request domain.SessionRequest
		command string
		want    string
	}{
		{name: "allowed by the owner", caller: decide(true, nil), request: owned, command: "ls", want: "permission allow"},
		{name: "denied by the owner", caller: decide(false, nil), request: owned, command: "rm", want: "permission reject"},
		{name: "owner cannot be reached", caller: decide(true, errors.New("no such workflow")), request: owned, command: "ls", want: "permission reject"},
		{name: "denied and no reject option", caller: decide(false, nil), request: owned, command: "allow-only", want: "permission cancelled"},
		{name: "no tool caller on the manager", caller: nil, request: owned, command: "ls", want: "permission reject"},
		{name: "a caller that cannot ask permissions", caller: toolCaller{}, request: owned, command: "ls", want: "permission reject"},
		{name: "no owner", caller: decide(true, nil), request: domain.SessionRequest{Permissions: true}, command: "ls", want: "permission reject"},
		{name: "permissions not asked for", caller: decide(false, nil), request: domain.SessionRequest{Owner: "workflow"}, command: "rm", want: "permission allow"},
		{name: "permissions not asked for, no caller", caller: nil, request: domain.SessionRequest{}, command: "rm", want: "permission allow"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := permissionAnswer(t, test.caller, test.request, test.command); got != test.want {
				t.Errorf("the agent was answered %q, want %q", got, test.want)
			}
		})
	}

	// What the owner is asked.
	caller := decide(true, nil)
	permissionAnswer(t, caller, owned, "ls")
	if len(caller.asked) != 1 || caller.owners[0] != "workflow" ||
		caller.asked[0].Session != "s1" || caller.asked[0].ToolCallID != "call-1" || caller.asked[0].Title != "ls" || len(caller.asked[0].Options) != 2 {
		t.Errorf("the owner was asked %+v of %q, want one request for tool call call-1 in session s1", caller.asked, caller.owners)
	}
}
