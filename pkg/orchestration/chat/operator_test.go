package chat_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/kubeop"
	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

func operatorChat(change func(*chat.Request)) chat.State {
	state := newChat("verify")
	state.Request.Repositories = chat.DefaultRepositories
	state.Request.StartedBy = &chat.Person{Login: "eddy@gfxlabs.io"}
	if change != nil {
		change(&state.Request)
	}
	return state
}

func hasTool(fake *session, name string) bool {
	for _, tool := range toolNames(fake.opened[0].Tools) {
		if tool == name {
			return true
		}
	}
	return false
}

// An admin's chat has kube_operator. The call asks the broker with the chat's
// own request, hands the token to the writer beside the core worker, records
// who got it, and never says the token.
func TestOperatorToolIssuesAndRecordsTheGrant(t *testing.T) {
	env, fake := newEnvironment(t)
	var asked kubeop.Subject
	var written kubeop.WriteRequest
	expires := time.Now().Add(time.Hour)
	env.RegisterActivityWithOptions(func(_ context.Context, subject kubeop.Subject) (kubeop.Credentials, error) {
		asked = subject
		return kubeop.Credentials{Token: "secret-token", Expires: expires, Server: "https://10.43.0.1:443", CA: "-----BEGIN CERTIFICATE-----", By: subject.StartedBy, Basis: kubeop.BasisStarted}, nil
	}, activity.RegisterOptions{Name: kubeop.ActivityIssue})
	env.RegisterActivityWithOptions(func(_ context.Context, request kubeop.WriteRequest) (kubeop.WriteResult, error) {
		written = request
		return kubeop.WriteResult{Kubeconfig: "/work/" + kubeop.Dir + "/kubeconfig"}, nil
	}, activity.RegisterOptions{Name: kubeop.ActivityWrite})

	var text string
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(domain.UpdateToolCall, "kube_operator", &testsuite.TestUpdateCallback{
			OnAccept: func() {},
			OnReject: func(err error) { t.Error(err) },
			OnComplete: func(result any, err error) {
				if err != nil {
					t.Error(err)
					return
				}
				text = result.(p7.ToolResult).Content[0].Text
			},
		}, p7.ToolCall{Session: fake.opened[0].ID, Tool: "kube_operator", Arguments: []byte(`{}`)})
	}, time.Minute)
	env.RegisterDelayedCallback(func() {
		state := queryState(t, env)
		if state.Operator == nil || state.Operator.Count != 1 || state.Operator.Last[0].By != "eddy@gfxlabs.io" || state.Operator.Last[0].Basis != kubeop.BasisStarted {
			t.Errorf("the chat recorded %+v", state.Operator)
		}
		env.SignalWorkflow(chat.SignalClose, nil)
	}, 2*time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, operatorChat(nil))
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if !hasTool(fake, "kube_operator") {
		t.Fatalf("the session's tools are %v", toolNames(fake.opened[0].Tools))
	}
	if asked.Chat != "c1" || asked.StartedBy != "eddy@gfxlabs.io" || asked.Restricted || len(asked.Repositories) != len(chat.DefaultRepositories) || asked.Repositories[0] != "gfx-labs/protocol7" {
		t.Errorf("the broker was asked %+v", asked)
	}
	if written.Session != fake.opened[0].ID || written.Credentials.Token != "secret-token" {
		t.Errorf("the writer was asked %+v", written)
	}
	if strings.Contains(text, "secret-token") || !strings.Contains(text, "kubeconfig") || !strings.Contains(text, "scripts/verify-isolation") {
		t.Errorf("the tool said %q", text)
	}
}

// A chat that is not an admin's, or is not in the core project, or works on
// another repository, does not see the tool.
func TestOperatorToolIsOfferedToEligibleChatsOnly(t *testing.T) {
	for name, change := range map[string]func(*chat.Request){
		"restricted": func(r *chat.Request) { r.Restricted = true },
		"project":    func(r *chat.Request) { r.Project = "acme" },
		"no person":  func(r *chat.Request) { r.StartedBy = nil },
		"child":      func(r *chat.Request) { r.Parent = "c0" },
		"other repo": func(r *chat.Request) {
			r.Repositories = []chat.Repository{{Name: "acpc", URL: "git@github.com:gfx-labs/acpc.git"}}
		},
		"local clone":   func(r *chat.Request) { r.Repositories = []chat.Repository{{Name: "repo", URL: "/remote/repo.git"}} },
		"an escalation": func(r *chat.Request) { r.Escalation = &chat.Escalation{Reason: "x"} },
	} {
		env, fake := newEnvironment(t)
		env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, time.Minute)
		env.ExecuteWorkflow(chat.WorkflowName, operatorChat(change))
		if err := env.GetWorkflowError(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if hasTool(fake, "kube_operator") {
			t.Errorf("%s: the session has kube_operator", name)
		}
	}
}
