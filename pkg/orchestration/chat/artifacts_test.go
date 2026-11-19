package chat_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/artifact"
	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

func toolNames(specs []domain.ToolSpec) []string {
	var names []string
	for _, spec := range specs {
		names = append(names, spec.Name)
	}
	return names
}

// The agent saves, lists and gets artifacts. Each call is an activity with
// the session, the project and the arguments, and the bytes are not in it.
func TestTheAgentUsesArtifacts(t *testing.T) {
	env, fake := newEnvironment(t)
	var saved artifact.SaveRequest
	var got artifact.GetRequest
	var listed artifact.ListRequest
	digest := strings.Repeat("ab", 32)
	env.RegisterActivityWithOptions(func(_ context.Context, request artifact.SaveRequest) (artifact.SaveResult, error) {
		saved = request
		return artifact.SaveResult{Ref: "p7://sha256:" + digest + "#" + request.Name, Digest: digest, Size: 2048}, nil
	}, activity.RegisterOptions{Name: artifact.ActivitySave})
	env.RegisterActivityWithOptions(func(_ context.Context, request artifact.GetRequest) (artifact.GetResult, error) {
		got = request
		return artifact.GetResult{Ref: request.Ref, Path: request.Path, Files: 3, Size: 100, Dir: true}, nil
	}, activity.RegisterOptions{Name: artifact.ActivityGet})
	env.RegisterActivityWithOptions(func(_ context.Context, request artifact.ListRequest) (artifact.ListResult, error) {
		listed = request
		return artifact.ListResult{Entries: []artifact.Entry{{Name: "ui/a.png", Version: artifact.Version{Digest: digest, Size: 10, Saved: time.Now().Add(-3 * time.Hour)}, Versions: 2, Kept: "newest"}}}, nil
	}, activity.RegisterOptions{Name: artifact.ActivityList})

	results := map[string]p7.ToolResult{}
	call := func(tool, arguments string) {
		env.UpdateWorkflow(domain.UpdateToolCall, tool, &testsuite.TestUpdateCallback{
			OnAccept: func() {},
			OnReject: func(err error) { t.Errorf("%s: %v", tool, err) },
			OnComplete: func(result any, err error) {
				if err != nil {
					t.Errorf("%s: %v", tool, err)
					return
				}
				results[tool] = result.(p7.ToolResult)
			},
		}, p7.ToolCall{Session: fake.opened[0].ID, Tool: tool, Arguments: []byte(arguments)})
	}
	env.RegisterDelayedCallback(func() {
		call("artifact_save", `{"path":"repo/shot.png","name":"ui/a.png"}`)
		call("artifact_get", `{"ref":"p7://sha256:`+digest+`","path":"ref"}`)
		call("artifact_list", `{"prefix":"ui/"}`)
	}, time.Minute)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, 2*time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("first"))
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}

	if names := toolNames(fake.opened[0].Tools); !strings.Contains(strings.Join(names, ","), "artifact_save,artifact_get,artifact_list") {
		t.Errorf("the session's tools are %v", names)
	}
	if want := (artifact.SaveRequest{Session: fake.opened[0].ID, Project: "selfdev", Path: "repo/shot.png", Name: "ui/a.png"}); saved != want {
		t.Errorf("save asked %+v, want %+v", saved, want)
	}
	if got.Session != fake.opened[0].ID || got.Project != "selfdev" || got.Path != "ref" || got.Ref != "p7://sha256:"+digest {
		t.Errorf("get asked %+v", got)
	}
	if listed != (artifact.ListRequest{Project: "selfdev", Prefix: "ui/"}) {
		t.Errorf("list asked %+v", listed)
	}
	if text := results["artifact_save"].Content[0].Text; !strings.Contains(text, "p7://sha256:"+digest+"#ui/a.png") || !strings.Contains(text, "2.0 KiB") {
		t.Errorf("save said %q", text)
	}
	if text := results["artifact_get"].Content[0].Text; !strings.Contains(text, "3 files") {
		t.Errorf("get said %q", text)
	}
	if text := results["artifact_list"].Content[0].Text; !strings.Contains(text, "ui/a.png") || !strings.Contains(text, "3h old") || !strings.Contains(text, "(2 versions)") {
		t.Errorf("list said %q", text)
	}
}

// Other projects have no artifact tools: their namespaces have no process
// that serves them. (The host and backend conditions are in
// artifacts_internal_test.go.)
func TestNoArtifactToolsInOtherProjects(t *testing.T) {
	for name, change := range map[string]func(*chat.Request){
		"project": func(r *chat.Request) { r.Project = "acme" },
	} {
		env, fake := newEnvironment(t)
		env.RegisterDelayedCallback(func() { env.SignalWorkflow(chat.SignalClose, nil) }, time.Minute)
		state := newChat("first")
		change(&state.Request)
		env.ExecuteWorkflow(chat.WorkflowName, state)
		if err := env.GetWorkflowError(); err != nil {
			t.Fatal(err)
		}
		for _, tool := range toolNames(fake.opened[0].Tools) {
			if strings.HasPrefix(tool, "artifact_") {
				t.Errorf("%s: the session has %s", name, tool)
			}
		}
	}
}
