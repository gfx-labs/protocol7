package chat_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

func toolCall(t *testing.T, env *testsuite.TestWorkflowEnvironment, session, tool string, args any, complete func(p7.ToolResult)) {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	env.UpdateWorkflow(domain.UpdateToolCall, "", &testsuite.TestUpdateCallback{
		OnAccept: func() {},
		OnReject: func(err error) { t.Errorf("%s rejected: %v", tool, err) },
		OnComplete: func(value any, err error) {
			if err != nil {
				t.Errorf("%s: %v", tool, err)
				return
			}
			complete(value.(p7.ToolResult))
		},
	}, p7.ToolCall{Session: session, Tool: tool, Arguments: raw})
}

// Native child futures provide terminal summaries, and child inputs inherit
// project/model/restrictions rather than accepting authority from tool arguments.
func TestSpawnTwoChildrenAndAwaitTheirResults(t *testing.T) {
	env, fake := newEnvironment(t)
	var started []chat.Request
	env.SetOnChildWorkflowStartedListener(func(_ *workflow.Info, _ workflow.Context, args converter.EncodedValues) {
		var state chat.State
		if err := args.Get(&state); err != nil {
			t.Error(err)
		}
		started = append(started, state.Request)
	})
	var results []chat.Child
	fake.onSlow = func() {
		fake.mu.Lock()
		session := fake.opened[0].ID
		fake.mu.Unlock()
		count := 0
		for _, task := range []string{"first child result", "second child result"} {
			toolCall(t, env, session, "chat_spawn", map[string]any{"task": task, "title": task, "project": "forbidden", "restricted": false}, func(result p7.ToolResult) {
				if result.IsError {
					t.Errorf("spawn: %+v", result)
					return
				}
				count++
				if count != 2 {
					return
				}
				toolCall(t, env, session, "chat_await", map[string]any{}, func(result p7.ToolResult) {
					if result.IsError {
						t.Errorf("await: %+v", result)
						return
					}
					if err := json.Unmarshal([]byte(result.Content[0].Text), &results); err != nil {
						t.Error(err)
					}
					env.SignalWorkflow(chat.SignalClose, nil)
				})
			})
		}
	}
	state := newChat("slow")
	state.Request.Project = "tenant"
	state.Request.Restricted = true
	state.Request.Models = []string{"model:shared"}
	env.ExecuteWorkflow(chat.WorkflowName, state)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(started) != 2 || len(results) != 2 {
		t.Fatalf("started %d children, received %+v", len(started), results)
	}
	for _, request := range started {
		if request.Project != "tenant" || !request.Restricted || request.Parent != state.Request.ID || request.Depth != 1 || strings.Join(request.Models, ",") != "model:shared" {
			t.Errorf("child authority not inherited: %+v", request)
		}
	}
	for _, result := range results {
		if !result.Done || result.Result.Error != "" || result.Result.Text != result.Title {
			t.Errorf("child summary: %+v", result)
		}
	}
	if len(fake.opened) != 3 || fake.closed != 3 {
		t.Errorf("session lifecycle opened %d closed %d", len(fake.opened), fake.closed)
	}
}

func TestSpawnBoundsAndUnrelatedMessageAreRefused(t *testing.T) {
	for _, bound := range []string{"depth", "live", "total"} {
		t.Run(bound, func(t *testing.T) {
			env, fake := newEnvironment(t)
			state := newChat("parent")
			switch bound {
			case "depth":
				state.Request.Depth = chat.MaxChildDepth
			case "live":
				for i := 0; i < chat.MaxLiveChildren; i++ {
					state.Children = append(state.Children, chat.Child{ID: fmt.Sprint(i)})
				}
			case "total":
				for i := 0; i < chat.MaxChildren; i++ {
					state.Children = append(state.Children, chat.Child{ID: fmt.Sprint(i), Done: true})
				}
			}
			env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, "", chat.SignalClose, mock.Anything).Return(nil)
			rejected := 0
			env.RegisterDelayedCallback(func() {
				session := fake.opened[0].ID
				toolCall(t, env, session, "chat_spawn", map[string]string{"task": "must not start"}, func(result p7.ToolResult) {
					if !result.IsError {
						t.Error("spawn exceeded bound")
					}
					rejected++
				})
				toolCall(t, env, session, "chat_message", map[string]string{"id": "unrelated", "message": "must not send"}, func(result p7.ToolResult) {
					if !result.IsError {
						t.Error("messaged unrelated chat")
					}
					rejected++
					env.SignalWorkflow(chat.SignalClose, nil)
				})
			}, time.Minute)
			env.ExecuteWorkflow(chat.WorkflowName, state)
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			if rejected != 2 || len(fake.opened) != 1 {
				t.Fatalf("bound failed: rejections=%d opens=%d", rejected, len(fake.opened))
			}
		})
	}
}

func TestChildAttentionWaitsForGuidance(t *testing.T) {
	env, fake := newEnvironment(t)
	var reports []chat.Report
	env.OnSignalExternalWorkflow(mock.Anything, "parent", "", chat.SignalReport, mock.Anything).Return(nil).Run(func(args mock.Arguments) { reports = append(reports, args.Get(4).(chat.Report)) })
	fake.onSlow = func() {
		fake.mu.Lock()
		session := fake.opened[0].ID
		fake.mu.Unlock()
		toolCall(t, env, session, "chat_attention", map[string]any{}, func(result p7.ToolResult) {
			if result.IsError {
				t.Error(result)
			}
			close(fake.release)
		})
	}
	state := newChat("slow")
	state.Request.Parent = "parent"
	state.Request.Owner = "parent"
	env.RegisterDelayedCallback(func() {
		current := queryState(t, env)
		if current.Status != chat.StatusIdle || !current.Attention {
			t.Errorf("child did not wait for attention: %+v", current)
		}
		env.SignalWorkflow(chat.SignalMessage, chat.Message{ID: "guidance", Text: "resolved"})
	}, time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, state)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result chat.Report
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if len(reports) < 2 || !reports[0].Attention || !result.Ended || result.Text != "resolved" {
		t.Errorf("attention lifecycle reports=%+v result=%+v", reports, result)
	}
}
