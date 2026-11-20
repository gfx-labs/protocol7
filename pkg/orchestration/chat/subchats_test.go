package chat_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/domain/navi"
	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

func subchatEnvironment(t *testing.T) (*testsuite.TestWorkflowEnvironment, *session) {
	t.Helper()
	env, sessions := newEnvironment(t)
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: chat.WorkflowID("c1")})
	env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, "", mock.Anything, mock.Anything).Return(
		func(_, id, _, signal string, value any) error {
			return env.SignalWorkflowByID(id, signal, value)
		})
	return env, sessions
}

func spawnUpdate(t *testing.T, env *testsuite.TestWorkflowEnvironment, parent, task string, complete func(string, error)) {
	t.Helper()
	toolCalls++
	err := env.UpdateWorkflowByID(chat.WorkflowID(parent), chat.UpdateSpawn, fmt.Sprintf("spawn-%d", toolCalls), &testsuite.TestUpdateCallback{
		OnAccept: func() {},
		OnReject: func(err error) { complete("", err) },
		OnComplete: func(value any, err error) {
			id, _ := value.(string)
			complete(id, err)
		},
	}, &navi.Spawn{Task: task, Title: "child task", By: &navi.Person{Login: "alice@example.com"}, Restricted: true})
	if err != nil {
		t.Error(err)
	}
}

func childState(t *testing.T, env *testsuite.TestWorkflowEnvironment, id string) *navi.ChatState {
	t.Helper()
	value, err := env.QueryWorkflowByID(chat.WorkflowID(id), chat.QueryState)
	if err != nil {
		t.Fatal(err)
	}
	var state *navi.ChatState
	if err := value.Get(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func stopSlowChild(t *testing.T, env *testsuite.TestWorkflowEnvironment, sessions *session) func(context.Context, domain.TurnRequest) (domain.TurnResult, error) {
	t.Helper()
	return func(ctx context.Context, turn domain.TurnRequest) (domain.TurnResult, error) {
		if turn.Prompt != "slow" {
			return domain.TurnResult{Turn: turn.Turn, Text: "answer to " + turn.Prompt, StopReason: p7.StopEndTurn}, nil
		}
		id := activity.GetInfo(ctx).WorkflowExecution.ID
		env.RegisterDelayedCallback(func() {
			if err := env.SignalWorkflowByID(id, chat.SignalStop, &chat.Action{By: &chat.Person{Login: "alice@example.com"}}); err != nil {
				t.Error(err)
			}
		}, 0)
		<-sessions.release
		return domain.TurnResult{Turn: turn.Turn, StopReason: p7.StopCancelled}, nil
	}
}

func TestChildResultAutomaticallyWakesItsParent(t *testing.T) {
	env, sessions := subchatEnvironment(t)
	var child string
	env.RegisterDelayedCallback(func() {
		spawnUpdate(t, env, "c1", "inspect the change", func(id string, err error) {
			if err != nil {
				t.Error(err)
			}
			child = id
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		state := queryState(t, env)
		if child == "" || len(state.Children) != 1 || !state.Children[0].Done || state.Turns != 2 {
			t.Errorf("parent did not consume child result without awaiting: %v", state)
		}
		request := childState(t, env, child).Request
		if request.Parent != "c1" || request.Depth != 1 || request.Project != state.Request.Project || !request.Restricted || request.StartedBy.GetLogin() != "alice@example.com" {
			t.Errorf("child did not inherit project and restriction or manual caller: %v", request)
		}
		env.SignalWorkflow(chat.SignalClose, &chat.Action{})
	}, time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("first"))
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(sessions.turns) != 3 || !strings.Contains(sessions.turns[2].Prompt, "answer to inspect the change") {
		t.Errorf("turns %v, want root, child and automatic result turn", sessions.turns)
	}
}

func TestStoppedChildWaitsForGuidanceAndWakesParent(t *testing.T) {
	env, sessions := subchatEnvironment(t)
	var child string
	sessions.onTurn = stopSlowChild(t, env, sessions)
	env.RegisterDelayedCallback(func() {
		spawnUpdate(t, env, "c1", "slow", func(id string, err error) {
			if err != nil {
				t.Error(err)
			}
			child = id
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		state := childState(t, env, child)
		parent := queryState(t, env)
		if state.Closing || !state.Attention || !state.Result.GetStopped() || state.Status != chat.StatusIdle {
			t.Errorf("stopped child did not remain idle for guidance: %v", state)
		}
		if parent.Turns != 2 || parent.Children[0].Done || !parent.Children[0].Attention {
			t.Errorf("stop did not wake parent with live attention child: %v", parent)
		}
		if err := env.SignalWorkflowByID(chat.WorkflowID(child), chat.SignalMessage, &chat.Message{Id: "guidance", Text: "continue with this guidance", From: &chat.Person{Login: "alice@example.com"}}); err != nil {
			t.Error(err)
		}
	}, time.Minute)
	env.RegisterDelayedCallback(func() {
		state := queryState(t, env)
		if !state.Children[0].Done || state.Children[0].Result.GetStopped() || state.Turns != 3 {
			t.Errorf("guided child did not complete and wake parent: %v", state)
		}
		env.SignalWorkflow(chat.SignalClose, &chat.Action{})
	}, 2*time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("first"))
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}

func TestSpawnEnforcesChildBounds(t *testing.T) {
	for _, limit := range []string{"live", "lifetime", "depth", "handler", "empty task", "large task"} {
		t.Run(limit, func(t *testing.T) {
			env, _ := subchatEnvironment(t)
			state := newChat("first")
			task := "work"
			switch limit {
			case "live", "lifetime":
				count := chat.MaxLiveChildren
				if limit == "lifetime" {
					count = chat.MaxChildren
				}
				for index := range count {
					state.Children = append(state.Children, &chat.Child{Id: fmt.Sprintf("old-%d", index), Done: limit == "lifetime"})
				}
			case "depth":
				state.Request.Depth = chat.MaxChildDepth
			case "handler":
				state.Request.Handler = true
			case "empty task":
				task = " "
			case "large task":
				task = strings.Repeat("x", (64<<10)+1)
			}
			refused := false
			env.RegisterDelayedCallback(func() {
				spawnUpdate(t, env, "c1", task, func(id string, err error) {
					refused = err != nil && id == ""
					env.SignalWorkflow(chat.SignalClose, &chat.Action{})
				})
			}, time.Second)
			env.ExecuteWorkflow(chat.WorkflowName, state)
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			if !refused {
				t.Fatal("spawn was not refused at its bound")
			}
		})
	}
}

func TestContinueAsNewWaitsForLiveChildren(t *testing.T) {
	env, sessions := subchatEnvironment(t)
	var child string
	sessions.onTurn = stopSlowChild(t, env, sessions)
	env.RegisterDelayedCallback(func() {
		spawnUpdate(t, env, "c1", "slow", func(id string, err error) {
			if err != nil {
				t.Error(err)
			}
			child = id
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() { env.SetContinueAsNewSuggested(true) }, time.Minute)
	env.RegisterDelayedCallback(func() {
		state := queryState(t, env)
		if len(state.Children) != 1 || state.Children[0].Done {
			t.Errorf("child not live while rollover was suggested: %v", state)
		}
		spawnUpdate(t, env, "c1", "another task", func(id string, err error) {
			if err == nil || id != "" {
				t.Error("spawn admitted while parent was draining")
			}
		})
		if err := env.SignalWorkflowByID(chat.WorkflowID(child), chat.SignalClose, &chat.Action{}); err != nil {
			t.Error(err)
		}
	}, 2*time.Hour)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("first"))
	var continued *workflow.ContinueAsNewError
	if err := env.GetWorkflowError(); !errors.As(err, &continued) {
		t.Fatalf("ended with %v, want continue-as-new after child closed", err)
	}
	var next *navi.ChatState
	if err := converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next); err != nil {
		t.Fatal(err)
	}
	if len(next.Children) != 1 || !next.Children[0].Done || next.Children[0].Result == nil || next.Session == nil {
		t.Errorf("rollover lost completed child result or its session: %v", next)
	}
}

func TestCloseCascadesToLiveGrandchildren(t *testing.T) {
	env, sessions := subchatEnvironment(t)
	var child, grandchild string
	sessions.onTurn = func(ctx context.Context, turn domain.TurnRequest) (domain.TurnResult, error) {
		switch turn.Prompt {
		case "delegate again":
			parent := strings.TrimPrefix(activity.GetInfo(ctx).WorkflowExecution.ID, chat.WorkflowID(""))
			started := make(chan struct{})
			env.RegisterDelayedCallback(func() {
				spawnUpdate(t, env, parent, "wait for guidance", func(id string, err error) {
					if err != nil {
						t.Error(err)
					}
					grandchild = id
					close(started)
				})
			}, 0)
			<-started
		case "wait for guidance":
			return domain.TurnResult{Turn: turn.Turn, StopReason: p7.StopCancelled}, nil
		}
		return domain.TurnResult{Turn: turn.Turn, Text: "delegated work", StopReason: p7.StopEndTurn}, nil
	}
	env.RegisterDelayedCallback(func() {
		spawnUpdate(t, env, "c1", "delegate again", func(id string, err error) {
			if err != nil {
				t.Error(err)
			}
			child = id
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		state := childState(t, env, child)
		if state.Closing || state.Attention || len(state.Children) != 1 || state.Children[0].Done || grandchild == "" {
			t.Errorf("child auto-closed or lost its live grandchild: %v", state)
		}
		env.SignalWorkflow(chat.SignalClose, &chat.Action{})
	}, time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("first"))
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{child, grandchild} {
		if state := childState(t, env, id); state.Status != chat.StatusClosed {
			t.Errorf("descendant %s did not close: %v", id, state)
		}
	}
	if sessions.closed != 3 || !queryState(t, env).Children[0].Done {
		t.Errorf("parent returned before descendants settled, sessions closed %d", sessions.closed)
	}
}
