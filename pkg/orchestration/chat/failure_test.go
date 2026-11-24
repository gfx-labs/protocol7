package chat_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

func TestFailedModelTurnIsVisibleAndTheChatCanRecover(t *testing.T) {
	env, fake := newEnvironment(t)
	reset := time.Date(2026, 11, 25, 5, 0, 0, 0, time.UTC)
	fake.onTurn = func(_ context.Context, request domain.TurnRequest) (domain.TurnResult, error) {
		if request.Turn == "m1" {
			return domain.TurnResult{Turn: request.Turn, StopReason: p7.StopFailed, Failure: &domain.TurnFailure{
				Kind: "rate_limit", Message: "HTTP 429", Account: "claude-1", Model: "sonnet", ResetAt: reset,
			}}, nil
		}
		return domain.TurnResult{Turn: request.Turn, StopReason: p7.StopEndTurn, Text: "recovered"}, nil
	}
	env.RegisterDelayedCallback(func() {
		state := queryState(t, env)
		if state.LastStop != p7.StopFailed || state.Result == nil || state.Result.Error == "" {
			t.Fatalf("failed turn was not reported: %+v", state)
		}
		for _, detail := range []string{"HTTP 429", "rate_limit", "claude-1", "sonnet", reset.Format(time.RFC3339)} {
			if !strings.Contains(state.Error, detail) {
				t.Errorf("error %q is missing %q", state.Error, detail)
			}
		}
		env.SignalWorkflow(chat.SignalMessage, &chat.Message{Id: "m2", Text: "try again"})
	}, time.Minute)
	env.RegisterDelayedCallback(func() {
		state := queryState(t, env)
		if state.Error != "" || state.Result.Error != "" || state.Result.Text != "recovered" || state.LastStop != p7.StopEndTurn {
			t.Errorf("chat did not recover: %+v", state)
		}
		env.SignalWorkflow(chat.SignalClose, nil)
	}, 2*time.Minute)
	env.ExecuteWorkflow(chat.WorkflowName, newChat("first"))
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(fake.turns) != 2 {
		t.Errorf("got %d model turns, want a failure and a person-requested retry", len(fake.turns))
	}
}

func TestFailedChildReturnsAnErrorReportEvenWithoutHarnessDiagnostics(t *testing.T) {
	for _, failure := range []*domain.TurnFailure{nil, {}} {
		t.Run(fmt.Sprintf("diagnostics-%v", failure != nil), func(t *testing.T) {
			env, fake := newEnvironment(t)
			fake.onTurn = func(_ context.Context, request domain.TurnRequest) (domain.TurnResult, error) {
				return domain.TurnResult{Turn: request.Turn, StopReason: p7.StopFailed, Failure: failure}, nil
			}
			env.OnSignalExternalWorkflow(mock.Anything, "parent", "", chat.SignalReport, mock.Anything).Return(nil)
			state := newChat("first")
			state.Request.Parent, state.Request.Owner = "parent", "parent"
			env.ExecuteWorkflow(chat.WorkflowName, state)
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			var result chat.Report
			if err := env.GetWorkflowResult(&result); err != nil {
				t.Fatal(err)
			}
			if result.Error == "" || result.StopReason != p7.StopFailed || !result.Ended {
				t.Errorf("failed child returned a successful report: %+v", &result)
			}
		})
	}
}
