package chat_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
)

// asker is a workflow that is no chat. It raises an escalation, waits for the
// escalation's notices and returns the last one.
func asker(ctx workflow.Context, request *chat.EscalationRequest) (*chat.EscalationNotice, error) {
	id, err := chat.RaiseEscalation(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := workflow.SetQueryHandler(ctx, "escalation", func() (string, error) { return id, nil }); err != nil {
		return nil, err
	}
	notices := workflow.GetSignalChannel(ctx, chat.SignalEscalation)
	var notice chat.EscalationNotice
	for notice.Status != chat.EscalationResolved {
		notices.Receive(ctx, &notice)
	}
	return &notice, nil
}

// A workflow that is no chat raises a person-only escalation. The handler
// never gets it, it refuses the handler's decision and a resolution that is not
// a named person's, and a person's answer is signalled to the workflow.
func TestAWorkflowEscalatesToAPersonOnly(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var handoffs atomic.Int32
	err := chat.Register(registry{env}, chat.Options{
		EscalationHandoff: "Handoff",
		Activities: map[string]any{
			"Handoff": func(context.Context, *chat.EscalationRequest) error { handoffs.Add(1); return nil },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	env.RegisterWorkflow(asker)
	env.OnGetVersion("chat-model-limit", workflow.DefaultVersion, 2).Return(workflow.DefaultVersion)

	var forwarded *chat.EscalationState
	var rejections []string
	env.RegisterDelayedCallback(func() {
		var raised string
		value, err := env.QueryWorkflow("escalation")
		if err != nil {
			t.Error(err)
			return
		}
		if err := value.Get(&raised); err != nil {
			t.Error(err)
			return
		}
		id := chat.EscalationWorkflowID(raised)
		value, err = env.QueryWorkflowByID(id, chat.QueryEscalation)
		if err != nil {
			t.Error(err)
			return
		}
		if err := value.Get(&forwarded); err != nil {
			t.Error(err)
		}
		reject := func(name string, update string, argument any) {
			err := env.UpdateWorkflowByID(id, update, name, &testsuite.TestUpdateCallback{
				OnAccept:   func() {},
				OnReject:   func(err error) { rejections = append(rejections, err.Error()) },
				OnComplete: func(any, error) { t.Errorf("%s was accepted", name) },
			}, argument)
			if err != nil {
				t.Error(err)
			}
		}
		reject("decide", chat.UpdateEscalationDecide, &chat.EscalationDecision{Outcome: chat.OutcomeResolved, Text: "yes", Reasoning: "the handler thinks so"})
		reject("handler answer", chat.UpdateEscalationResolve, &chat.EscalationAnswer{Text: "yes", By: &chat.Person{Login: chat.HandlerLogin}})
		reject("anonymous answer", chat.UpdateEscalationResolve, &chat.EscalationAnswer{Text: "yes"})
		err = env.UpdateWorkflowByID(id, chat.UpdateEscalationResolve, "resolve", &testsuite.TestUpdateCallback{
			OnAccept: func() {},
			OnReject: func(err error) { t.Errorf("a person's answer was rejected: %v", err) },
			OnComplete: func(_ any, err error) {
				if err != nil {
					t.Errorf("resolve: %v", err)
				}
			},
		}, &chat.EscalationAnswer{Text: "ask towa too", By: &chat.Person{Login: "eddy@example.com"}})
		if err != nil {
			t.Error(err)
		}
	}, time.Minute)

	env.ExecuteWorkflow(asker, &chat.EscalationRequest{Need: "Which repositories does selfdev build in", Why: "No guidance covers towa"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var notice *chat.EscalationNotice
	if err := env.GetWorkflowResult(&notice); err != nil {
		t.Fatal(err)
	}
	if notice.Status != chat.EscalationResolved || notice.Answer.GetText() != "ask towa too" || notice.Answer.GetBy().GetLogin() != "eddy@example.com" {
		t.Errorf("notice %+v, want the person's answer", notice)
	}
	if forwarded == nil || forwarded.Status != chat.EscalationForwarded || !forwarded.Request.PersonOnly {
		t.Errorf("escalation %+v, want person-only and forwarded to a person at once", forwarded)
	}
	if forwarded != nil && forwarded.Request.Chat != "" {
		t.Errorf("escalation of chat %q, want none", forwarded.Request.Chat)
	}
	if handoffs.Load() != 0 {
		t.Errorf("the handler got %d escalations, want none", handoffs.Load())
	}
	if len(rejections) != 3 {
		t.Errorf("rejections %q, want the handler's decision, the handler's answer and an anonymous one", rejections)
	}
	if forwarded != nil && forwarded.Request.Notify != "default-test-workflow-id" {
		t.Errorf("escalation tells %q, want the asking workflow", forwarded.Request.Notify)
	}
}
