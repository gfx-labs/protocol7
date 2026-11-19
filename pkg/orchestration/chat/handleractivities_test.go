package chat_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/temporal"

	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
)

type stateValue struct{ value any }

func (v stateValue) HasValue() bool { return true }
func (v stateValue) Get(into any) error {
	switch target := into.(type) {
	case *chat.State:
		*target = v.value.(chat.State)
	case *chat.EscalationState:
		*target = v.value.(chat.EscalationState)
	}
	return nil
}

// The hand-off signals the handler workflow, with the handler's own model
// choice, and starts it when it does not run.
func TestTheHandoffSignalsAndStartsTheHandler(t *testing.T) {
	temporalClient := &mocks.Client{}
	var options client.StartWorkflowOptions
	var intake chat.HandlerIntake
	temporalClient.On("SignalWithStartWorkflow", mock.Anything, chat.HandlerWorkflowID, chat.SignalEscalationIntake, mock.Anything, mock.Anything, chat.HandlerWorkflowName, mock.Anything).
		Return(nil, nil).Run(func(args mock.Arguments) {
		intake = args.Get(3).(chat.HandlerIntake)
		options = args.Get(4).(client.StartWorkflowOptions)
	})
	activities := &chat.HandlerActivities{Client: temporalClient, Config: chat.HandlerConfig{Model: "flash", Models: []string{"deepseek/flash"}}}
	require.NoError(t, activities.EscalationHandoff(context.Background(), chat.EscalationRequest{ID: "e1", Chat: "c1"}))
	assert.Equal(t, chat.HandlerWorkflowID, options.ID)
	assert.Equal(t, chat.TaskQueue, options.TaskQueue)
	assert.Equal(t, "e1", intake.Request.ID)
	assert.Equal(t, []string{"deepseek/flash"}, intake.Models)
	assert.Equal(t, "flash", intake.Model)
}

// The brief is what the chat's workflow keeps: its work, last turn and error, and
// its other escalations. It is bounded.
func TestTheBriefIsBounded(t *testing.T) {
	temporalClient := &mocks.Client{}
	state := chat.State{
		Request: chat.Request{ID: "c1", Title: "Work on 79"}, Status: chat.StatusEscalated, Turns: 3,
		Current:     &chat.Message{ID: "m", Text: "implement the handler"},
		Result:      chat.Report{Text: string(make([]byte, 20000))},
		Error:       "the push failed",
		Escalations: []chat.EscalationRef{{ID: "e1", Title: "this one", Status: chat.EscalationOpen}, {ID: "e0", Title: "earlier", Status: chat.EscalationResolved}},
	}
	temporalClient.On("QueryWorkflow", mock.Anything, "chat-c1", "", chat.QueryState).Return(stateValue{state}, nil)
	brief, err := (&chat.HandlerActivities{Client: temporalClient}).EscalationBrief(context.Background(), chat.EscalationRequest{ID: "e1", Chat: "c1"})
	require.NoError(t, err)
	assert.Contains(t, brief, "implement the handler")
	assert.Contains(t, brief, "the push failed")
	assert.Contains(t, brief, "e0")
	assert.NotContains(t, brief, `"this one"`, "the escalation itself is not listed among the others")
	assert.Less(t, len(brief), 8000)

	temporalClient = &mocks.Client{}
	temporalClient.On("QueryWorkflow", mock.Anything, "chat-c1", "", chat.QueryState).Return(nil, errors.New("not found"))
	_, err = (&chat.HandlerActivities{Client: temporalClient}).EscalationBrief(context.Background(), chat.EscalationRequest{ID: "e1", Chat: "c1"})
	assert.Error(t, err)
}

// The escalation's refusal of a decision is final: the activity does not retry it.
func TestADecisionTheEscalationRefusesIsFinal(t *testing.T) {
	temporalClient := &mocks.Client{}
	handle := &mocks.WorkflowUpdateHandle{}
	handle.On("Get", mock.Anything, mock.Anything).Return(temporal.NewApplicationError("this escalation is resolved already", ""))
	var options client.UpdateWorkflowOptions
	temporalClient.On("UpdateWorkflow", mock.Anything, mock.Anything).Return(handle, nil).Run(func(args mock.Arguments) { options = args.Get(1).(client.UpdateWorkflowOptions) })
	_, err := (&chat.HandlerActivities{Client: temporalClient}).EscalationDecide(context.Background(), chat.DecideRequest{Escalation: "e1", Decision: chat.EscalationDecision{Outcome: chat.OutcomeForwarded}})
	require.Error(t, err)
	var application *temporal.ApplicationError
	require.True(t, errors.As(err, &application))
	assert.True(t, application.NonRetryable())
	assert.Equal(t, chat.EscalationWorkflowID("e1"), options.WorkflowID)
	assert.Equal(t, chat.UpdateEscalationDecide, options.UpdateName)
}
