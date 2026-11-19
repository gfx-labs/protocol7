package chat

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/p7"
)

// escalationExpiry bounds how long an escalation may wait for an answer.
const escalationExpiry = 14 * 24 * time.Hour

// escalationTools give every chat the escalate tool.
func escalationTools(state *State) []p7.Tool {
	return []p7.Tool{{
		Name: "escalate",
		Description: "Ask a person for something outside this chat's reach, instead of ending your turn and waiting. Say what you need, why you cannot do it yourself, what you already tried, and what you will do once you have it. " +
			"The request goes to the escalation handler and then to a person in navi's Escalations list. It returns the escalation's ID at once: end your turn after calling it. " +
			"The answer arrives in this chat as a message, and the chat waits without using a model slot until then. Never use it for something you can do or find out yourself.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"need":{"type":"string","maxLength":60000,"description":"What is needed."},"why":{"type":"string","maxLength":60000,"description":"Why this chat cannot do it or get it itself."},"tried":{"type":"string","maxLength":60000,"description":"What it already tried."},"next":{"type":"string","maxLength":60000,"description":"What it will do once it is unblocked."}},"required":["need","why"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
			var args struct {
				Need  string `json:"need"`
				Why   string `json:"why"`
				Tried string `json:"tried"`
				Next  string `json:"next"`
			}
			if err := json.Unmarshal(call.Arguments, &args); err != nil {
				return p7.ToolResult{}, err
			}
			id, err := raiseEscalation(ctx, state, EscalationRequest{Need: args.Need, Why: args.Why, Tried: args.Tried, Next: args.Next})
			if err != nil {
				return p7.ToolResult{}, err
			}
			return p7.TextResult(EscalatedText(id)), nil
		},
	}}
}

// EscalatedText is what the agent is told after it escalated.
func EscalatedText(id string) string {
	return fmt.Sprintf("Escalation %s is raised and a person will see it. End your turn now. The answer arrives in this chat as a message, and you go on from it. Do not poll or wait for it.", id)
}

// raiseEscalation starts the escalation's workflow for the chat and returns
// its ID. The escalation is a workflow of its own: it outlives a turn, and a
// continue-as-new of the chat, and tells the chat by signal.
func raiseEscalation(ctx workflow.Context, state *State, request EscalationRequest) (string, error) {
	request.Need, request.Why = strings.TrimSpace(request.Need), strings.TrimSpace(request.Why)
	request.Tried, request.Next = strings.TrimSpace(request.Tried), strings.TrimSpace(request.Next)
	if err := validateEscalation(request); err != nil {
		return "", err
	}
	if state.Request.Handler {
		return "", fmt.Errorf("the escalation handler does not escalate to itself: forward the escalation instead")
	}
	if state.Closing || openEscalations(state) >= MaxOpenEscalations || len(state.Escalations) >= MaxEscalations {
		return "", fmt.Errorf("the chat is closing, or it has %d escalations waiting or has raised %d", MaxOpenEscalations, MaxEscalations)
	}
	id, err := NewID(ctx)
	if err != nil {
		return "", err
	}
	request.ID, request.Chat = id, state.Request.ID
	request.Issue, request.Project, request.StartedBy = state.Request.Issue, state.Request.Project, state.Request.StartedBy
	request.Session = &EscalationSession{
		Host: state.Request.Host, Backend: state.Request.Backend, Model: state.Request.Model, Models: state.Request.Models,
		Repositories: state.Request.Repositories, Restricted: state.Request.Restricted, Scope: InheritScope(state.Request.Scope, nil),
	}
	if request.Title == "" {
		request.Title = oneLine(request.Need, 72)
	}
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: EscalationWorkflowID(id), TaskQueue: TaskQueue,
		// The escalation is not the chat's child in its lifetime: it keeps
		// waiting when the chat continues as new. The chat cancels it when
		// it ends.
		ParentClosePolicy:        enumspb.PARENT_CLOSE_POLICY_ABANDON,
		TypedSearchAttributes:    EscalationSearchAttributes(request),
		Memo:                     EscalationMemo(request),
		WorkflowExecutionTimeout: escalationExpiry,
	})
	future := workflow.ExecuteChildWorkflow(childCtx, EscalationWorkflowName, EscalationState{Request: request})
	if err := future.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
		return "", err
	}
	ref := EscalationRef{ID: id, Title: request.Title, Status: EscalationOpen}
	state.Escalations = append(state.Escalations, ref)
	if state.Status == StatusIdle {
		state.Status = StatusEscalated
	}
	// An owner that works for the chat, such as an issue's workflow, hears of
	// it at once and not when the turn ends.
	report(ctx, *state, Report{ChatID: state.Request.ID, Escalation: &ref})
	return id, nil
}

// withdrawEscalations asks the escalations that still wait to end, because
// the chat does.
func withdrawEscalations(ctx workflow.Context, state *State) {
	for _, ref := range state.Escalations {
		if ref.Status.Waiting() {
			_ = workflow.RequestCancelExternalWorkflow(ctx, EscalationWorkflowID(ref.ID), "").Get(ctx, nil)
		}
	}
}
