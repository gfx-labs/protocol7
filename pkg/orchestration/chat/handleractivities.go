package chat

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"github.com/gfx-labs/protocol7/pkg/transport/githubapi"
)

// Names of the activities of the escalation handler. They are methods of
// HandlerActivities, which the selfdev worker registers.
const (
	ActivityEscalationHandoff = "EscalationHandoff"
	ActivityEscalationBrief   = "EscalationBrief"
	ActivityEscalationOutcome = "EscalationOutcome"
	ActivityEscalationDecide  = "EscalationDecide"
)

// ErrorRefused is the type of the error of a call that a workflow refused. It
// is final.
const ErrorRefused = githubapi.ErrorRefused

// HandlerConfig is the handler's own configuration.
type HandlerConfig struct {
	// Model and Models are the handler's own model choice, as the page's
	// choices are: a name and the harness's model list. Empty leaves the
	// handler on the models of the chat it serves, so that it draws on the
	// same provider limits.
	Model  string
	Models []string
}

// HandlerActivities are the handler's calls to Temporal: what a workflow
// cannot do for itself, which is to start a workflow with a signal, to read
// the state of another and to update another.
type HandlerActivities struct {
	Client client.Client
	Config HandlerConfig
}

// EscalationHandoff gives an escalation to the handler workflow. It signals
// the workflow and starts it if it does not run.
func (a *HandlerActivities) EscalationHandoff(ctx context.Context, request EscalationRequest) error {
	intake := HandlerIntake{Request: request, Models: a.Config.Models, Model: a.Config.Model}
	_, err := a.Client.SignalWithStartWorkflow(ctx, HandlerWorkflowID, SignalEscalationIntake, intake,
		client.StartWorkflowOptions{ID: HandlerWorkflowID, TaskQueue: TaskQueue}, HandlerWorkflowName, HandlerState{})
	return err
}

// briefLimit bounds each text that the brief takes from the chat's state.
const briefLimit = 3000

// EscalationBrief says what the handler's chat is told of the chat that
// raised the escalation: its recent state, read from its workflow. The chat's
// session transcript is not readable here, so the brief is what the workflow
// keeps: the message it works on, the last turn's text, the last error and
// the other escalations it raised.
func (a *HandlerActivities) EscalationBrief(ctx context.Context, request EscalationRequest) (string, error) {
	value, err := a.Client.QueryWorkflow(ctx, WorkflowID(request.Chat), "", QueryState)
	if err != nil {
		return "", fmt.Errorf("query the chat %s: %w", request.Chat, err)
	}
	var state State
	if err := value.Get(&state); err != nil {
		return "", fmt.Errorf("read the state of the chat %s: %w", request.Chat, err)
	}
	return briefOf(state, request.ID), nil
}

func briefOf(state State, escalation string) string {
	var text strings.Builder
	fmt.Fprintf(&text, "Chat %s %q, status %s, %d turns so far, branch %s.\n", state.Request.ID, state.Request.Title, state.Status, state.Turns, Branch(state.Request.ID))
	if state.Request.Restricted {
		text.WriteString("Its person may change only the project's own repositories, and so may you.\n")
	}
	if state.Current != nil {
		fmt.Fprintf(&text, "\nThe message it works on now:\n%s\n", clipText(state.Current.Text, briefLimit))
	}
	if state.Result.Text != "" {
		fmt.Fprintf(&text, "\nWhat it said at the end of its last turn:\n%s\n", clipText(state.Result.Text, briefLimit))
	}
	if state.LastStop != "" {
		fmt.Fprintf(&text, "\nLast turn's stop reason: %s\n", state.LastStop)
	}
	if state.Error != "" {
		fmt.Fprintf(&text, "\nLast error: %s\n", clipText(state.Error, 1000))
	}
	for _, ref := range state.Escalations {
		if ref.ID != escalation {
			fmt.Fprintf(&text, "\nIts other escalation %s (%q) is %s.\n", ref.ID, ref.Title, ref.Status)
		}
	}
	return text.String()
}

func clipText(text string, limit int) string {
	text = strings.TrimSpace(text)
	if len(text) <= limit {
		return text
	}
	return strings.ToValidUTF8(text[:limit], "") + "\n[cut]"
}

// EscalationOutcome reads an escalation's state: what the handler decided.
func (a *HandlerActivities) EscalationOutcome(ctx context.Context, id string) (EscalationState, error) {
	return queryEscalationState(ctx, a.Client, id)
}

func queryEscalationState(ctx context.Context, c client.Client, id string) (EscalationState, error) {
	var state EscalationState
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	value, err := c.QueryWorkflow(ctx, EscalationWorkflowID(id), "", QueryEscalation)
	if err != nil {
		return state, err
	}
	return state, value.Get(&state)
}

// DecideRequest is what the handler's chat decides about an escalation.
type DecideRequest struct {
	Escalation string             `json:"escalation"`
	Decision   EscalationDecision `json:"decision"`
}

// EscalationDecide gives the escalation the handler's decision, as an update
// that the escalation's workflow may refuse. A refusal is final.
func (a *HandlerActivities) EscalationDecide(ctx context.Context, request DecideRequest) (EscalationState, error) {
	handle, err := a.Client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: EscalationWorkflowID(request.Escalation), UpdateName: UpdateEscalationDecide,
		Args: []any{request.Decision}, WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	var state EscalationState
	if err == nil {
		err = handle.Get(ctx, &state)
	}
	if refused, ok := errors.AsType[*temporal.ApplicationError](err); ok {
		return state, temporal.NewNonRetryableApplicationError(refused.Message(), ErrorRefused, nil)
	}
	return state, err
}
