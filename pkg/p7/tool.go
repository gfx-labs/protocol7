package p7

import (
	"encoding/json"
	"fmt"
	"slices"

	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/domain"
)

// Tool is offered to a session's agent as an MCP tool. Its calls run in the
// workflow that opened the session, as a Temporal update, so the workflow
// decides what a call may do and every call is in its history.
type Tool struct {
	Name        string
	Description string
	// InputSchema is the JSON Schema of the arguments, an object schema.
	// Nil accepts an object with any members.
	InputSchema json.RawMessage
	// Run runs one call in the workflow. It may run activities and read the
	// workflow's state. An error is shown to the agent as the tool failing.
	Run func(ctx workflow.Context, call ToolCall) (ToolResult, error)
}

// ToolCall is one call of a tool. Session is the ID of the session whose
// agent called it.
type ToolCall = domain.ToolCall

// ToolResult is what a call returns to the agent.
type ToolResult = domain.ToolResult

// ToolContent is one block of a ToolResult: text, or an image when MIMEType
// is set.
type ToolContent = domain.ToolContent

// TextResult is a result of one text block.
func TextResult(text string) ToolResult {
	return ToolResult{Content: []ToolContent{{Text: text}}}
}

// HandleTools makes the workflow answer the tool calls of the sessions it
// opened with tools. Call it at the start of every run, before the first
// OpenSession and after Continue-As-New, with the tools the sessions were
// opened with. A call from a session not in sessions is refused before it
// enters history, as is a tool not in tools.
//
// Before Continue-As-New, wait for workflow.AllHandlersFinished so that no
// call is left without an answer.
func HandleTools(ctx workflow.Context, tools []Tool, sessions func() []string) error {
	byName := make(map[string]Tool, len(tools))
	for _, tool := range tools {
		if tool.Name == "" || tool.Run == nil {
			return fmt.Errorf("handle tools: tool %q needs a name and a Run function", tool.Name)
		}
		byName[tool.Name] = tool
	}
	return workflow.SetUpdateHandlerWithOptions(ctx, domain.UpdateToolCall,
		func(ctx workflow.Context, call ToolCall) (ToolResult, error) {
			result, err := byName[call.Tool].Run(ctx, call)
			if err != nil {
				return ToolResult{Content: []ToolContent{{Text: err.Error()}}, IsError: true}, nil
			}
			return result, nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func(ctx workflow.Context, call ToolCall) error {
				if !slices.Contains(sessions(), call.Session) {
					return fmt.Errorf("session %q is not one this workflow opened", call.Session)
				}
				if _, ok := byName[call.Tool]; !ok {
					return fmt.Errorf("no tool %q", call.Tool)
				}
				return nil
			},
			Description: "A tool call from the agent of a session this workflow opened.",
		})
}

func toolSpecs(tools []Tool) []domain.ToolSpec {
	specs := make([]domain.ToolSpec, 0, len(tools))
	for _, tool := range tools {
		specs = append(specs, domain.ToolSpec{Name: tool.Name, Description: tool.Description, InputSchema: tool.InputSchema})
	}
	return specs
}
