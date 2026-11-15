package p7

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/domain"
)

// ReportTool is the name of the tool through which an agent reports the
// result of a typed prompt.
const ReportTool = "p7_report"

// Reports are used in three steps. A workflow calls WithReports on its
// context before HandleTools and OpenSession. OpenSession then offers
// ReportTool to the session, HandleTools answers its calls, and PromptTyped
// reads the report that the agent made during its turn.

type reportsKey struct{}

// reports is a workflow's record of the typed prompts it waits on. Update
// handlers and the workflow's own coroutines both use it, and the SDK runs
// them one at a time, so it needs no lock.
type reports struct {
	handled bool
	pending map[string]*pendingReport // by session ID
}

// pendingReport is the report that a session's current typed prompt waits for.
type pendingReport struct {
	// check validates a call's arguments and returns them to keep.
	check func(arguments json.RawMessage) (json.RawMessage, error)
	// value is the arguments of the last valid call.
	value json.RawMessage
	// problem is why the last call was refused, if one was.
	problem string
}

func reportsOf(ctx workflow.Context) *reports {
	box, _ := ctx.Value(reportsKey{}).(*reports)
	return box
}

// WithReports returns a context that lets the workflow run typed prompts.
// Call it before HandleTools and OpenSession, and use the result for both
// and for PromptTyped. Each run of the workflow calls it again, also after
// Continue-As-New.
func WithReports(ctx workflow.Context) workflow.Context {
	if reportsOf(ctx) != nil {
		return ctx
	}
	return workflow.WithValue(ctx, reportsKey{}, &reports{pending: map[string]*pendingReport{}})
}

// reportSpec is how ReportTool is listed to the agent. A session's tools are
// fixed when it opens, so the schema of one report cannot be in it: the
// prompt of each typed turn carries that schema.
var reportSpec = domain.ToolSpec{
	Name: ReportTool,
	Description: "Reports the result of the task you were asked to report on. " +
		"Call it only when a prompt tells you to, with the JSON object that the prompt describes as the arguments.",
	InputSchema: json.RawMessage(`{"type":"object"}`),
}

// tool is the report tool as HandleTools runs it.
func (r *reports) tool() Tool {
	return Tool{
		Name:        reportSpec.Name,
		Description: reportSpec.Description,
		InputSchema: reportSpec.InputSchema,
		Run: func(_ workflow.Context, call ToolCall) (ToolResult, error) {
			pending := r.pending[call.Session]
			if pending == nil {
				return ToolResult{}, errors.New("no report is expected from this session now")
			}
			value, err := pending.check(call.Arguments)
			if err != nil {
				pending.problem = err.Error()
				return ToolResult{}, fmt.Errorf("the report was refused: %w. Call %s again with a corrected report", err, ReportTool)
			}
			pending.value, pending.problem = value, ""
			return TextResult("The report was received."), nil
		},
	}
}

// ReportErrorKind says how a typed prompt failed.
type ReportErrorKind string

const (
	// ReportMissing: the agent did not call ReportTool, in the turn or in
	// the repair turn.
	ReportMissing ReportErrorKind = "missing"
	// ReportInvalid: the agent called ReportTool, and the arguments of its
	// last call did not match the schema, in the turn and in the repair turn.
	ReportInvalid ReportErrorKind = "invalid"
	// ReportTurnEnded: a turn did not end normally, so it was not repaired.
	// Its stop reason is in the error.
	ReportTurnEnded ReportErrorKind = "turn_ended"
)

// ReportError is what PromptTyped fails with when it got no valid report. Use
// errors.As to read it.
type ReportError struct {
	Session string
	Turn    string
	Kind    ReportErrorKind
	// Problem says what was wrong with the last report, or how the turn ended.
	Problem string
}

func (e *ReportError) Error() string {
	return fmt.Sprintf("typed prompt of session %s, turn %s: %s: %s", e.Session, e.Turn, e.Kind, e.Problem)
}

// Typed is the result of PromptTyped.
type Typed[T any] struct {
	Value T
	// Turn is the last turn that ran: the repair turn if there was one.
	Turn Turn
	// Repaired is true when the report came from the repair turn.
	Repaired bool
}

// PromptTyped runs a turn whose answer is a value of type T. The agent is
// told to report it by calling ReportTool, with arguments that match the
// JSON Schema of T. The schema is made from T's fields and their json tags,
// as github.com/google/jsonschema-go does, and the schema is in the prompt.
// When T is not a struct or a map, the arguments are {"value": <T>}.
//
// A call whose arguments do not match is refused to the agent as a tool
// error, so the agent may fix it within the turn. If the turn ends with no
// valid report, the agent gets one repair turn that says what was wrong. If
// that turn does not produce one either, the call fails with a *ReportError.
//
// The session must have been opened from a context made by WithReports, and
// the workflow must have called HandleTools with it. A turn that does not
// end normally (failed, cancelled, interrupted) is not repaired and fails
// with ReportTurnEnded. A prompt with Retry and Timeout applies them to each
// of the two turns. Only one typed prompt may run on a session at a time.
func PromptTyped[T any](ctx workflow.Context, session *Session, prompt TextPrompt) (Typed[T], error) {
	var result Typed[T]
	if session == nil {
		return result, errors.New("typed prompt: no session")
	}
	box := reportsOf(ctx)
	if box == nil {
		return result, errors.New("typed prompt: the context has no reports; make it with p7.WithReports")
	}
	if !session.Reports {
		return result, fmt.Errorf("typed prompt: session %s was not opened from a context made by p7.WithReports", session.ID)
	}
	if box.pending[session.ID] != nil {
		return result, fmt.Errorf("typed prompt: session %s already has a typed prompt running", session.ID)
	}
	schema, check, err := reportSchema[T]()
	if err != nil {
		return result, fmt.Errorf("typed prompt: %w", err)
	}
	if prompt.TurnID == "" {
		if err := newID(ctx, &prompt.TurnID); err != nil {
			return result, fmt.Errorf("typed prompt: %w", err)
		}
	}
	pending := &pendingReport{check: check}
	box.pending[session.ID] = pending
	defer delete(box.pending, session.ID)

	turn := prompt
	turn.Context = joinParagraphs(prompt.Context, reportInstructions(schema))
	for attempt := 0; ; attempt++ {
		completed, err := PromptText(ctx, session, turn)
		result.Turn = completed
		if err != nil {
			return result, fmt.Errorf("typed prompt: %w", err)
		}
		fail := func(kind ReportErrorKind, problem string) error {
			return &ReportError{Session: session.ID, Turn: completed.Turn, Kind: kind, Problem: problem}
		}
		switch completed.StopReason {
		case StopEndTurn, StopMaxTokens, StopMaxTurnRequests, StopRefusal:
		default:
			problem := "the turn ended with " + completed.StopReason
			if completed.Failure != nil {
				problem += ": " + completed.Failure.Message
			}
			return result, fail(ReportTurnEnded, problem)
		}
		if pending.value != nil {
			result.Repaired = attempt > 0
			if err := unwrapReport(pending.value, schema, &result.Value); err != nil {
				return result, fail(ReportInvalid, err.Error())
			}
			return result, nil
		}
		kind, problem := ReportMissing, "the agent did not call "+ReportTool
		if pending.problem != "" {
			kind, problem = ReportInvalid, pending.problem
		}
		if attempt > 0 {
			return result, fail(kind, problem)
		}
		turn = prompt
		turn.TurnID = prompt.TurnID + "-repair"
		turn.Context = ""
		turn.Text = repairText(kind, problem, schema)
	}
}

// reportShape is the schema of a report and whether it is wrapped.
type reportShape struct {
	schema  *jsonschema.Schema
	wrapped bool
}

// reportSchema makes the schema of T and a function that checks a call's
// arguments against it.
func reportSchema[T any]() (reportShape, func(json.RawMessage) (json.RawMessage, error), error) {
	var zero reportShape
	schema, err := jsonschema.For[T](nil)
	if err != nil {
		return zero, nil, fmt.Errorf("the JSON schema of %T: %w", *new(T), err)
	}
	shape := reportShape{schema: schema}
	if schema.Type != "object" {
		shape.wrapped = true
		shape.schema = &jsonschema.Schema{
			Type:                 "object",
			Properties:           map[string]*jsonschema.Schema{"value": schema},
			Required:             []string{"value"},
			AdditionalProperties: &jsonschema.Schema{Not: &jsonschema.Schema{}},
		}
	}
	resolved, err := shape.schema.Resolve(nil)
	if err != nil {
		return zero, nil, fmt.Errorf("resolve the JSON schema of %T: %w", *new(T), err)
	}
	check := func(arguments json.RawMessage) (json.RawMessage, error) {
		if len(bytes.TrimSpace(arguments)) == 0 {
			arguments = json.RawMessage("{}")
		}
		var instance any
		if err := json.Unmarshal(arguments, &instance); err != nil {
			return nil, fmt.Errorf("the arguments are not JSON: %w", err)
		}
		if err := resolved.Validate(instance); err != nil {
			return nil, err
		}
		var value T
		if err := unwrapReport(arguments, shape, &value); err != nil {
			return nil, err
		}
		return arguments, nil
	}
	return shape, check, nil
}

// unwrapReport decodes the arguments of a valid report into out.
func unwrapReport(arguments json.RawMessage, shape reportShape, out any) error {
	if shape.wrapped {
		var wrapper struct {
			Value json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal(arguments, &wrapper); err != nil {
			return err
		}
		arguments = wrapper.Value
	}
	if err := json.Unmarshal(arguments, out); err != nil {
		return fmt.Errorf("the arguments do not decode into %T: %w", out, err)
	}
	return nil
}

func schemaText(shape reportShape) string {
	text, err := json.MarshalIndent(shape.schema, "", "  ")
	if err != nil {
		return "{}"
	}
	return string(text)
}

func reportInstructions(shape reportShape) string {
	return "Report your result by calling the " + ReportTool + " tool, once, when you are done. " +
		"The arguments of the call are a JSON object that matches this JSON Schema:\n\n" + schemaText(shape) + "\n\n" +
		"The report is read from that call and not from your message. A message with the report in it is not a report."
}

func repairText(kind ReportErrorKind, problem string, shape reportShape) string {
	var text strings.Builder
	switch kind {
	case ReportInvalid:
		text.WriteString("Your last report was refused: " + problem + "\n\n")
	default:
		text.WriteString("You ended your turn without calling " + ReportTool + ", so no report was received.\n\n")
	}
	text.WriteString("Call " + ReportTool + " now, once, with a JSON object that matches this JSON Schema:\n\n" + schemaText(shape))
	return text.String()
}

func joinParagraphs(parts ...string) string {
	var kept []string
	for _, part := range parts {
		if part != "" {
			kept = append(kept, part)
		}
	}
	return strings.Join(kept, "\n\n")
}
