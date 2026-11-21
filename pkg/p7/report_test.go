package p7_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	acp "github.com/gfx-labs/go-acp"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/p7"
)

// reportAgentEnv, when set, makes this test binary an ACP agent that reports
// through p7_report as its script says. The first prompt of a session is
// "scenario <name>". The scripts are in reportAgent.report.
const reportAgentEnv = "P7_TEST_REPORT_AGENT"

type reportAgent struct {
	*toolAgent
	scenario string
	turns    int
}

const (
	validReport   = `{"verdict":"approve","issues":["a","b"]}`
	invalidReport = `{"verdict":5}`
)

// report returns the arguments of the calls the agent makes in its turn.
func (a *reportAgent) report() []string {
	switch a.scenario {
	case "ok":
		return []string{validReport}
	case "wrapped":
		return []string{`{"value":[3,4]}`}
	case "bad-then-fix":
		return []string{invalidReport, validReport}
	case "silent-then-report":
		if a.turns == 2 {
			return []string{validReport}
		}
	case "invalid-then-report":
		if a.turns == 2 {
			return []string{validReport}
		}
		return []string{invalidReport}
	case "invalid":
		return []string{invalidReport}
	case "extra-member":
		return []string{`{"verdict":"approve","issues":[],"mood":"fine"}`}
	}
	return nil
}

func (a *reportAgent) Prompt(ctx context.Context, request acp.PromptRequest) (acp.PromptResponse, error) {
	a.turns++
	blocks := request.Prompt
	text := blocks[len(blocks)-1].Text.Text
	if name, ok := strings.CutPrefix(text, "scenario "); ok {
		a.scenario = name
	}
	sawSchema := false
	for _, block := range blocks[:len(blocks)-1] {
		sawSchema = sawSchema || strings.Contains(block.Text.Text, "JSON Schema")
	}
	reply := fmt.Sprintf("turn %d schema-in-context=%t", a.turns, sawSchema)
	if strings.HasPrefix(text, "Your last report was refused") {
		reply += " refused-named"
	}
	if strings.HasPrefix(text, "You ended your turn without calling") {
		reply += " missing-named"
	}
	for _, arguments := range a.report() {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(arguments), &decoded); err != nil {
			return acp.PromptResponse{}, err
		}
		if _, err := a.mcp(ctx, "tools/call", map[string]any{"name": p7.ReportTool, "arguments": decoded}); err != nil {
			return acp.PromptResponse{}, err
		}
	}
	err := a.connection.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: request.SessionId,
		Update: acp.SessionUpdate{AgentMessageChunk: &acp.SessionUpdateAgentMessageChunk{
			Content: acp.ContentBlock{Text: &acp.ContentBlockText{Type: "text", Text: reply}},
		}},
	})
	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, err
}

type verdict struct {
	Verdict string   `json:"verdict"`
	Issues  []string `json:"issues"`
}

// reportOutcome is what reportWorkflow returns.
type reportOutcome struct {
	Verdict  verdict
	Numbers  []int
	Repaired bool
	Text     string
	ErrKind  string
	Problem  string
	Turn     string
}

func reportWorkflow(ctx workflow.Context, host, prompt, shape string) (reportOutcome, error) {
	ctx = p7.WithReports(ctx)
	var opened []string
	if err := p7.HandleTools(ctx, nil, func() []string { return opened }); err != nil {
		return reportOutcome{}, err
	}
	session, err := p7.OpenSession(ctx, p7.SessionSpec{Placement: p7.Placement{Backend: p7.BackendHost, Host: host}})
	if err != nil {
		return reportOutcome{}, err
	}
	opened = append(opened, session.ID)
	var outcome reportOutcome
	var reportErr error
	switch shape {
	case "wrapped":
		var typed p7.Typed[[]int]
		typed, reportErr = p7.PromptTyped[[]int](ctx, session, p7.TextPrompt{Text: prompt})
		outcome.Numbers, outcome.Repaired, outcome.Text = typed.Value, typed.Repaired, typed.Turn.Text
	default:
		var typed p7.Typed[verdict]
		typed, reportErr = p7.PromptTyped[verdict](ctx, session, p7.TextPrompt{Text: prompt, Context: "Review the change."})
		outcome.Verdict, outcome.Repaired, outcome.Text = typed.Value, typed.Repaired, typed.Turn.Text
		outcome.Turn = typed.Turn.Turn
	}
	var failure *p7.ReportError
	if errors.As(reportErr, &failure) {
		outcome.ErrKind, outcome.Problem = string(failure.Kind), failure.Problem
	} else if reportErr != nil {
		return outcome, reportErr
	}
	if err := session.Close(ctx); err != nil {
		return outcome, err
	}
	return outcome, nil
}

// A typed prompt returns the value the agent reported through p7_report, asks
// for one repair turn when the first turn gave no valid report, and then
// fails with a *ReportError.
func TestPromptTyped(t *testing.T) {
	temporalClient := startStack(t, reportAgentEnv, func(w *p7.Worker) { w.RegisterWorkflow("report", reportWorkflow) })

	good := verdict{Verdict: "approve", Issues: []string{"a", "b"}}
	for _, test := range []struct {
		scenario, shape string
		want            reportOutcome
		textHas         []string
	}{
		{scenario: "ok", want: reportOutcome{Verdict: good}, textHas: []string{"turn 1", "schema-in-context=true"}},
		{scenario: "wrapped", shape: "wrapped", want: reportOutcome{Numbers: []int{3, 4}}, textHas: []string{"turn 1"}},
		// A refused call is an error to the agent, which fixes it in the turn.
		{scenario: "bad-then-fix", want: reportOutcome{Verdict: good}, textHas: []string{"turn 1"}},
		{scenario: "silent-then-report", want: reportOutcome{Verdict: good, Repaired: true}, textHas: []string{"turn 2", "missing-named"}},
		{scenario: "invalid-then-report", want: reportOutcome{Verdict: good, Repaired: true}, textHas: []string{"turn 2", "refused-named"}},
		{scenario: "silent", want: reportOutcome{ErrKind: "missing"}, textHas: []string{"turn 2"}},
		{scenario: "invalid", want: reportOutcome{ErrKind: "invalid"}, textHas: []string{"turn 2"}},
		{scenario: "extra-member", want: reportOutcome{ErrKind: "invalid"}, textHas: []string{"turn 2"}},
	} {
		t.Run(test.scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			run, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: "consumer"},
				"report", "test", "scenario "+test.scenario, test.shape)
			if err != nil {
				t.Fatal(err)
			}
			var got reportOutcome
			if err := run.Get(ctx, &got); err != nil {
				t.Fatal(err)
			}
			if test.scenario == "ok" {
				// The workflow lists the session it opened and closed in its memo.
				description, err := temporalClient.DescribeWorkflowExecution(ctx, run.GetID(), "")
				if err != nil {
					t.Fatal(err)
				}
				records, err := p7.SessionRecords(description.GetWorkflowExecutionInfo().GetMemo().GetFields())
				if err != nil {
					t.Fatal(err)
				}
				if len(records) != 1 || records[0].ID == "" || records[0].Opened.IsZero() || records[0].Closed.IsZero() {
					t.Errorf("the memo lists %+v, want one opened and closed session", records)
				}
			}
			for _, want := range test.textHas {
				if !strings.Contains(got.Text, want) {
					t.Errorf("the last turn's text is %q, want it to contain %q", got.Text, want)
				}
			}
			if test.want.ErrKind != "" && got.Problem == "" {
				t.Errorf("the error has no problem")
			}
			got.Text, got.Problem, got.Turn = "", "", ""
			if fmt.Sprint(got) != fmt.Sprint(test.want) {
				t.Errorf("got %+v, want %+v", got, test.want)
			}
		})
	}
}
