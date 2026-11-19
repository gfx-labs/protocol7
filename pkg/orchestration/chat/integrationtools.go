package chat

import (
	"encoding/json"
	"fmt"
	"strings"

	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/p7"
)

// Integrations says which outside services the worker can reach for a chat's
// tools. A service the worker has no placeholder for gets no tool.
type Integrations struct {
	// Linear offers the Linear tools.
	Linear bool
	// LinearTeams are the team keys the Linear tools may touch, for the tool
	// descriptions. The activities enforce them.
	LinearTeams []string
	// Web offers web_search and web_extract.
	Web bool
}

// NewWorkflow is the chat workflow of a worker with these integrations and
// the operator credential on, as the core worker had it before Options.
// Register it under WorkflowName.
func NewWorkflow(integrations Integrations) func(workflow.Context, State) (Report, error) {
	return newWorkflow(Options{Integrations: integrations, OperatorCredentials: true})
}

// newWorkflow is the chat workflow of a worker with these options.
func newWorkflow(options Options) func(workflow.Context, State) (Report, error) {
	return func(ctx workflow.Context, state State) (Report, error) {
		state.options = options
		state.operatorOff = !options.OperatorCredentials
		return Workflow(ctx, state)
	}
}

// decodeArguments decodes a tool call's arguments.
func decodeArguments(call p7.ToolCall, into any) error {
	if err := json.Unmarshal(call.Arguments, into); err != nil {
		return fmt.Errorf("the arguments do not fit the tool: %w", err)
	}
	return nil
}

// linearTools let the agent read, search, comment on and open Linear issues.
// The activities run in the selfdev pod and reach Linear through PDP, which
// adds the token. The session pod has no Linear credential.
func linearTools(state *State) []p7.Tool {
	if !state.options.Integrations.Linear {
		return nil
	}
	call := func(ctx workflow.Context, name string, request LinearRequest) (p7.ToolResult, error) {
		request.Chat = state.Request.ID
		var result LinearResult
		if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, issueActivity), name, request).Get(ctx, &result); err != nil {
			return p7.ToolResult{}, fmt.Errorf("%s", cause(err))
		}
		return p7.TextResult(result.Text), nil
	}
	note := ""
	if teams := state.options.Integrations.LinearTeams; len(teams) > 0 {
		note = " Only the teams " + strings.Join(teams, ", ") + " may be used."
	}
	const identifier = `"identifier":{"type":"string","description":"The issue's identifier, such as ABC-123."}`
	view := p7.Tool{
		Name: "linear_issue_view",
		Description: "Read a Linear issue: its title, state, assignee, labels, description and last comments." + note +
			fmt.Sprintf(" The description is cut to %d bytes and each comment to %d. Give comments to change how many of the last comments it shows (default %d, at most %d).", DescriptionBytes, CommentBytes, DefaultLinearComments, MaxViewComments),
		InputSchema: json.RawMessage(`{"type":"object","properties":{` + identifier + `,"comments":{"type":"integer","minimum":1,"maximum":` + fmt.Sprint(MaxViewComments) + `}},"required":["identifier"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, tool p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct {
				Identifier string
				Comments   int
			}
			if err := decodeArguments(tool, &arguments); err != nil {
				return p7.ToolResult{}, err
			}
			return call(ctx, ActivityLinearIssueView, LinearRequest{Identifier: arguments.Identifier, Comments: arguments.Comments})
		},
	}
	search := p7.Tool{
		Name: "linear_issue_search",
		Description: "Search Linear issues by text in the title or description, and optionally by team key and state name, most recently updated first." + note +
			fmt.Sprintf(" Give at least one of query, team and state. Returns at most limit issues (default %d, at most %d), one line each.", DefaultLinearResults, MaxLinearResults),
		InputSchema: json.RawMessage(`{"type":"object","properties":{"query":{"type":"string"},"team":{"type":"string","description":"Team key, such as ABC."},"state":{"type":"string","description":"State name, such as Todo or In Progress."},"limit":{"type":"integer","minimum":1,"maximum":` + fmt.Sprint(MaxLinearResults) + `}},"additionalProperties":false}`),
		Run: func(ctx workflow.Context, tool p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct {
				Query, Team, State string
				Limit              int
			}
			if err := decodeArguments(tool, &arguments); err != nil {
				return p7.ToolResult{}, err
			}
			return call(ctx, ActivityLinearIssueSearch, LinearRequest{Query: arguments.Query, Team: arguments.Team, State: arguments.State, Limit: arguments.Limit})
		},
	}
	comment := p7.Tool{
		Name:        "linear_comment",
		Description: "Comment on a Linear issue, in Markdown." + note,
		InputSchema: json.RawMessage(`{"type":"object","properties":{` + identifier + `,"body":{"type":"string"}},"required":["identifier","body"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, tool p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct{ Identifier, Body string }
			if err := decodeArguments(tool, &arguments); err != nil {
				return p7.ToolResult{}, err
			}
			return call(ctx, ActivityLinearComment, LinearRequest{Identifier: arguments.Identifier, Body: arguments.Body})
		},
	}
	create := p7.Tool{
		Name:        "linear_issue_create",
		Description: "Open a Linear issue in a team, by team key." + note + " Each label must already exist in the team or the workspace. Returns the new issue's identifier and URL.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"team":{"type":"string","description":"Team key, such as ABC."},"title":{"type":"string"},"description":{"type":"string","description":"Markdown."},"labels":{"type":"array","items":{"type":"string"}}},"required":["team","title"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, tool p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct {
				Team, Title, Description string
				Labels                   []string
			}
			if err := decodeArguments(tool, &arguments); err != nil {
				return p7.ToolResult{}, err
			}
			return call(ctx, ActivityLinearIssueCreate, LinearRequest{Team: arguments.Team, Title: arguments.Title, Description: arguments.Description, Labels: arguments.Labels})
		},
	}
	return []p7.Tool{view, search, comment, create}
}

// webActivity gives a web call more time than a GitHub call: Parallel can
// take most of a minute to read 20 pages.
var webActivity = workflow.ActivityOptions{
	TaskQueue:           TaskQueue,
	StartToCloseTimeout: 3 * designActivity.StartToCloseTimeout,
	RetryPolicy:         designActivity.RetryPolicy,
}

// webTools let the agent search the web and read pages with Parallel. The
// activities run in the selfdev pod and reach Parallel through PDP, which
// adds the key.
func webTools(state *State) []p7.Tool {
	if !state.options.Integrations.Web {
		return nil
	}
	call := func(ctx workflow.Context, name string, request WebRequest) (p7.ToolResult, error) {
		request.Chat = state.Request.ID
		var result WebResult
		if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, webActivity), name, request).Get(ctx, &result); err != nil {
			return p7.ToolResult{}, fmt.Errorf("%s", cause(err))
		}
		return p7.TextResult(result.Text), nil
	}
	const rules = " Base claims on the returned excerpts and give the URL for each. Prefer primary sources: official documentation, the project's repository, release notes."
	search := p7.Tool{
		Name: "web_search",
		Description: "Search the web. Use it when a task needs information that is not in the repository or is newer than your training, such as current library versions, API documentation, release notes or error messages." +
			fmt.Sprintf(" The objective is one self-contained sentence saying what you need and why. Add two or three keyword queries of three to six words each (at most %d). With none, the objective is the only query. Returns each result's title, URL, date and excerpts.", MaxSearchQueries) + rules,
		InputSchema: json.RawMessage(`{"type":"object","properties":{"objective":{"type":"string"},"queries":{"type":"array","items":{"type":"string"},"maxItems":` + fmt.Sprint(MaxSearchQueries) + `}},"required":["objective"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, tool p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct {
				Objective string
				Queries   []string
			}
			if err := decodeArguments(tool, &arguments); err != nil {
				return p7.ToolResult{}, err
			}
			return call(ctx, ActivityWebSearch, WebRequest{Objective: arguments.Objective, Queries: arguments.Queries})
		},
	}
	extract := p7.Tool{
		Name:        "web_extract",
		Description: fmt.Sprintf("Read web pages: up to %d URLs per call, with excerpts that the objective focuses. URLs that failed are listed as errors while the others succeed.", MaxExtractURLs) + rules,
		InputSchema: json.RawMessage(`{"type":"object","properties":{"objective":{"type":"string","description":"What to look for in the pages."},"urls":{"type":"array","items":{"type":"string"},"minItems":1,"maxItems":` + fmt.Sprint(MaxExtractURLs) + `}},"required":["objective","urls"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, tool p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct {
				Objective string
				URLs      []string
			}
			if err := decodeArguments(tool, &arguments); err != nil {
				return p7.ToolResult{}, err
			}
			return call(ctx, ActivityWebExtract, WebRequest{Objective: arguments.Objective, URLs: arguments.URLs})
		},
	}
	return []p7.Tool{search, extract}
}
