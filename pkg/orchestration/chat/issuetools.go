package chat

import (
	"encoding/json"
	"fmt"
	"strings"

	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/p7"
)

// chatTools are the tools of a chat's session.
func chatTools(state *State) []p7.Tool {
	if state.Request.Handler {
		return handlerChatTools(state)
	}
	tools := append(designTools(state), issueTools(state)...)
	tools = append(tools, pullTools(state)...)
	tools = append(tools, linearTools(state)...)
	tools = append(tools, webTools(state)...)
	tools = append(tools, coordinationTools(state)...)
	tools = append(tools, artifactTools(state)...)
	tools = append(tools, escalationTools(state)...)
	tools = append(tools, learningTools(state)...)
	tools = append(tools, operatorTools(state)...)
	tools = append(tools, probeTools(state)...)
	return append(tools, state.options.Tools...)
}

// githubRepo is owner/name of a clone address on github.com, or "".
func githubRepo(address string) string {
	address = strings.TrimSuffix(strings.TrimSpace(address), ".git")
	for _, prefix := range []string{"git@github.com:", "https://github.com/", "ssh://git@github.com/"} {
		if rest, ok := strings.CutPrefix(address, prefix); ok && strings.Count(rest, "/") == 1 {
			return rest
		}
	}
	return ""
}

// issueRepos are the repositories whose issues a chat's tools may touch: the
// ones the chat clones.
func issueRepos(request Request) []string {
	var repos []string
	for _, repository := range request.Repositories {
		if repo := githubRepo(repository.URL); repo != "" {
			repos = append(repos, repo)
		}
	}
	return repos
}

// issueActivity is how the issue tools reach GitHub. The activities run in
// the selfdev pod, which holds the token with issues write, and a refusal
// is final.
var issueActivity = workflow.ActivityOptions{
	TaskQueue:           TaskQueue,
	StartToCloseTimeout: designActivity.StartToCloseTimeout,
	RetryPolicy:         designActivity.RetryPolicy,
}

const issueToolsNote = " The repository is owner/name and must be one of this chat's repositories."

// issueTools let the agent manage GitHub issues. The guards are in the
// activities, not here, so that they hold whatever calls them: the agent may
// not take the label that hands an issue to selfdev off an issue it neither
// opened nor works on, and may not close one either.
func issueTools(state *State) []p7.Tool {
	call := func(ctx workflow.Context, name string, build func(*IssueRequest)) (p7.ToolResult, error) {
		request := IssueRequest{Chat: state.Request.ID, Own: state.Request.Issue, Repos: issueRepos(state.Request)}
		build(&request)
		var result IssueResult
		if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, issueActivity), name, request).Get(ctx, &result); err != nil {
			return p7.ToolResult{}, fmt.Errorf("%s", cause(err))
		}
		text := fmt.Sprintf("%s#%d", result.Repo, result.Number)
		if result.URL != "" {
			text += " " + result.URL
		}
		return p7.TextResult(text), nil
	}
	decode := func(call p7.ToolCall, into any) error {
		if err := json.Unmarshal(call.Arguments, into); err != nil {
			return fmt.Errorf("the arguments do not fit the tool: %w", err)
		}
		return nil
	}
	const repoProperty = `"repo":{"type":"string","description":"owner/name, such as gfx-labs/protocol7."}`
	create := p7.Tool{
		Name:        "issue_create",
		Description: "Open a GitHub issue." + issueToolsNote + " Give labels to set them. The label `" + HandoffLabel + "` hands the issue to selfdev, which works on it in a chat of its own. You may close or unlabel an issue you opened.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{` + repoProperty + `,"title":{"type":"string"},"body":{"type":"string"},"labels":{"type":"array","items":{"type":"string"}}},"required":["repo","title"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, tool p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct {
				Repo, Title, Body string
				Labels            []string
			}
			if err := decode(tool, &arguments); err != nil {
				return p7.ToolResult{}, err
			}
			return call(ctx, ActivityIssueCreate, func(r *IssueRequest) {
				r.Repo, r.Title, r.Body, r.Labels = arguments.Repo, arguments.Title, arguments.Body, arguments.Labels
			})
		},
	}
	comment := p7.Tool{
		Name:        "issue_comment",
		Description: "Comment on a GitHub issue or pull request, yours or anyone's." + issueToolsNote,
		InputSchema: json.RawMessage(`{"type":"object","properties":{` + repoProperty + `,"number":{"type":"integer"},"body":{"type":"string"}},"required":["repo","number","body"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, tool p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct {
				Repo   string
				Number int
				Body   string
			}
			if err := decode(tool, &arguments); err != nil {
				return p7.ToolResult{}, err
			}
			return call(ctx, ActivityIssueComment, func(r *IssueRequest) {
				r.Repo, r.Number, r.Body = arguments.Repo, arguments.Number, arguments.Body
			})
		},
	}
	labels := p7.Tool{
		Name: "issue_labels",
		Description: "Add and remove labels of a GitHub issue." + issueToolsNote + " You may add any label. You may remove the label `" + HandoffLabel +
			"` only from an issue you opened or the issue you work on, because removing it stops selfdev's work on that issue. Other labels you may remove from any issue.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{` + repoProperty + `,"number":{"type":"integer"},"add":{"type":"array","items":{"type":"string"}},"remove":{"type":"array","items":{"type":"string"}}},"required":["repo","number"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, tool p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct {
				Repo        string
				Number      int
				Add, Remove []string
			}
			if err := decode(tool, &arguments); err != nil {
				return p7.ToolResult{}, err
			}
			return call(ctx, ActivityIssueLabels, func(r *IssueRequest) {
				r.Repo, r.Number, r.Add, r.Remove = arguments.Repo, arguments.Number, arguments.Add, arguments.Remove
			})
		},
	}
	closeIssue := p7.Tool{
		Name:        "issue_close",
		Description: "Close a GitHub issue that you opened or that you work on." + issueToolsNote + " An issue someone else opened stays open: comment on it instead. Reason is completed or not_planned.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{` + repoProperty + `,"number":{"type":"integer"},"reason":{"type":"string","enum":["completed","not_planned"]}},"required":["repo","number"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, tool p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct {
				Repo   string
				Number int
				Reason string
			}
			if err := decode(tool, &arguments); err != nil {
				return p7.ToolResult{}, err
			}
			return call(ctx, ActivityIssueClose, func(r *IssueRequest) {
				r.Repo, r.Number, r.Reason = arguments.Repo, arguments.Number, arguments.Reason
			})
		},
	}
	edit := p7.Tool{
		Name: "issue_edit",
		Description: "Change the title or the body of a GitHub issue that you opened or that you work on." + issueToolsNote +
			" Give at least one of title and body; a body replaces the whole body, so send the full text. Use it to correct an issue after a mistake or a changed requirement. An issue someone else opened stays as it is: comment on it instead.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{` + repoProperty + `,"number":{"type":"integer"},"title":{"type":"string"},"body":{"type":"string"}},"required":["repo","number"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, tool p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct {
				Repo        string
				Number      int
				Title, Body string
			}
			if err := decode(tool, &arguments); err != nil {
				return p7.ToolResult{}, err
			}
			return call(ctx, ActivityIssueEdit, func(r *IssueRequest) {
				r.Repo, r.Number, r.Title, r.Body = arguments.Repo, arguments.Number, arguments.Title, arguments.Body
			})
		},
	}
	return []p7.Tool{create, comment, labels, closeIssue, edit}
}
