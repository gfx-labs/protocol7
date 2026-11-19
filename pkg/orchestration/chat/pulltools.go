package chat

import (
	"encoding/json"
	"fmt"

	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/p7"
)

// pullTools let the agent open, comment on and read GitHub pull requests,
// beside the issue tools and with the same guards, which are in the
// activities. There is no tool to merge or close a pull request: the review
// workflow merges, and closing one is a comment to a person. The head of a
// pull request that pr_create opens is the chat's own branch, and the tool
// has no argument to change it.
func pullTools(state *State) []p7.Tool {
	call := func(ctx workflow.Context, name string, build func(*IssueRequest)) (p7.ToolResult, error) {
		request := IssueRequest{Chat: state.Request.ID, Own: state.Request.Issue, Repos: issueRepos(state.Request), ReviewHint: state.Request.ReviewHint}
		build(&request)
		var result PullResult
		if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, issueActivity), name, request).Get(ctx, &result); err != nil {
			return p7.ToolResult{}, fmt.Errorf("%s", cause(err))
		}
		return p7.TextResult(result.Text), nil
	}
	decode := func(call p7.ToolCall, into any) error {
		if err := json.Unmarshal(call.Arguments, into); err != nil {
			return fmt.Errorf("the arguments do not fit the tool: %w", err)
		}
		return nil
	}
	const repoProperty = `"repo":{"type":"string","description":"owner/name, such as gfx-labs/protocol7."}`
	create := p7.Tool{
		Name: "pr_create",
		Description: "Open a GitHub pull request from this chat's own branch " + "chat/<id>." + issueToolsNote +
			" The base is the repository's default branch unless you give one. The head cannot be given. It is refused when the branch is not pushed, has no commits ahead of the base, or already has an open pull request, which is returned instead. Put `Closes owner/name#N` in the body of the pull request that completes an issue.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{` + repoProperty + `,"title":{"type":"string"},"body":{"type":"string"},"base":{"type":"string","description":"Branch to merge into. Default: the repository's default branch."},"draft":{"type":"boolean"}},"required":["repo","title"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, tool p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct {
				Repo, Title, Body, Base string
				Draft                   bool
			}
			if err := decode(tool, &arguments); err != nil {
				return p7.ToolResult{}, err
			}
			return call(ctx, ActivityPullCreate, func(r *IssueRequest) {
				r.Repo, r.Title, r.Body, r.Base, r.Draft = arguments.Repo, arguments.Title, arguments.Body, arguments.Base, arguments.Draft
			})
		},
	}
	comment := p7.Tool{
		Name:        "pr_comment",
		Description: "Comment on a GitHub pull request, yours or anyone's." + issueToolsNote + " It is refused for an issue that is no pull request: use issue_comment for that. This is the only write to a pull request that you did not open. To ask for one to be closed, comment on it.",
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
			return call(ctx, ActivityPullComment, func(r *IssueRequest) {
				r.Repo, r.Number, r.Body = arguments.Repo, arguments.Number, arguments.Body
			})
		},
	}
	view := p7.Tool{
		Name: "pr_view",
		Description: "Read a GitHub pull request, yours or anyone's." + issueToolsNote +
			fmt.Sprintf(" Returns the title, state, head and base, the review state and the reviews' text, the checks with their conclusion, the tail of the log (at most %d lines) of a failed check, and the last comments, each cut to %d bytes. Give comments to change how many of the last comments it shows (default %d, at most %d).", LogLines, CommentBytes, DefaultViewComments, MaxViewComments),
		InputSchema: json.RawMessage(`{"type":"object","properties":{` + repoProperty + `,"number":{"type":"integer"},"comments":{"type":"integer","minimum":1,"maximum":` + fmt.Sprint(MaxViewComments) + `}},"required":["repo","number"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, tool p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct {
				Repo     string
				Number   int
				Comments int
			}
			if err := decode(tool, &arguments); err != nil {
				return p7.ToolResult{}, err
			}
			return call(ctx, ActivityPullView, func(r *IssueRequest) {
				r.Repo, r.Number, r.Comments = arguments.Repo, arguments.Number, arguments.Comments
			})
		},
	}
	return []p7.Tool{create, comment, view}
}
