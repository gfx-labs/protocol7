package chat

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/orchestration/learning"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// versionLearningBrief guards the activity that reads the learnings log for
// the first prompt: a chat that started before it replays without the call.
const versionLearningBrief = "learning-brief"

// MaxLearnings bounds the entries one chat may record, so that a chat that
// loops cannot fill the log.
const MaxLearnings = 8

// learningInstructions tell the agent about the learnings log.
const learningInstructions = "- Learnings: when you find out why something went wrong and what fixed it, and a later chat would waste time finding it out again, record it once with learning_record (what went wrong, the cause, what worked, and the scope). It is proposed, and the escalation handler or a person accepts it. Write no secrets, tokens, logs or transcripts in it. Before you dig into an odd failure, call learning_search.\n"

// learningActivity is how the learning tools reach the log. The activities
// run in the selfdev worker, which holds the namespace's Temporal client.
var learningActivity = workflow.ActivityOptions{
	TaskQueue:           TaskQueue,
	StartToCloseTimeout: 30 * time.Second,
	RetryPolicy:         designActivity.RetryPolicy,
}

// learningBrief is the block of learnings for the first prompt of a chat
// that works in these repositories, or "". The log being unreadable never
// stops a chat: the block is left out.
func learningBrief(ctx workflow.Context, request Request) string {
	if workflow.GetVersion(ctx, versionLearningBrief, workflow.DefaultVersion, 1) < 1 {
		return ""
	}
	var brief string
	options := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{TaskQueue: TaskQueue, StartToCloseTimeout: 20 * time.Second, RetryPolicy: learningActivity.RetryPolicy})
	if err := workflow.ExecuteActivity(options, learning.ActivityBrief, learning.BriefRequest{Repos: issueRepos(request)}).Get(ctx, &brief); err != nil {
		return ""
	}
	return brief
}

// learningTools give every chat the log: it may search it and propose an
// entry, and the escalation handler's chat records accepted ones and decides
// on the proposed ones.
func learningTools(state *State) []p7.Tool {
	handler := state.Request.Handler
	call := func(ctx workflow.Context, name string, input, result any) error {
		if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, learningActivity), name, input).Get(ctx, result); err != nil {
			return fmt.Errorf("%s", cause(err))
		}
		return nil
	}
	recordDescription := "Record a learning in the learnings log: something that went wrong, why, and what worked, so that no later chat finds it out again. " +
		"Say it in a short note: title, problem (what went wrong), cause, fix (what worked) and the scope: project, repository (give repos as owner/name) or area (such as ci or kubernetes, with repos if they are specific). " +
		"Name the escalation it came from, if any. Never write a secret, a token, a log or a transcript in it: say which credential and where it lives, not its value. "
	if handler {
		recordDescription += "As the escalation handler you record accepted entries, which chats read from now on."
	} else {
		recordDescription += "The entry is proposed. The escalation handler or a person accepts it before chats read it."
	}
	record := p7.Tool{
		Name:        "learning_record",
		Description: recordDescription,
		InputSchema: json.RawMessage(`{"type":"object","properties":{"title":{"type":"string","maxLength":120},"problem":{"type":"string","maxLength":1000,"description":"What went wrong."},"cause":{"type":"string","maxLength":1000},"fix":{"type":"string","maxLength":1000,"description":"What worked."},"scope":{"type":"string","enum":["project","repository","area"]},"repos":{"type":"array","items":{"type":"string"},"description":"owner/name. Required for the repository scope."},"area":{"type":"string","maxLength":60,"description":"Required for the area scope."},"escalation":{"type":"string","description":"The ID of the escalation it was learned from."}},"required":["title","problem","cause","fix","scope"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, tool p7.ToolCall) (p7.ToolResult, error) {
			var args struct {
				Title, Problem, Cause, Fix string
				Scope                      learning.ScopeKind
				Repos                      []string
				Area, Escalation           string
			}
			if err := json.Unmarshal(tool.Arguments, &args); err != nil {
				return p7.ToolResult{}, fmt.Errorf("the arguments do not fit the tool: %w", err)
			}
			if state.Learnings >= MaxLearnings {
				return p7.ToolResult{}, fmt.Errorf("this chat recorded %d learnings already, which is the most it may", MaxLearnings)
			}
			id, err := workflowID(ctx)
			if err != nil {
				return p7.ToolResult{}, err
			}
			if handler && args.Escalation == "" && state.Request.Serves != nil {
				args.Escalation = state.Request.Serves.Escalation
			}
			entry := learning.Entry{
				ID: strings.ToLower(id), Title: args.Title, Problem: args.Problem, Cause: args.Cause, Fix: args.Fix,
				Scope:    learning.Scope{Kind: args.Scope, Repos: args.Repos, Area: args.Area},
				Source:   learning.Source{Chat: state.Request.ID, Escalation: args.Escalation},
				Project:  state.Request.Project,
				Recorded: workflow.Now(ctx).UTC(),
			}
			// Checked here too, so that a refusal reaches the agent without
			// an activity.
			entry.State = learning.Proposed
			entry.Normalize()
			if err := entry.Validate(); err != nil {
				return p7.ToolResult{}, err
			}
			var result learning.RecordResult
			if err := call(ctx, learning.ActivityRecord, learning.RecordRequest{Entry: entry, Handler: handler}, &result); err != nil {
				return p7.ToolResult{}, err
			}
			state.Learnings++
			state.LastLearning = result.ID
			if result.State == learning.Accepted {
				return p7.TextResult(fmt.Sprintf("Learning %s is recorded and accepted: chats read it from now on.", result.ID)), nil
			}
			return p7.TextResult(fmt.Sprintf("Learning %s is proposed. The escalation handler or a person accepts it in navi's Learnings page before chats read it.", result.ID)), nil
		},
	}
	searchDescription := "Search the learnings log for what earlier work found out: a failure that was understood, its cause and what worked. Give words to find and the repository you work in (owner/name), and it returns the best accepted entries, at most " +
		fmt.Sprint(learning.SearchLimit) + ". Without words it returns the entries for the repository."
	stateProperty := ""
	if handler {
		searchDescription += " As the escalation handler you may ask for the proposed entries with state proposed, to decide on them."
		stateProperty = `,"state":{"type":"string","enum":["accepted","proposed"]}`
	}
	search := p7.Tool{
		Name:        "learning_search",
		Description: searchDescription,
		InputSchema: json.RawMessage(`{"type":"object","properties":{"query":{"type":"string","maxLength":300},"repo":{"type":"string","description":"owner/name of the repository you work in."}` + stateProperty + `},"additionalProperties":false}`),
		Run: func(ctx workflow.Context, tool p7.ToolCall) (p7.ToolResult, error) {
			var args struct {
				Query, Repo string
				State       learning.State
			}
			if err := json.Unmarshal(tool.Arguments, &args); err != nil {
				return p7.ToolResult{}, fmt.Errorf("the arguments do not fit the tool: %w", err)
			}
			if args.State == learning.Proposed && !handler {
				return p7.ToolResult{}, fmt.Errorf("only the escalation handler reads proposed learnings")
			}
			request := learning.SearchRequest{Query: learning.Query{Text: args.Query}, State: args.State}
			if args.Repo != "" {
				request.Query.Repos = []string{args.Repo}
			}
			var text string
			if err := call(ctx, learning.ActivitySearch, request, &text); err != nil {
				return p7.ToolResult{}, err
			}
			return p7.TextResult(text), nil
		},
	}
	tools := []p7.Tool{record, search}
	if !handler {
		return tools
	}
	decide := p7.Tool{
		Name:        "learning_decide",
		Description: "Decide on a learning, as the escalation handler: accept a proposed one so that chats read it, reject a proposed one that is wrong, a secret or a duplicate, or retire an accepted one that no longer holds.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"id":{"type":"string"},"action":{"type":"string","enum":["accept","reject","retire"]}},"required":["id","action"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, tool p7.ToolCall) (p7.ToolResult, error) {
			var args struct{ ID, Action string }
			if err := json.Unmarshal(tool.Arguments, &args); err != nil {
				return p7.ToolResult{}, fmt.Errorf("the arguments do not fit the tool: %w", err)
			}
			var result learning.RecordResult
			by := "handler chat " + state.Request.ID
			if err := call(ctx, learning.ActivityDecide, learning.DecideRequest{ID: args.ID, Decision: learning.Decision{Action: args.Action, By: by}}, &result); err != nil {
				return p7.ToolResult{}, err
			}
			return p7.TextResult(fmt.Sprintf("Learning %s is now %s.", result.ID, result.State)), nil
		},
	}
	return append(tools, decide)
}
