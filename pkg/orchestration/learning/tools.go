package learning

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	base "github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// versionLearningBrief guards the activity that reads the learnings log for
// the first prompt: a chat that started before it replays without the call.
const versionLearningBrief = "learning-brief"

// MaxLearnings bounds the entries one chat may record, so that a chat that
// loops cannot fill the log.
const MaxLearnings = 8

// Instructions is the line of an app's chat instructions that tells the agent
// about the learnings log. An app adds it to the instructions of its chats
// and of its escalation handler.
const Instructions = "- Learnings: when you find out why something went wrong and what fixed it, and a later chat would waste time finding it out again, record it once with learning_record (what went wrong, the cause, what worked, and the scope). It is proposed, and the escalation handler or a person accepts it. Write no secrets, tokens, logs or transcripts in it. Before you dig into an odd failure, call learning_search.\n"

// learningActivity is how the learning tools reach the log. The activities
// run in the chat worker, which holds the namespace's Temporal client.
var learningActivity = workflow.ActivityOptions{
	TaskQueue:           base.TaskQueue,
	StartToCloseTimeout: 30 * time.Second,
	RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
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

// requestRepos are the repositories on github.com that the chat clones.
func requestRepos(request *base.Request) []string {
	var repos []string
	for _, repository := range request.Repositories {
		if repo := githubRepo(repository.Url); repo != "" {
			repos = append(repos, repo)
		}
	}
	return repos
}

// ChatBrief is the block of learnings for the first prompt of a chat that works in
// its repositories, or "". It is an app's base.Options.Brief. The log being
// unreadable never stops a chat: the block is left out.
func ChatBrief(ctx workflow.Context, current *base.Chat) string {
	request := current.State.Request
	if workflow.GetVersion(ctx, versionLearningBrief, workflow.DefaultVersion, 1) < 1 {
		return ""
	}
	var brief string
	options := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{TaskQueue: base.TaskQueue, StartToCloseTimeout: 20 * time.Second, RetryPolicy: learningActivity.RetryPolicy})
	if err := workflow.ExecuteActivity(options, ActivityBrief, BriefRequest{Repos: requestRepos(request), Text: briefText(request, current)}).Get(ctx, &brief); err != nil {
		return ""
	}
	return brief
}

// briefText is what the chat was asked, which ranks the entries of the brief
// when the embeddings server answers.
func briefText(request *base.Request, current *base.Chat) string {
	text := request.Title
	if message := current.State.Current; message != nil {
		text += "\n" + message.Text
	}
	return strings.TrimSpace(text)
}

// heldText tells the agent that its entry repeats a recorded one.
func heldText(result RecordResult) string {
	return "Not recorded: this likely repeats a learning that is in the log already.\n" + duplicatesText(result.Duplicates) +
		"\nIf that learning already says what yours does, do not record it again and go on. If yours adds something it lacks, call learning_record again with the same arguments and distinct true, and say in the problem, cause or fix what it adds." + areaText(result.Area, "", "")
}

// duplicatesText lists the recorded learnings that an entry likely repeats.
func duplicatesText(duplicates []Duplicate) string {
	var text strings.Builder
	text.WriteString("Likely duplicates:")
	for _, duplicate := range duplicates {
		fmt.Fprintf(&text, "\nid %s (%s, similarity %.2f)\n%s", duplicate.ID, duplicate.State, duplicate.Score, duplicate.Note)
	}
	return text.String()
}

// areaText suggests the area that fits the entry, when it is not the one the
// agent gave.
func areaText(suggestion *AreaSuggestion, scope ScopeKind, area string) string {
	if suggestion == nil || (scope == ScopeArea && strings.EqualFold(area, suggestion.Name)) {
		return ""
	}
	return fmt.Sprintf("\nThe area that fits best is %q (similarity %.2f). Use it as the area of an area scope if it is the right one.", suggestion.Name, suggestion.Score)
}

// ChatTools give a chat the log: it may search it and propose an entry, and the
// escalation handler's chat records accepted ones and decides on the proposed
// ones. An app adds them to the tools of its chats and of its handler's chat.
func ChatTools(state *base.State) []p7.Tool {
	handler := state.Request.Handler
	call := func(ctx workflow.Context, name string, input, result any) error {
		if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, learningActivity), name, input).Get(ctx, result); err != nil {
			return fmt.Errorf("%s", base.Cause(err))
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
		InputSchema: json.RawMessage(`{"type":"object","properties":{"title":{"type":"string","maxLength":120},"problem":{"type":"string","maxLength":1000,"description":"What went wrong."},"cause":{"type":"string","maxLength":1000},"fix":{"type":"string","maxLength":1000,"description":"What worked."},"scope":{"type":"string","enum":["project","repository","area"]},"repos":{"type":"array","items":{"type":"string"},"description":"owner/name. Required for the repository scope."},"area":{"type":"string","maxLength":60,"description":"Required for the area scope."},"escalation":{"type":"string","description":"The ID of the escalation it was learned from."},"distinct":{"type":"boolean","description":"Set only after a call answered that the entry likely repeats a recorded one, when yours adds something that one lacks."}},"required":["title","problem","cause","fix","scope"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, tool p7.ToolCall) (p7.ToolResult, error) {
			var args struct {
				Title, Problem, Cause, Fix string
				Scope                      ScopeKind
				Repos                      []string
				Area, Escalation           string
				Distinct                   bool
			}
			if err := json.Unmarshal(tool.Arguments, &args); err != nil {
				return p7.ToolResult{}, fmt.Errorf("the arguments do not fit the tool: %w", err)
			}
			if state.Learnings >= MaxLearnings {
				return p7.ToolResult{}, fmt.Errorf("this chat recorded %d learnings already, which is the most it may", MaxLearnings)
			}
			id, err := base.NewID(ctx)
			if err != nil {
				return p7.ToolResult{}, err
			}
			if handler && args.Escalation == "" && state.Request.Serves != nil {
				args.Escalation = state.Request.Serves.Escalation
			}
			entry := Entry{
				ID: strings.ToLower(id), Title: args.Title, Problem: args.Problem, Cause: args.Cause, Fix: args.Fix,
				Scope:    Scope{Kind: args.Scope, Repos: args.Repos, Area: args.Area},
				Source:   Source{Chat: state.Request.Id, Escalation: args.Escalation},
				Project:  state.Request.Project,
				Recorded: workflow.Now(ctx).UTC(),
			}
			// Checked here too, so that a refusal reaches the agent without
			// an activity.
			entry.State = Proposed
			entry.Normalize()
			if err := entry.Validate(); err != nil {
				return p7.ToolResult{}, err
			}
			var result RecordResult
			if err := call(ctx, ActivityRecord, RecordRequest{Entry: entry, Handler: handler, Distinct: args.Distinct}, &result); err != nil {
				return p7.ToolResult{}, err
			}
			if result.Held {
				// The entry was not recorded. For the handler, the learning
				// that already says it is the one its resolution names.
				if handler && len(result.Duplicates) > 0 {
					state.LastLearning = result.Duplicates[0].ID
				}
				return p7.TextResult(heldText(result)), nil
			}
			state.Learnings++
			state.LastLearning = result.ID
			text := fmt.Sprintf("Learning %s is proposed. The escalation handler or a person accepts it in navi's Learnings page before chats read it.", result.ID)
			if result.State == Accepted {
				text = fmt.Sprintf("Learning %s is recorded and accepted: chats read it from now on.", result.ID)
			}
			if len(result.Duplicates) > 0 {
				text += "\n" + duplicatesText(result.Duplicates)
			}
			return p7.TextResult(text + areaText(result.Area, args.Scope, args.Area)), nil
		},
	}
	searchDescription := "Search the learnings log for what earlier work found out: a failure that was understood, its cause and what worked. Give words to find and the repository you work in (owner/name), and it returns the best accepted entries, at most " +
		fmt.Sprint(SearchLimit) + ". Without words it returns the entries for the repository."
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
				State       State
			}
			if err := json.Unmarshal(tool.Arguments, &args); err != nil {
				return p7.ToolResult{}, fmt.Errorf("the arguments do not fit the tool: %w", err)
			}
			if args.State == Proposed && !handler {
				return p7.ToolResult{}, fmt.Errorf("only the escalation handler reads proposed learnings")
			}
			request := SearchRequest{Query: Query{Text: args.Query}, State: args.State}
			if args.Repo != "" {
				request.Query.Repos = []string{args.Repo}
			}
			var text string
			if err := call(ctx, ActivitySearch, request, &text); err != nil {
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
			var result RecordResult
			by := "handler chat " + state.Request.Id
			if err := call(ctx, ActivityDecide, DecideRequest{ID: args.ID, Decision: Decision{Action: args.Action, By: by}}, &result); err != nil {
				return p7.ToolResult{}, err
			}
			return p7.TextResult(fmt.Sprintf("Learning %s is now %s.", result.ID, result.State)), nil
		},
	}
	return append(tools, decide)
}
