package chat

import (
	"encoding/json"
	"fmt"
	"strings"

	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/p7"
)

// handlerChatTools are the tools of the escalation handler's chat: what it
// needs to read an escalation and decide it, and no tool that escalates,
// starts chats or probes, or touches the design board.
func handlerChatTools(state *State) []p7.Tool {
	tools := append(issueTools(state), pullTools(state)...)
	tools = append(tools, learningTools(state)...)
	return append(tools, handlerTools(state)...)
}

// handlerTools are the three decisions. Each ends the handler's attempt.
func handlerTools(state *State) []p7.Tool {
	decide := func(ctx workflow.Context, outcome Outcome, text, reasoning string) (p7.ToolResult, error) {
		serves := state.Request.Serves
		if serves == nil {
			return p7.ToolResult{}, fmt.Errorf("this chat serves no escalation")
		}
		decision := EscalationDecision{Outcome: outcome, Text: text, Reasoning: reasoning, Handler: state.Request.ID}
		if outcome == OutcomeResolved {
			if state.LastLearning == "" {
				return p7.ToolResult{}, fmt.Errorf("record the learning first with learning_record: what the chat did not know and the fix. A resolution without one is refused")
			}
			decision.Learning = state.LastLearning
		}
		var result EscalationState
		options := workflow.WithActivityOptions(ctx, handlerActivity)
		if err := workflow.ExecuteActivity(options, ActivityEscalationDecide, DecideRequest{Escalation: serves.Escalation, Decision: decision}).Get(ctx, &result); err != nil {
			return p7.ToolResult{}, fmt.Errorf("%s", cause(err))
		}
		return p7.TextResult(fmt.Sprintf("Escalation %s is %s. End your turn now.", serves.Escalation, result.Status)), nil
	}
	type arguments struct {
		Answer, Summary, Reason, Reasoning string
	}
	decode := func(call p7.ToolCall) (arguments, error) {
		var args arguments
		if err := json.Unmarshal(call.Arguments, &args); err != nil {
			return args, fmt.Errorf("the arguments do not fit the tool: %w", err)
		}
		return args, nil
	}
	return []p7.Tool{
		{
			Name: "escalation_resolve",
			Description: "Resolve the escalation you were given: you can unblock the chat yourself, by answering from documentation, code or learnings, pointing out a different approach, restarting its turn, or correcting the issue. " +
				"The answer goes to the chat as a message and starts its next turn. Record a learning with learning_record first: this is refused without one. Never use it for a credential, an access, a permission, a merge that rules block, a hypervisor or cluster write, money or a judgement call: forward those.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"answer":{"type":"string","maxLength":60000,"description":"What the chat reads and acts on."},"reasoning":{"type":"string","maxLength":60000,"description":"What you looked at and why you can resolve it. A person reads this."}},"required":["answer","reasoning"],"additionalProperties":false}`),
			Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
				args, err := decode(call)
				if err != nil {
					return p7.ToolResult{}, err
				}
				return decide(ctx, OutcomeResolved, args.Answer, args.Reasoning)
			},
		},
		{
			Name: "escalation_forward",
			Description: "Forward the escalation to a person: it needs something you may not or cannot do (a credential, access, a merge that rules block, a hypervisor or cluster write, money, a judgement call), or you are not sure. " +
				"The summary is what the person reads in navi and on the issue: what is needed, why you could not do it, and the exact action asked for.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"summary":{"type":"string","maxLength":60000,"description":"What is needed, why the handler could not do it, and the exact action asked for."},"reasoning":{"type":"string","maxLength":60000}},"required":["summary","reasoning"],"additionalProperties":false}`),
			Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
				args, err := decode(call)
				if err != nil {
					return p7.ToolResult{}, err
				}
				return decide(ctx, OutcomeForwarded, args.Summary, args.Reasoning)
			},
		},
		{
			Name: "escalation_decline",
			Description: "Decline the escalation: the request should not be done (unsafe, goes round a rule, outside the project's scope, or the issue says not to). " +
				"The chat is told why and a person is notified. It gives the chat no new permission.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"reason":{"type":"string","maxLength":60000,"description":"Why the request should not be done. The chat reads this."},"reasoning":{"type":"string","maxLength":60000}},"required":["reason","reasoning"],"additionalProperties":false}`),
			Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
				args, err := decode(call)
				if err != nil {
					return p7.ToolResult{}, err
				}
				result, err := decide(ctx, OutcomeDeclined, args.Reason, args.Reasoning)
				if err != nil {
					return result, err
				}
				// A person is notified on the issue the chat works on, too. A
				// comment that cannot be posted does not undo the refusal.
				if issue := state.Request.Issue; issue != nil && state.Request.Serves != nil {
					request := IssueRequest{Chat: state.Request.ID, Own: issue, Repos: issueRepos(state.Request), Repo: issue.Repo, Number: issue.Number,
						Body: fmt.Sprintf("The escalation handler declined the escalation `%s` of the chat `%s`:\n\n%s\n\nIt shows in navi's Escalations list until someone reviews it.", state.Request.Serves.Escalation, state.Request.Serves.Chat, strings.TrimSpace(args.Reason))}
					_ = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, issueActivity), ActivityIssueComment, request).Get(ctx, nil)
				}
				return result, nil
			},
		},
	}
}
