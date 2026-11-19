package chat

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/orchestration/probe"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

const MaxProbes = 8

// versionProbeEscalate gates probe_escalate raising an escalation, where it
// started a child chat before.
const versionProbeEscalate = "probe-escalate"

type Probe = navi.Probe

func probeTools(state *State) []p7.Tool {
	// This broker is deployed only in the core namespace, never for tenant chats.
	if state.Request.Project != "" || state.Request.Restricted {
		return nil
	}
	options := workflow.ActivityOptions{TaskQueue: TaskQueue, StartToCloseTimeout: 30 * time.Second, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 3}}
	return []p7.Tool{
		{Name: "probe_escalate", Description: "Ask a person in navi to review and deploy a probe needing network access, credentials or privileges. Raises an escalation, as the escalate tool does, and returns its ID: end your turn after calling it, and the person's reply arrives in this chat as a message. Never grants the agent additional permissions.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"plan":{"type":"string","maxLength":60000}},"required":["plan"],"additionalProperties":false}`),
			Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
				var args struct {
					Plan string `json:"plan"`
				}
				if err := json.Unmarshal(call.Arguments, &args); err != nil {
					return p7.ToolResult{}, err
				}
				if strings.TrimSpace(args.Plan) == "" || len(args.Plan) > 60000 {
					return p7.ToolResult{}, fmt.Errorf("plan must be nonempty and at most 60000 bytes")
				}
				// Before this version a probe review was a child chat that the
				// agent waited for with chat_await. A history that holds that
				// call replays as it ran.
				if workflow.GetVersion(ctx, versionProbeEscalate, workflow.DefaultVersion, 1) == workflow.DefaultVersion {
					id, err := spawnChild(ctx, state, "Present this proposed probe to the person for review and manual deployment. Do not execute privileged or network actions. Wait for their reply, then summarize it to the parent. Plan:\n"+args.Plan, "Probe review", &Escalation{Reason: "Probe needs person review and deployment"})
					return p7.TextResult(id), err
				}
				id, err := raiseEscalation(ctx, state, EscalationRequest{
					Title: "Probe review",
					Need:  "Review this proposed probe and deploy it by hand. It needs network access, credentials or privileges that a chat's probes do not get, so the chat cannot run it. Say whether it may be deployed, or what to change.\n\nPlan:\n" + args.Plan,
					Why:   "A chat's own probes have no network access, credentials, mounts or exec. This one needs a person to review it and deploy it in navi.",
					Next:  "Go on from the person's reply. An approval gives the chat no extra authority.",
				})
				if err != nil {
					return p7.ToolResult{}, err
				}
				return p7.TextResult(escalatedText(id)), nil
			}},
		{Name: "probe_create", Description: "Run a temporary isolated cluster probe. Kind is Pod, Job, ConfigMap or Service. Pod/Job takes image and command, ConfigMap takes data, Service takes port. No credentials, mounts, exec or network access. At most eight objects per chat, 10-minute TTL. A reaper also deletes them when the chat closes. Network or privileged probes need a person to review and deploy them in navi instead.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"kind":{"type":"string","enum":["Pod","Job","ConfigMap","Service"]},"image":{"type":"string","maxLength":512},"command":{"type":"array","items":{"type":"string"},"maxItems":64},"data":{"type":"object","additionalProperties":{"type":"string"}},"port":{"type":"integer","minimum":1024,"maximum":65535}},"required":["kind"],"additionalProperties":false}`),
			Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
				if state.Closing || len(state.Probes) >= MaxProbes {
					return p7.ToolResult{}, fmt.Errorf("chat is closing or eight-probe bound reached")
				}
				var request probe.Request
				if err := json.Unmarshal(call.Arguments, &request); err != nil {
					return p7.ToolResult{}, err
				}
				id, err := workflowID(ctx)
				if err != nil {
					return p7.ToolResult{}, err
				}
				request.Chat = state.Request.ID
				request.Name = "probe-" + state.Request.ID + "-" + id
				state.Probes = append(state.Probes, Probe{Kind: request.Kind, Name: request.Name})
				var name string
				err = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, options), probe.ActivityCreate, request).Get(ctx, &name)
				return p7.TextResult(name), err
			}},
		{Name: "probe_read", Description: "Read this chat's probe status, and up to 100 lines of Pod logs. Only names returned by probe_create are accepted.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"],"additionalProperties":false}`),
			Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
				request, err := ownProbe(state, call)
				if err != nil {
					return p7.ToolResult{}, err
				}
				var text string
				err = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, options), probe.ActivityRead, request).Get(ctx, &text)
				return p7.TextResult(text), err
			}},
		{Name: "probe_delete", Description: "Delete one of this chat's probes. Cannot delete unlabelled or other chats' resources.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"],"additionalProperties":false}`),
			Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
				request, err := ownProbe(state, call)
				if err != nil {
					return p7.ToolResult{}, err
				}
				err = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, options), probe.ActivityDelete, request).Get(ctx, nil)
				return p7.TextResult("Probe deleted."), err
			}},
	}
}

func ownProbe(state *State, call p7.ToolCall) (probe.Request, error) {
	var args struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(call.Arguments, &args); err != nil {
		return probe.Request{}, err
	}
	index := slices.IndexFunc(state.Probes, func(p Probe) bool { return p.Name == args.Name })
	if index < 0 {
		return probe.Request{}, fmt.Errorf("not a probe created by this chat")
	}
	return probe.Request{Chat: state.Request.ID, Name: args.Name, Kind: state.Probes[index].Kind}, nil
}
