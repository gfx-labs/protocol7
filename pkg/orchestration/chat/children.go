package chat

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"
	"github.com/gfx-labs/protocol7/pkg/orchestration/upgrade"
	"github.com/gfx-labs/protocol7/pkg/support/prompt"
	"google.golang.org/protobuf/proto"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/p7"
	"github.com/gfx-labs/protocol7/pkg/support/ids"
)

const versionSubchatSpawn = "subchat-spawn"
const versionSubchatWake = "subchat-wake"

// versionSubchatWakeRestricted gates the restriction of a wake notice. A
// history from before it queued the notice with the chat's own flag, and
// replays that way.
const versionSubchatWakeRestricted = "subchat-wake-restricted"

//go:embed child-notice.md
var childNoticeTemplate string

// The bounds are per chat. With a depth of two, one root holds at most
// MaxLiveChildren live children and MaxLiveChildren*MaxLiveChildren live
// grandchildren, so 20 live descendants, and a validator cannot count the
// ones below a child. A per-tree cap would need a query of every descendant.
const (
	MaxLiveChildren = 4
	MaxChildDepth   = 2
	MaxChildren     = 16
)

// Child is a native Temporal child owned by this chat.
type Child = navi.Child

func liveChildren(state *State) int {
	count := 0
	for _, child := range state.Children {
		if !child.Done {
			count++
		}
	}
	return count
}

// NewID is a new ID made in a side effect, so that a replay reads the same
// one. Chats, escalations and an app's records take their IDs from it.
func NewID(ctx workflow.Context) (string, error) {
	var id string
	err := workflow.SideEffect(ctx, func(workflow.Context) any { return ids.New() }).Get(&id)
	return id, err
}

func coordinationTools(state *State) []p7.Tool {
	return []p7.Tool{
		{Name: "chat_spawn", Description: "Start a child chat for a bounded task, in this project with this model and restrictions. Children commit on their own branches. At most four live children, sixteen total, two levels deep. Returns its chat ID. Use chat_await for results and chat_message for guidance.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"task":{"type":"string","maxLength":65536},"title":{"type":"string","maxLength":120}},"required":["task"],"additionalProperties":false}`),
			Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
				var args struct {
					Task  string `json:"task"`
					Title string `json:"title"`
				}
				if err := json.Unmarshal(call.Arguments, &args); err != nil {
					return p7.ToolResult{}, err
				}
				id, err := spawnChild(ctx, state, args.Task, args.Title, nil)
				return p7.TextResult(id), err
			}},
		{Name: "chat_await", Description: "Wait for the selected child chats to finish or ask for attention and return their summaries. Omit ids for all children. A timeout returns pending children too. Results and attention also automatically queue a parent turn without awaiting.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"ids":{"type":"array","items":{"type":"string"},"maxItems":16},"timeout_seconds":{"type":"integer","minimum":1,"maximum":600}},"additionalProperties":false}`),
			Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
				var args struct {
					IDs     []string `json:"ids"`
					Timeout int      `json:"timeout_seconds"`
				}
				if err := json.Unmarshal(call.Arguments, &args); err != nil {
					return p7.ToolResult{}, err
				}
				if args.Timeout < 0 || args.Timeout > 600 {
					return p7.ToolResult{}, fmt.Errorf("timeout_seconds must be 1 to 600")
				}
				if len(args.IDs) == 0 {
					for _, child := range state.Children {
						args.IDs = append(args.IDs, child.Id)
					}
				}
				if len(args.IDs) == 0 {
					return p7.ToolResult{}, fmt.Errorf("no children to await")
				}
				for _, id := range args.IDs {
					if !slices.ContainsFunc(state.Children, func(c *Child) bool { return c.Id == id }) {
						return p7.ToolResult{}, fmt.Errorf("%s is not this chat's child", id)
					}
				}
				if state.awaiting {
					return p7.ToolResult{}, fmt.Errorf("another chat_await is already in progress; await all desired IDs together")
				}
				state.awaiting = true
				defer func() { state.awaiting = false }()
				awaitCtx, cancelAwait := workflow.WithCancel(ctx)
				defer cancelAwait()
				if state.modelActive {
					number := state.modelTurnNumber
					workflow.Go(awaitCtx, func(watchCtx workflow.Context) {
						_ = workflow.Await(watchCtx, func() bool { return !state.modelActive || state.modelTurnNumber != number || state.Closing })
						cancelAwait()
					})
				}
				resume := len(state.ModelLeases) > 0
				releaseModel(ctx, state)
				timeout := time.Duration(args.Timeout) * time.Second
				if timeout == 0 {
					timeout = 10 * time.Minute
				}
				_, err := workflow.AwaitWithTimeout(awaitCtx, timeout, func() bool {
					if state.Closing {
						return true
					}
					for _, child := range state.Children {
						if slices.Contains(args.IDs, child.Id) && !child.Done && !child.Attention {
							return false
						}
					}
					return true
				})
				if err != nil {
					return p7.ToolResult{}, err
				}
				if state.Closing {
					return p7.ToolResult{}, fmt.Errorf("chat is closing")
				}
				if resume {
					if err := acquireModel(awaitCtx, state); err != nil {
						return p7.ToolResult{}, err
					}
				}
				result := []*Child{}
				for _, child := range state.Children {
					if slices.Contains(args.IDs, child.Id) {
						result = append(result, child)
					}
				}
				raw, err := json.Marshal(result)
				return p7.TextResult(string(raw)), err
			}},
		{Name: "chat_message", Description: "Send guidance to one of this chat's live children. Cannot message arbitrary chats.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"id":{"type":"string"},"message":{"type":"string","maxLength":65536}},"required":["id","message"],"additionalProperties":false}`),
			Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
				var args struct {
					ID      string `json:"id"`
					Message string `json:"message"`
				}
				if err := json.Unmarshal(call.Arguments, &args); err != nil {
					return p7.ToolResult{}, err
				}
				if strings.TrimSpace(args.Message) == "" || len(args.Message) > 64<<10 {
					return p7.ToolResult{}, fmt.Errorf("message must be nonempty and at most 64 KiB")
				}
				for index, child := range state.Children {
					if child.Id != args.ID || child.Done {
						continue
					}
					id, err := NewID(ctx)
					if err != nil {
						return p7.ToolResult{}, err
					}
					state.Children[index].Attention = false
					state.Children[index].Message = id
					err = workflow.SignalExternalWorkflow(ctx, WorkflowID(child.Id), "", SignalMessage, &Message{Id: id, Text: args.Message, Restricted: state.Request.Restricted}).Get(ctx, nil)
					return p7.TextResult("Guidance sent."), err
				}
				return p7.ToolResult{}, fmt.Errorf("not a live child of this chat")
			}},
		{Name: "chat_attention", Description: "A child calls this when it needs its parent's guidance instead of completing the task. End the turn with the question. Only child chats can call it.", InputSchema: json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
			Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
				if state.Request.Parent == "" {
					return p7.ToolResult{}, fmt.Errorf("only child chats may request parent attention")
				}
				state.Attention = true
				return p7.TextResult("End this turn with the question for your parent."), nil
			}},
	}
}

func spawnChild(ctx workflow.Context, state *State, task, title string, escalation *Escalation) (string, error) {
	return spawnChildMessage(ctx, state, task, title, escalation, nil)
}

// spawnBounds is why a chat refuses to spawn a child, from its state alone.
func spawnBounds(state *State, task, title string) error {
	if strings.TrimSpace(task) == "" || len(task) > 64<<10 || len(title) > 120 {
		return fmt.Errorf("task must be nonempty and at most 64 KiB, title at most 120 bytes")
	}
	if state.Closing || state.Request.Depth >= MaxChildDepth || liveChildren(state) >= MaxLiveChildren || len(state.Children) >= MaxChildren {
		return fmt.Errorf("chat is closing or its child depth/live/total bound is reached")
	}
	return nil
}

// validateSpawn rejects a manual spawn that the chat would refuse, before the
// update enters history. It runs only on a live update, never in a replay, so
// it needs no version. The handler checks again, because two updates in one
// task are validated before either runs.
func validateSpawn(ctx workflow.Context, state *State, request *navi.Spawn) error {
	if request == nil || request.By.GetLogin() == "" {
		return errors.New("manual spawn requires a person")
	}
	if err := spawnBounds(state, request.Task, request.Title); err != nil {
		return err
	}
	if state.Request.Handler || upgrade.Due(ctx) {
		return fmt.Errorf("the chat is a handler or is draining for continue-as-new")
	}
	return nil
}

func spawnChildMessage(ctx workflow.Context, state *State, task, title string, escalation *Escalation, guidance *navi.Spawn) (string, error) {
	if err := spawnBounds(state, task, title); err != nil {
		return "", err
	}
	modern := workflow.GetVersion(ctx, versionSubchatSpawn, workflow.DefaultVersion, 1) >= 1
	if modern && (state.Request.Handler || upgrade.Due(ctx)) {
		return "", fmt.Errorf("the chat is a handler or is draining for continue-as-new")
	}
	id, err := NewID(ctx)
	if err != nil {
		return "", err
	}
	if title == "" {
		title = "Child task"
	}
	// The child's request starts as a copy of its parent's, which it must not change.
	request := proto.CloneOf(state.Request)
	request.Id, request.Title, request.Parent, request.Owner = id, title, state.Request.Id, workflow.GetInfo(ctx).WorkflowExecution.ID
	request.Depth++
	request.Issue, request.Escalation = nil, escalation
	// A child of a chat that answers a request does not answer it: the
	// answer is the parent's.
	request.ProjectRequest = nil
	// A child holds its parent's scope and may only narrow it.
	request.Scope = InheritScope(state.Request.Scope, nil)
	request.Repositories = ScopedRepositories(state.Request.Repositories, request.Scope)
	// The child's pool is named from its own request when it opens its session
	// (withPool). Nothing in the name differs from its parent's, except for an
	// escalation, or the issue that the child does not work on: those are
	// other permissions and so another pool, never the parent's.
	request.Pool = ""
	message := &Message{Id: id, Text: task, Restricted: request.Restricted}
	if guidance != nil {
		request.StartedBy = proto.CloneOf(guidance.By)
		request.Restricted = request.Restricted || guidance.Restricted
		message.From, message.Restricted = proto.CloneOf(guidance.By), request.Restricted
	}
	index := len(state.Children)
	state.Children = append(state.Children, &Child{Id: id, Title: title, Message: id})
	// A task belongs to the chat, not the tool update or the spawning turn.
	childRoot := ctx
	if modern {
		childRoot, _ = workflow.NewDisconnectedContext(ctx)
	}
	childCtx := workflow.WithChildOptions(childRoot, workflow.ChildWorkflowOptions{
		WorkflowID: WorkflowID(id), TaskQueue: TaskQueue,
		ParentClosePolicy:     enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		TypedSearchAttributes: SearchAttributes(request), Memo: Memo(request),
		WorkflowExecutionTimeout: 24 * time.Hour,
		WaitForCancellation:      modern,
	})
	future := workflow.ExecuteChildWorkflow(childCtx, WorkflowName, &navi.ChatState{Request: request, Pending: []*Message{message}})
	workflow.Go(childRoot, func(ctx workflow.Context) {
		var result *Report
		if err := future.Get(ctx, &result); err != nil {
			result = &Report{Error: Cause(err)}
		}
		if result == nil {
			result = &Report{}
		}
		result.ChatId = id
		if modern {
			result = boundedChildReport(result)
		}
		state.Children[index].Done = true
		state.Children[index].Attention = false
		state.Children[index].Result = result
		wakeForChild(ctx, state, state.Children[index], "result", result)
	})
	if err := future.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
		return "", err
	}
	return id, nil
}

func boundedChildReport(result *Report) *Report {
	notice := proto.CloneOf(result)
	notice.Text = prompt.Truncate(notice.Text, 8<<10)
	notice.Error = prompt.Truncate(notice.Error, 4<<10)
	return notice
}

// wakeForChild uses the ordinary pending queue, never a concurrent model turn.
func wakeForChild(ctx workflow.Context, state *State, child *Child, event string, result *Report) {
	if state.Closing || workflow.GetVersion(ctx, versionSubchatWake, workflow.DefaultVersion, 1) < 1 {
		return
	}
	notice := boundedChildReport(result)
	raw, err := json.Marshal(notice)
	if err != nil {
		workflow.GetLogger(ctx).Error("encode child notice", "child", child.Id, "error", err)
		return
	}
	text := prompt.New(prompt.Vars{"Child": child.Id, "Event": event}).Template(childNoticeTemplate).Fence("json", string(raw))
	if err := text.Err(); err != nil {
		workflow.GetLogger(ctx).Error("build child notice", "child", child.Id, "error", err)
		return
	}
	id := "child-" + child.Id + "-" + child.Message + "-" + event
	if result.GetEscalation() != nil {
		id += "-" + result.Escalation.Id
	}
	// A child's report derives from content that no person vetted, so the turn
	// it starts never holds the tools that need a person's own message.
	restricted := state.Request.Restricted
	if workflow.GetVersion(ctx, versionSubchatWakeRestricted, workflow.DefaultVersion, 1) >= 1 {
		restricted = true
	}
	state.Pending = append(state.Pending, &Message{Id: id, Text: text.String(), Restricted: restricted})
	state.childPaused = false
}

func takeChildReport(ctx workflow.Context, state *State, result *Report) {
	for _, child := range state.Children {
		if child.Id != result.GetChatId() || child.Done {
			continue
		}
		if workflow.GetVersion(ctx, versionSubchatWake, workflow.DefaultVersion, 1) < 1 {
			if child.Message == result.GetMessage() && result.GetAttention() {
				child.Attention, child.Result = true, result
			}
			return
		}
		if result.GetStarted() && result.GetMessage() != "" {
			child.Message, child.Attention, child.Result = result.Message, false, nil
			return
		}
		if result.GetMessage() != "" && child.Message != result.Message {
			return
		}
		if !result.GetAttention() && !result.GetStopped() && !navi.EscalationWaiting(result.GetEscalation().GetStatus()) {
			return
		}
		if proto.Equal(child.Result, result) {
			return
		}
		// An escalation is announced before the turn ends. Do not wake twice for it.
		if child.Attention && result.GetEscalation() == nil && !result.GetStopped() {
			child.Result = result
			return
		}
		child.Attention, child.Result = true, boundedChildReport(result)
		wakeForChild(ctx, state, child, "attention", result)
	}
}

func acquireModel(ctx workflow.Context, state *State) error {
	// Different models on the same PDP account share a slot. Fallback lists
	// hold every account in sorted order, so they cannot evade the bound.
	names := []string{}
	for _, entry := range state.Request.Models {
		for _, model := range strings.Split(entry, ",") {
			account, _, _ := strings.Cut(strings.TrimSpace(model), "/")
			if account != "" && !slices.Contains(names, account) {
				names = append(names, account)
			}
		}
	}
	if len(names) == 0 {
		names = append(names, "default")
	}
	slices.Sort(names)
	for _, name := range names {
		lease, err := p7.AcquireLimit(ctx, "model:"+name, 0)
		if err != nil {
			releaseModel(ctx, state)
			return err
		}
		state.ModelLeases = append(state.ModelLeases, navi.LeaseMessage(lease))
	}
	return nil
}

func releaseModel(ctx workflow.Context, state *State) {
	leases := state.ModelLeases
	state.ModelLeases = nil
	for _, lease := range leases {
		navi.LeaseOf(lease).Release(ctx)
	}
}

func modelTurn(ctx workflow.Context, state *State, prompt p7.TextPrompt) (p7.Turn, error) {
	state.modelTurnNumber++
	state.modelActive = true
	defer func() { state.modelActive = false }()
	// Version 1 took a p7.limit lease per PDP account for each turn. Version 2
	// drops it: the providers' own rate limits bound concurrency. Version 1
	// stays so that histories recorded with leases replay.
	if workflow.GetVersion(ctx, "chat-model-limit", workflow.DefaultVersion, 2) != 1 {
		return p7.PromptText(ctx, state.session, prompt)
	}
	if err := acquireModel(ctx, state); err != nil {
		return p7.Turn{}, err
	}
	defer releaseModel(ctx, state)
	return p7.PromptText(ctx, state.session, prompt)
}

func closeChildren(ctx workflow.Context, state *State) {
	for _, child := range state.Children {
		if !child.Done {
			_ = workflow.SignalExternalWorkflow(ctx, WorkflowID(child.Id), "", SignalClose, &Action{}).Get(ctx, nil)
		}
	}
}
