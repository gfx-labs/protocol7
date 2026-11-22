package chat

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"
	"github.com/gfx-labs/protocol7/pkg/p7"
	"github.com/gfx-labs/protocol7/pkg/support/prompt"
)

// Project requests: a chat asks another project for something it cannot do
// itself, and the other project decides. See navi.RequestWorkflow for the
// parts. This file is the chat's side (the tools, the record workflow) and the
// target chat's side (the reply tool and the reminders).

const (
	// ProjectRequestWorkflowName is the registered name of the request
	// workflow.
	ProjectRequestWorkflowName = navi.RequestWorkflow
	// SignalProjectRequest is what a request sends the chat that asked.
	SignalProjectRequest = navi.SignalProjectRequest
	// QueryProjectRequest returns the request's ProjectRequestState.
	QueryProjectRequest = navi.QueryProjectRequest
	// UpdateProjectRequestDeliver and UpdateProjectRequestReply are navi's.
	UpdateProjectRequestDeliver = navi.UpdateProjectRequestDeliver
	UpdateProjectRequestReply   = navi.UpdateProjectRequestReply
	// KindProjectRequest is what SelfdevKind says of a request.
	KindProjectRequest = navi.KindProjectRequest

	// MaxOpenRequests bounds the requests of one chat that wait at once, and
	// MaxRequests those it may send in all.
	MaxOpenRequests = 4
	MaxRequests     = 16
	// RequestFieldLimit bounds each text of a request and of an answer.
	RequestFieldLimit = 32 << 10

	// requestDeliverDeadline is how long navi has to hand a request to the
	// target project, requestAnswerDeadline how long the target's chat has to
	// answer it (accept, redirect or decline), and requestDoneDeadline how long
	// an accepted request may take to be done. The workflow declines a request
	// that passes one, as navi, and tells the chat. projectRequestExpiry is the
	// workflow's own timeout: past the three, so it never fires first.
	requestDeliverDeadline = time.Hour
	requestAnswerDeadline  = 24 * time.Hour
	requestDoneDeadline    = 7 * 24 * time.Hour
	projectRequestExpiry   = 15 * 24 * time.Hour
	// maxRequestNudges bounds the reminders a request chat gets to answer, and
	// maxAcceptedNudges those it gets to say a request it accepted is done. The
	// work of an accepted request may go on outside the chat, so one reminder is
	// all it gets: the asking project's deadline ends the wait.
	maxRequestNudges  = 3
	maxAcceptedNudges = 1
	// versionProjectRequest gates what changes a running chat's commands for a
	// request chat: the reminders and the close after the answer.
	versionProjectRequest = "project-request"
	// requestSystem names navi where an answer's author would stand.
	requestSystem = "navi"
)

//go:embed request-asker.md
var requestAskerInstructions string

//go:embed request-target.md
var requestTargetInstructions string

//go:embed request-nudge.md
var requestNudgeTemplate string

// ProjectRequest is what a chat asks of another project.
type ProjectRequest = navi.ProjectRequest

// ProjectRequestState is everything the request workflow keeps.
type ProjectRequestState = navi.ProjectRequestState

// ProjectReply is an answer to a request.
type ProjectReply = navi.ProjectReply

// ProjectRequestRef is what a chat keeps of a request it sent.
type ProjectRequestRef = navi.ProjectRequestRef

// ProjectRequestNotice is the payload of SignalProjectRequest.
type ProjectRequestNotice = navi.ProjectRequestNotice

// ProjectDelivery is the payload of UpdateProjectRequestDeliver.
type ProjectDelivery = navi.ProjectDelivery

// ProjectRequestWorkflowID is the workflow ID of a request.
func ProjectRequestWorkflowID(id string) string { return navi.ProjectRequestWorkflowID(id) }

// ProjectRequestSearchAttributes are the search attributes a request workflow
// is started with. The status changes with the workflow, in the attribute an
// escalation keeps its state in.
func ProjectRequestSearchAttributes(request *ProjectRequest) temporal.SearchAttributes {
	updates := []temporal.SearchAttributeUpdate{
		AttributeKind.ValueSet(string(KindProjectRequest)),
		AttributeParent.ValueSet(request.Chat),
		AttributeEscalationState.ValueSet(navi.RequestOpen),
	}
	if request.StartedBy != nil {
		updates = append(updates, AttributeStartedBy.ValueSet(request.StartedBy.Login))
	}
	if request.Issue != nil {
		updates = append(updates, AttributeIssue.ValueSet(fmt.Sprintf("%s#%d", request.Issue.Repo, request.Issue.Number)))
	}
	if request.Project != "" {
		updates = append(updates, AttributeProject.ValueSet(request.Project))
	}
	return temporal.NewSearchAttributes(updates...)
}

// ProjectRequestMemo is the memo a request workflow is started with: what the
// page lists it by.
func ProjectRequestMemo(request *ProjectRequest) map[string]any {
	return map[string]any{MemoTitle: request.Title, MemoReason: oneLine(request.Why, 200), navi.MemoTarget: request.Target, navi.MemoContractVersion: navi.ContractVersion}
}

// takeRequestTargets reads the projects the chat may ask from the memo that
// navi starts it with (navi.ChatInput says why they are not in its input). The
// state keeps them from then on, over a continue-as-new. A chat that started
// with them in its input, or with none, is as it was.
func takeRequestTargets(ctx workflow.Context, state *State) {
	if len(state.Request.GetRequestTargets()) > 0 {
		return
	}
	payload, ok := workflow.GetInfo(ctx).Memo.GetFields()[navi.MemoRequestTargets]
	if !ok {
		return
	}
	var targets []string
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &targets); err != nil {
		return
	}
	state.Request.RequestTargets = targets
}

// requestTools are the tools a chat has for project requests: project_request
// when navi lets its project ask others, and project_request_reply in a chat
// that a request started.
func requestTools(state *State) []p7.Tool {
	var tools []p7.Tool
	if len(state.Request.RequestTargets) > 0 && state.Request.ProjectRequest == nil {
		tools = append(tools, requestTool(state))
	}
	if state.Request.ProjectRequest != nil {
		tools = append(tools, replyTool(state))
	}
	return tools
}

func requestTool(state *State) p7.Tool {
	return p7.Tool{
		Name: "project_request",
		Description: "Ask another project for something this chat cannot do itself, when it needs a change that only that project can make. " +
			"Name the project and say what is needed, why, what you tried and what you will do with the answer. The project's own agent judges the request: it takes the work (accepted, with the issue or pull request it will deliver, and a later answer when it is done), tells you what to do instead (redirected) or declines it with a reason. " +
			"It returns the request's ID at once: end your turn after calling it. The answer arrives in this chat as a message, and the chat waits without using a model slot until then. Use it for work in another project's repositories, never for something you can do or find out yourself, and put no secret in it.",
		InputSchema: requestSchema(state.Request.RequestTargets),
		Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
			var args struct {
				Target  string `json:"target"`
				Need    string `json:"need"`
				Why     string `json:"why"`
				Tried   string `json:"tried"`
				Next    string `json:"next"`
				Follows string `json:"follows"`
			}
			if err := json.Unmarshal(call.Arguments, &args); err != nil {
				return p7.ToolResult{}, err
			}
			id, err := raiseProjectRequest(ctx, state, &ProjectRequest{
				Target: args.Target, Need: args.Need, Why: args.Why, Tried: args.Tried, Next: args.Next, Follows: args.Follows,
			})
			if err != nil {
				return p7.ToolResult{}, err
			}
			return p7.TextResult(RequestedText(id, args.Target)), nil
		},
	}
}

// requestSchema is the input schema of project_request, with the projects the
// chat may ask as the choices of target. It is encoded, not assembled from
// text, so a project name cannot break out of its string.
func requestSchema(targets []string) json.RawMessage {
	text := func(description string) map[string]any {
		return map[string]any{"type": "string", "maxLength": RequestFieldLimit, "description": description}
	}
	raw, err := json.Marshal(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"target":  map[string]any{"type": "string", "enum": targets, "description": "The project that is asked."},
			"need":    text("What is needed."),
			"why":     text("Why this chat cannot do it or get it itself."),
			"tried":   text("What it already tried."),
			"next":    text("What it will do with the answer."),
			"follows": map[string]any{"type": "string", "description": "The ID of an earlier request of this chat to the same project, when this one continues it. The project starts a new chat for this request, tells it that it continues the earlier one, and answers it by itself."},
		},
		"required":             []string{"target", "need", "why"},
		"additionalProperties": false,
	})
	if err != nil {
		panic(err) // strings and a fixed shape always encode
	}
	return raw
}

// RequestedText is what the agent is told after it sent a request.
func RequestedText(id, target string) string {
	return fmt.Sprintf("Request %s is sent to the project %s, whose agent decides. End your turn now. The answer arrives in this chat as a message, and you go on from it. Do not poll or wait for it.", id, target)
}

// validateRequest refuses a request that lacks what the other project's agent
// needs to judge it.
func validateRequest(r *ProjectRequest) error {
	for name, text := range map[string]string{"need": r.Need, "why": r.Why, "tried": r.Tried, "next": r.Next} {
		if len(text) > RequestFieldLimit {
			return fmt.Errorf("%s is longer than %d bytes", name, RequestFieldLimit)
		}
	}
	if r.Need == "" || r.Why == "" {
		return fmt.Errorf("a request says what is needed and why this chat cannot get it itself")
	}
	return nil
}

// raiseProjectRequest starts the request's workflow for the chat and returns
// its ID. The request is a workflow of its own: it outlives a turn and a
// continue-as-new of the chat, and tells the chat by signal.
func raiseProjectRequest(ctx workflow.Context, state *State, request *ProjectRequest) (string, error) {
	request.Target, request.Follows = strings.TrimSpace(request.Target), strings.TrimSpace(request.Follows)
	request.Need, request.Why = strings.TrimSpace(request.Need), strings.TrimSpace(request.Why)
	request.Tried, request.Next = strings.TrimSpace(request.Tried), strings.TrimSpace(request.Next)
	if !slices.Contains(state.Request.RequestTargets, request.Target) {
		return "", fmt.Errorf("this chat's project may not send requests to %q: the projects it may ask are %s", request.Target, strings.Join(state.Request.RequestTargets, ", "))
	}
	if err := validateRequest(request); err != nil {
		return "", err
	}
	if state.Request.Handler || state.Request.ProjectRequest != nil {
		return "", fmt.Errorf("a chat that answers a request does not send requests of its own: answer it, or decline it and say who to ask")
	}
	if state.Closing || openRequests(state) >= MaxOpenRequests || len(state.ProjectRequests) >= MaxRequests {
		return "", fmt.Errorf("the chat is closing, or it has %d requests waiting or has sent %d", MaxOpenRequests, MaxRequests)
	}
	if request.Follows != "" {
		index := slices.IndexFunc(state.ProjectRequests, func(ref *ProjectRequestRef) bool { return ref.Id == request.Follows })
		if index < 0 || state.ProjectRequests[index].Target != request.Target {
			return "", fmt.Errorf("%q is not an earlier request of this chat to %s", request.Follows, request.Target)
		}
		request.FollowsChat = state.ProjectRequests[index].TargetChat
	}
	id, err := NewID(ctx)
	if err != nil {
		return "", err
	}
	request.Id, request.Chat = id, state.Request.Id
	request.Issue, request.Project, request.StartedBy = state.Request.Issue, state.Request.Project, state.Request.StartedBy
	if request.Title == "" {
		request.Title = oneLine(request.Need, 72)
	}
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: ProjectRequestWorkflowID(id), TaskQueue: TaskQueue,
		// The request is not the chat's child in its lifetime: it keeps
		// waiting when the chat continues as new. The chat cancels it when it
		// ends.
		ParentClosePolicy:        enumspb.PARENT_CLOSE_POLICY_ABANDON,
		TypedSearchAttributes:    ProjectRequestSearchAttributes(request),
		Memo:                     ProjectRequestMemo(request),
		WorkflowExecutionTimeout: projectRequestExpiry,
	})
	future := workflow.ExecuteChildWorkflow(childCtx, ProjectRequestWorkflowName, &ProjectRequestState{Request: request})
	if err := future.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
		return "", err
	}
	state.ProjectRequests = append(state.ProjectRequests, &ProjectRequestRef{Id: id, Title: request.Title, Target: request.Target, Status: navi.RequestOpen})
	if state.Status == StatusIdle {
		state.Status = StatusEscalated
	}
	return id, nil
}

// withdrawRequests asks the requests that still wait to end, because the chat
// does.
func withdrawRequests(ctx workflow.Context, state *State) {
	for _, ref := range state.ProjectRequests {
		if navi.RequestWaiting(ref.Status) {
			_ = workflow.RequestCancelExternalWorkflow(ctx, ProjectRequestWorkflowID(ref.Id), "").Get(ctx, nil)
		}
	}
}

// openRequests counts the requests of the chat that wait.
func openRequests(state *State) int {
	count := 0
	for _, ref := range state.ProjectRequests {
		if navi.RequestWaiting(ref.Status) {
			count++
		}
	}
	return count
}

// takeProjectRequest applies what a request told the chat. An answer comes as
// a message, which the chat takes like any other, and which is restricted:
// it derives from another project's agent, which no person of this one vetted.
// It returns the message to queue, if any.
func takeProjectRequest(state *State, notice *ProjectRequestNotice) (*Message, bool) {
	for _, ref := range state.ProjectRequests {
		if ref.Id != notice.Id {
			continue
		}
		reply := notice.Reply
		if reply == nil {
			// A delivery: the chat that works on it, and no answer yet.
			if ref.Status == navi.RequestOpen {
				ref.Status = notice.Status
			}
			ref.TargetChat = cmpFirst(notice.TargetChat, ref.TargetChat)
			return nil, false
		}
		if reply.Seq <= ref.LastReply {
			// A signal that came again.
			return nil, false
		}
		ref.Status, ref.LastReply = notice.Status, reply.Seq
		ref.TargetChat = cmpFirst(notice.TargetChat, ref.TargetChat)
		if state.Status == StatusIdle || state.Status == StatusEscalated {
			state.Status = idleStatus(state)
		}
		text, err := navi.ReplyMessageText(ref, reply)
		if err != nil {
			// The answer's own words stay out of a text that is not fenced.
			text = fmt.Sprintf("The project %s answered your request %s: %s. Its words could not be put in this message: read them on the request's page in navi.", ref.Target, ref.Id, reply.Outcome)
		}
		return &Message{Id: fmt.Sprintf("request-%s-%d", ref.Id, reply.Seq), Text: text, Restricted: true}, true
	}
	return nil, false
}

func cmpFirst(first, second string) string {
	if first != "" {
		return first
	}
	return second
}

// runProjectRequest is one request: a chat asked another project for
// something. It waits for navi, which hands it to the target project and hands
// the target chat's answers back with the updates UpdateProjectRequestDeliver
// and UpdateProjectRequestReply, and it tells the chat what changes.
//
// A new request is open. navi delivers it (delivered), the target's chat
// answers accepted, redirected or declined, and an accepted request ends with
// done or declined. A request navi does not deliver within an hour, or that the
// target's chat does not answer within a day, is declined by the workflow,
// with the reason. A request the chat ends with is withdrawn.
func runProjectRequest(ctx workflow.Context, state *ProjectRequestState) (*ProjectRequestState, error) {
	if state == nil {
		state = &ProjectRequestState{}
	}
	if state.Request == nil {
		state.Request = &ProjectRequest{}
	}
	if err := workflow.SetQueryHandler(ctx, QueryProjectRequest, func() (*ProjectRequestState, error) { return state, nil }); err != nil {
		return state, err
	}
	now := func(ctx workflow.Context) *timestamppb.Timestamp { return navi.Timestamp(workflow.Now(ctx)) }
	if state.Status == "" {
		state.Status, state.Raised, state.Changed = navi.RequestOpen, now(ctx), now(ctx)
	}
	chatID := WorkflowID(state.Request.Chat)
	finished := func() bool { return !navi.RequestWaiting(state.Status) }
	// move records the status, in the search attribute that navi lists by,
	// and tells the chat. The chat having ended is no concern of the request.
	move := func(ctx workflow.Context, status string, reply *ProjectReply) error {
		state.Status, state.Changed = status, now(ctx)
		if err := workflow.UpsertTypedSearchAttributes(ctx, AttributeEscalationState.ValueSet(status)); err != nil {
			return err
		}
		_ = workflow.SignalExternalWorkflow(ctx, chatID, "", SignalProjectRequest, &ProjectRequestNotice{
			Id: state.Request.Id, Status: status, TargetChat: state.TargetChat, Reply: reply,
		}).Get(ctx, nil)
		return nil
	}

	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateProjectRequestDeliver, func(ctx workflow.Context, delivery *ProjectDelivery) (*ProjectRequestState, error) {
		if state.Status != navi.RequestOpen {
			return state, nil
		}
		state.TargetChat, state.DeliveredAt = delivery.TargetChat, now(ctx)
		return state, move(ctx, navi.RequestDelivered, nil)
	}, workflow.UpdateHandlerOptions{Validator: func(_ workflow.Context, delivery *ProjectDelivery) error {
		switch {
		case strings.TrimSpace(delivery.GetTargetChat()) == "":
			return fmt.Errorf("a delivery names the chat that works on the request")
		case state.Status != navi.RequestOpen && state.TargetChat != delivery.TargetChat:
			return fmt.Errorf("this request is %s already", state.Status)
		}
		return nil
	}}); err != nil {
		return state, err
	}

	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateProjectRequestReply, func(ctx workflow.Context, reply *ProjectReply) (*ProjectRequestState, error) {
		reply.Text, reply.Reference = strings.TrimSpace(reply.Text), strings.TrimSpace(reply.Reference)
		reply.Seq, reply.At = int32(len(state.Replies)+1), now(ctx)
		state.Replies = append(state.Replies, reply)
		return state, move(ctx, reply.Outcome, reply)
	}, workflow.UpdateHandlerOptions{Validator: func(_ workflow.Context, reply *ProjectReply) error {
		return validateProjectReply(state, reply)
	}}); err != nil {
		return state, err
	}

	// The deadlines: navi hands the request over, and the target answers it.
	workflow.Go(ctx, func(ctx workflow.Context) {
		if ok, err := workflow.AwaitWithTimeout(ctx, requestDeliverDeadline, func() bool { return state.Status != navi.RequestOpen }); err != nil || ok {
			return
		}
		giveUp(ctx, state, move, "navi did not hand this request to the project "+state.Request.Target+" within "+requestDeliverDeadline.String()+": the project may not be reachable. Try again later or ask a person.")
	})
	workflow.Go(ctx, func(ctx workflow.Context) {
		if err := workflow.Await(ctx, func() bool { return state.Status != navi.RequestOpen }); err != nil {
			return
		}
		if ok, err := workflow.AwaitWithTimeout(ctx, requestAnswerDeadline, func() bool { return state.Status != navi.RequestDelivered }); err != nil {
			return
		} else if !ok {
			giveUp(ctx, state, move, "the project "+state.Request.Target+" did not answer within "+requestAnswerDeadline.String()+".")
			return
		}
		if state.Status != navi.RequestAccepted {
			return
		}
		if ok, err := workflow.AwaitWithTimeout(ctx, requestDoneDeadline, func() bool { return state.Status != navi.RequestAccepted }); err == nil && !ok {
			giveUp(ctx, state, move, "the project "+state.Request.Target+" accepted this request and did not report it done within "+requestDoneDeadline.String()+". Look at what it named as the reference to see how far the work is.")
		}
	})

	if err := workflow.Await(ctx, finished); err != nil {
		// The chat ended, and with it the question.
		if !finished() {
			closeCtx, _ := workflow.NewDisconnectedContext(ctx)
			state.Withdrawn = true
			if err := move(closeCtx, navi.RequestWithdrawn, nil); err != nil {
				return state, err
			}
		}
		return state, nil
	}
	// The answer reaches the chat before the workflow ends.
	if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
		return state, err
	}
	return state, nil
}

// giveUp declines a request that nobody answered, as navi, with the reason.
func giveUp(ctx workflow.Context, state *ProjectRequestState, move func(workflow.Context, string, *ProjectReply) error, reason string) {
	if finished := !navi.RequestWaiting(state.Status); finished {
		return
	}
	reply := &ProjectReply{Seq: int32(len(state.Replies) + 1), Outcome: navi.ReplyDeclined, Text: reason, By: requestSystem, At: navi.Timestamp(workflow.Now(ctx))}
	state.Replies = append(state.Replies, reply)
	_ = move(ctx, navi.RequestDeclined, reply)
}

// validateProjectReply refuses an answer that does not follow from the
// request's state. The first answer of a delivered request is accepted,
// redirected or declined, an accepted request is then done or declined, and
// navi may decline a request it could not deliver.
func validateProjectReply(state *ProjectRequestState, reply *ProjectReply) error {
	if reply == nil {
		return fmt.Errorf("the answer is empty")
	}
	if strings.TrimSpace(reply.Text) == "" {
		return fmt.Errorf("an answer says what the project decided and why")
	}
	if len(reply.Text) > RequestFieldLimit {
		return fmt.Errorf("the answer is longer than %d bytes", RequestFieldLimit)
	}
	if want := int32(len(state.Replies) + 1); reply.Seq != want {
		return fmt.Errorf("the answer is number %d, and the next is %d", reply.Seq, want)
	}
	switch state.Status {
	case navi.RequestOpen:
		if reply.Outcome != navi.ReplyDeclined || reply.By != requestSystem {
			return fmt.Errorf("an undelivered request is only declined, by navi")
		}
	case navi.RequestDelivered:
		if reply.Outcome != navi.ReplyAccepted && reply.Outcome != navi.ReplyRedirected && reply.Outcome != navi.ReplyDeclined {
			return fmt.Errorf("a request that was not answered yet is accepted, redirected or declined, not %s", reply.Outcome)
		}
	case navi.RequestAccepted:
		if reply.Outcome != navi.ReplyDone && reply.Outcome != navi.ReplyDeclined {
			return fmt.Errorf("an accepted request is done or declined, not %s", reply.Outcome)
		}
	default:
		return fmt.Errorf("this request is %s already", state.Status)
	}
	return nil
}

// replyTool is project_request_reply: how the chat that a request started
// answers it. The answer is kept in the chat's state, which navi reads and
// hands to the request in the project that asked.
func replyTool(state *State) p7.Tool {
	return p7.Tool{
		Name: "project_request_reply",
		Description: "Answer the request from another project that started this chat. Outcomes: accepted (you will do the work: give the issue or pull request as the reference), redirected (what the requester should do instead), declined (why), and done (what you accepted is delivered). " +
			"An accepted request is later answered with done or declined. The answer goes to the chat that asked, as a message. The chat closes after redirected, declined or done.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{` +
			`"outcome":{"type":"string","enum":["accepted","redirected","declined","done"]},` +
			`"text":{"type":"string","maxLength":32768,"description":"The answer, written for an agent that cannot see this project's code or chat."},` +
			`"reference":{"type":"string","maxLength":2048,"description":"The issue or pull request the work is delivered with (required for accepted), or the merged pull request or commit for done."}` +
			`},"required":["outcome","text"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
			var args struct {
				Outcome   string `json:"outcome"`
				Text      string `json:"text"`
				Reference string `json:"reference"`
			}
			if err := json.Unmarshal(call.Arguments, &args); err != nil {
				return p7.ToolResult{}, err
			}
			reply := &ProjectReply{Seq: int32(len(state.ProjectReplies) + 1), Outcome: args.Outcome, Text: strings.TrimSpace(args.Text), Reference: strings.TrimSpace(args.Reference), By: "chat " + state.Request.Id}
			if err := validateChatReply(state, reply); err != nil {
				return p7.ToolResult{}, err
			}
			reply.At = navi.Timestamp(workflow.Now(ctx))
			state.ProjectReplies = append(state.ProjectReplies, reply)
			if reply.Outcome == navi.ReplyAccepted {
				// The reminders to say it is done have a budget of their own.
				state.RequestNudges = 0
			}
			if navi.ReplyTerminal(reply.Outcome) {
				return p7.TextResult("Answered. This chat closes when you end your turn."), nil
			}
			return p7.TextResult("Answered: accepted. Deliver the work, then answer again with done. If you cannot deliver it, answer declined and say why."), nil
		},
	}
}

// validateChatReply refuses an answer the request chat may not give now.
func validateChatReply(state *State, reply *ProjectReply) error {
	if state.Request.ProjectRequest == nil {
		return fmt.Errorf("this chat was not started by a request from another project")
	}
	if reply.Text == "" || len(reply.Text) > RequestFieldLimit {
		return fmt.Errorf("an answer says what the project decided and why, in at most %d bytes", RequestFieldLimit)
	}
	var last *ProjectReply
	if n := len(state.ProjectReplies); n > 0 {
		last = state.ProjectReplies[n-1]
	}
	switch {
	case last != nil && navi.ReplyTerminal(last.Outcome):
		return fmt.Errorf("you answered %s already: the chat closes, and the answer is final", last.Outcome)
	case last == nil && reply.Outcome == navi.ReplyDone:
		return fmt.Errorf("accept the request before you say it is done")
	case last != nil && reply.Outcome != navi.ReplyDone && reply.Outcome != navi.ReplyDeclined:
		return fmt.Errorf("you accepted the request: now answer done or declined")
	case reply.Outcome != navi.ReplyAccepted && reply.Outcome != navi.ReplyRedirected && reply.Outcome != navi.ReplyDeclined && reply.Outcome != navi.ReplyDone:
		return fmt.Errorf("the outcome is accepted, redirected, declined or done")
	case reply.Outcome == navi.ReplyAccepted && reply.Reference == "":
		return fmt.Errorf("an accepted request names the issue or pull request that delivers it: say it as the reference")
	}
	return nil
}

// requestTurnEnded is what a request chat does when a turn ends. It reminds
// the agent to answer, a few times, and gives up on a chat that never does by
// declining the request as navi. After a final answer it closes.
func requestTurnEnded(ctx workflow.Context, state *State, stopped bool) {
	if state.Request.ProjectRequest == nil || state.Closing || stopped {
		return
	}
	if workflow.GetVersion(ctx, versionProjectRequest, workflow.DefaultVersion, 1) < 1 {
		return
	}
	var last *ProjectReply
	if n := len(state.ProjectReplies); n > 0 {
		last = state.ProjectReplies[n-1]
	}
	switch {
	case last != nil && navi.ReplyTerminal(last.Outcome):
		if len(state.Pending) == 0 {
			state.Closing = true
		}
	case len(state.Pending) > 0:
	case int(state.RequestNudges) >= maxRequestNudges || last != nil && int(state.RequestNudges) >= maxAcceptedNudges:
		// An accepted request stays open after its one reminder. Nobody is
		// stuck: the asking project's deadline ends the wait.
		if last == nil {
			state.ProjectReplies = append(state.ProjectReplies, &ProjectReply{
				Seq: 1, Outcome: navi.ReplyDeclined, By: requestSystem, At: navi.Timestamp(workflow.Now(ctx)),
				Text: "The agent of this project ended its turns without answering the request.",
			})
			state.Closing = true
		}
	default:
		state.RequestNudges++
		text := prompt.New(prompt.Vars{"Accepted": last != nil}).Template(requestNudgeTemplate)
		if text.Err() != nil {
			return
		}
		state.Pending = append(state.Pending, &Message{Id: fmt.Sprintf("request-nudge-%d", state.RequestNudges), Text: text.String(), Restricted: true})
	}
}

// requestInstructions are added to the hidden context of a chat's first turn,
// after the app's instructions: what a request chat decides and how it
// answers, and which projects a chat may ask.
func requestInstructions(request *Request) string {
	switch {
	case request.ProjectRequest != nil:
		return requestTargetInstructions
	case len(request.RequestTargets) > 0:
		text := prompt.New(prompt.Vars{"Targets": strings.Join(request.RequestTargets, ", ")}).Template(requestAskerInstructions)
		if text.Err() != nil {
			return ""
		}
		return text.String()
	}
	return ""
}
