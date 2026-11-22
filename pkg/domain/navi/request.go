package navi

import (
	naviv1 "github.com/gfx-labs/protocol7/pkg/transport/naviapi/navi/v1"
)

// Project requests. A chat of one project asks another project for something
// it cannot do itself, and the other project decides. The request is a
// workflow in the asking project's namespace (RequestWorkflow). navi is the
// only party that reaches both namespaces: it checks the asking project may
// ask the target, starts or continues a chat in the target project, and hands
// that chat's answers back to the request workflow, which tells the chat that
// asked.
const (
	// RequestWorkflow is a chat's request to another project.
	RequestWorkflow = "selfdev.ProjectRequest"
	// SignalProjectRequest is what a request sends the chat that asked, on
	// each change. It carries a ProjectRequestNotice.
	SignalProjectRequest = "project-request"
	// QueryProjectRequest returns a request's ProjectRequestState.
	QueryProjectRequest = "project-request"
	// UpdateProjectRequestDeliver is navi's: the target project's chat
	// started. It carries a ProjectDelivery.
	UpdateProjectRequestDeliver = "project-request-deliver"
	// UpdateProjectRequestReply is navi's: the target chat answered, or navi
	// refused the request. It carries a ProjectReply.
	UpdateProjectRequestReply = "project-request-reply"
	// KindProjectRequest is what AttributeKind says of a request workflow.
	KindProjectRequest Kind = "project-request"
	// MemoTarget is the memo field that names the project a request asks.
	MemoTarget = "target"
	// RequestChatPrefix starts the id of a chat that a request started in the
	// target project.
	RequestChatPrefix = "pr-"
)

// ProjectRequestStatus is the state of a request. ProjectRequestState.Status
// and AttributeEscalationState hold it.
type ProjectRequestStatus = string

const (
	// RequestOpen is a request that navi has not delivered.
	RequestOpen = "open"
	// RequestDelivered is a request that a chat of the target project works
	// on and has not answered.
	RequestDelivered = "delivered"
	// RequestAccepted is a request that the target project took: it says what
	// it will deliver, and answers again when that is done.
	RequestAccepted = "accepted"
	// RequestDone is an accepted request that the target project delivered.
	RequestDone = "done"
	// RequestRedirected is a request that the target project answered with
	// what to do instead.
	RequestRedirected = "redirected"
	// RequestDeclined is a request that was refused, with a reason.
	RequestDeclined = "declined"
	// RequestWithdrawn is a request whose chat ended before it was answered.
	RequestWithdrawn = "withdrawn"
)

// RequestWaiting says whether the chat that asked still waits on a request in
// status: for an answer, or for the delivery of what the target accepted.
func RequestWaiting(status ProjectRequestStatus) bool {
	switch status {
	case RequestOpen, RequestDelivered, RequestAccepted:
		return true
	}
	return false
}

// ReplyOutcome is what a target's answer says. ProjectReply.Outcome holds it.
type ReplyOutcome = string

const (
	ReplyAccepted   = RequestAccepted
	ReplyRedirected = RequestRedirected
	ReplyDeclined   = RequestDeclined
	ReplyDone       = RequestDone
)

// ReplyTerminal says whether an outcome ends the request.
func ReplyTerminal(outcome ReplyOutcome) bool {
	return outcome == ReplyRedirected || outcome == ReplyDeclined || outcome == ReplyDone
}

// ProjectRequestWorkflowID is the workflow ID of a request.
func ProjectRequestWorkflowID(id string) string { return "project-request-" + id }

// RequestChatID is the id of the chat that works on a request in the target
// project: it names the project that asked, so that two projects' request ids
// cannot meet.
func RequestChatID(origin, id string) string { return RequestChatPrefix + origin + "-" + id }

// ProjectRequest is what a chat asks of another project.
type ProjectRequest = naviv1.ProjectRequest

// ProjectRequestState is the request workflow's input, result and query
// answer.
type ProjectRequestState = naviv1.ProjectRequestState

// ProjectDelivery is the payload of UpdateProjectRequestDeliver.
type ProjectDelivery = naviv1.ProjectDelivery

// ProjectReply is the payload of UpdateProjectRequestReply, and what a request
// chat records.
type ProjectReply = naviv1.ProjectReply

// ProjectRequestNotice is the payload of SignalProjectRequest.
type ProjectRequestNotice = naviv1.ProjectRequestNotice

// ProjectRequestRef is what a chat keeps of a request it sent.
type ProjectRequestRef = naviv1.ProjectRequestRef

// ProjectOrigin names the request that started a chat in the target project.
type ProjectOrigin = naviv1.ProjectOrigin
