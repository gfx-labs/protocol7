package navi

import (
	"time"

	naviv1 "github.com/gfx-labs/protocol7/pkg/transport/naviapi/navi/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/gfx-labs/protocol7/pkg/p7"
)

// The chat workflow's payloads are the navi.v1 messages, the same ones
// ChatService serves, so the API and the workflow cannot drift apart.
// Temporal's default data converter encodes them as protobuf JSON.

// Kind says what a chat is to the page. AttributeKind holds it.
type Kind string

const (
	// KindChat is a person's chat, started from the page.
	KindChat Kind = "chat"
	// KindSession is an agent session that another workflow started and talks
	// to. The page shows it read-only.
	KindSession Kind = "session"
	// KindEscalation is an escalation workflow, or an older chat the system
	// opened to ask a person something.
	KindEscalation Kind = "escalation"
	// KindLearning is what AttributeKind says of a learning.
	KindLearning Kind = "learning"
)

// Person is someone who acted through the page, as the tailnet named them:
// Login is the Tailscale login name, such as alice@example.com, and Name the
// display name.
type Person = naviv1.Person

// Repository is one repository cloned into a chat's session. Name is the
// directory the clone is placed in.
type Repository = naviv1.Repository

// Action is the payload of SignalStop and SignalClose. Discard makes a stop
// drop the waiting messages as well.
type Action = naviv1.Action

// Dequeue is the payload of SignalDequeue.
type Dequeue = naviv1.Dequeue

// Archive is the payload of UpdateArchive.
type Archive = naviv1.Archive

// Board is a design board shown beside a chat. Dir is the board's directory,
// relative to the session's workspace.
type Board = naviv1.Board

// Escalation is set on an older chat the system opened to ask a person
// something.
type Escalation = naviv1.Escalation

// Serving names what an escalation handler's chat serves.
type Serving = naviv1.Serving

// Issue names a GitHub issue. Repo is owner/name.
type Issue = naviv1.Issue

// ChatRequest describes a new chat. It is what navi starts a chat with.
// Host names the machine whose core worker runs the session. Backend is
// "kubernetes" for a pod of its own, anything else the host backend on Host.
// Owner is the ID of a workflow that gets a Report after each turn. Project
// is the navi project, empty for the core project. Scope is the chat's
// effective GitHub authority, resolved once by navi.
type ChatRequest = naviv1.ChatRequest

// KindOf is what the chat the request starts is to the page.
func KindOf(request *ChatRequest) Kind {
	switch {
	case request.GetEscalation() != nil:
		return KindEscalation
	case request.GetOwner() != "":
		return KindSession
	default:
		return KindChat
	}
}

// Pick is one element the person picked on a design board.
type Pick = naviv1.Pick

// PickBox is where a picked element is on its artboard.
type PickBox = naviv1.Pick_Box

// Message is the payload of SignalMessage. Its ID is that of the turn that
// answers it.
type Message = naviv1.Message

// Report is the payload of SignalReport, and a chat's last result.
type Report = naviv1.Report

// ChatStatus says what a chat is doing. ChatState.Status holds it.
type ChatStatus = string

// Statuses of a chat.
const (
	ChatOpening   = "opening"
	ChatIdle      = "idle"
	ChatRunning   = "running"
	ChatEscalated = "escalated"
	ChatClosing   = "closing"
	ChatClosed    = "closed"
	ChatFailed    = "failed"
)

// Probe is a probe a chat deployed.
type Probe = naviv1.Probe

// Child is a child chat owned by a chat.
type Child = naviv1.Child

// OperatorBasis is why an operator token was issued. OperatorIssued.Basis
// holds it.
type OperatorBasis = string

const (
	// OperatorBasisStarted is a chat that an admin started in the page.
	OperatorBasisStarted = "started"
	// OperatorBasisIssue is the chat of an issue that an admin labelled.
	OperatorBasisIssue = "issue"
)

// OperatorIssued is one operator token grant. It holds no token.
type OperatorIssued = naviv1.OperatorIssued

// OperatorAccess is the record of a chat's operator grants that navi shows.
type OperatorAccess = naviv1.OperatorAccess

// ChatState is the chat workflow's input and state, and what QueryState
// returns. A new chat starts from a ChatState that holds only its Request
// and its first message in Pending.
type ChatState = naviv1.ChatState

// Lease is a place a session or a model turn holds in a limit.
type Lease = naviv1.Lease

// Session is the chat's open session, as its state keeps it.
type Session = naviv1.Session

// LeaseMessage is the state's record of a p7 lease. Nil stays nil.
func LeaseMessage(lease *p7.Lease) *Lease {
	if lease == nil {
		return nil
	}
	return &Lease{Limit: lease.Limit, Id: lease.ID}
}

// LeaseOf is the p7 lease the state records. Nil stays nil.
func LeaseOf(lease *Lease) *p7.Lease {
	if lease == nil {
		return nil
	}
	return &p7.Lease{Limit: lease.GetLimit(), ID: lease.GetId()}
}

// SessionMessage is the state's record of a p7 session. Nil stays nil.
func SessionMessage(session *p7.Session) *Session {
	if session == nil {
		return nil
	}
	message := &Session{
		Id:        session.ID,
		TaskQueue: session.TaskQueue,
		Lease:     LeaseMessage(session.Lease),
		Reports:   session.Reports,
		Pod:       session.Pod,
	}
	for _, input := range session.Inputs {
		message.Inputs = append(message.Inputs, &naviv1.ResolvedInput{
			Destination: input.Destination, Repository: input.Repository, Ref: input.Ref, Commit: input.Commit,
		})
	}
	if pool := session.Pool; pool != nil {
		message.Pool = &naviv1.SessionPool{
			Name:              pool.Name,
			MaxWorkers:        int32(pool.MaxWorkers),
			SessionsPerWorker: int32(pool.SessionsPerWorker),
			Pod:               &naviv1.PodSpec{Image: pool.Pod.Image, Requests: pool.Pod.Requests, Limits: pool.Pod.Limits},
		}
		if pool.IdleTimeout != 0 {
			message.Pool.IdleTimeout = durationpb.New(pool.IdleTimeout)
		}
	}
	return message
}

// SessionOf is the p7 session the state records, to run turns on and close.
// Nil stays nil.
func SessionOf(message *Session) *p7.Session {
	if message == nil {
		return nil
	}
	session := &p7.Session{
		ID:        message.GetId(),
		TaskQueue: message.GetTaskQueue(),
		Lease:     LeaseOf(message.GetLease()),
		Reports:   message.GetReports(),
		Pod:       message.GetPod(),
	}
	for _, input := range message.GetInputs() {
		session.Inputs = append(session.Inputs, p7.ResolvedInput{
			Destination: input.GetDestination(), Repository: input.GetRepository(), Ref: input.GetRef(), Commit: input.GetCommit(),
		})
	}
	if pool := message.GetPool(); pool != nil {
		session.Pool = &p7.SessionPool{
			Name:              pool.GetName(),
			MaxWorkers:        int(pool.GetMaxWorkers()),
			SessionsPerWorker: int(pool.GetSessionsPerWorker()),
			IdleTimeout:       pool.GetIdleTimeout().AsDuration(),
		}
		session.Pool.Pod.Image = pool.GetPod().GetImage()
		session.Pool.Pod.Requests = pool.GetPod().GetRequests()
		session.Pool.Pod.Limits = pool.GetPod().GetLimits()
	}
	return session
}

// Timestamp is t as a message field. The zero time is unset.
func Timestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// TimeOf is the time a message field holds. Unset is the zero time.
func TimeOf(t *timestamppb.Timestamp) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.AsTime()
}
