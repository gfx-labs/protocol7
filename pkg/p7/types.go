// Package p7 is the protocol7 library. Consumer workers import it to run
// agent sessions from ordinary Temporal workflows.
//
// What exists is a subset of the design in docs/plan.md, section 7:
// workers, sessions on the host backend with text turns, and a client that
// watches a session. docs/status.md lists what is built.
package p7

import (
	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/snapshot"
)

// BackendHost runs a session as a process on a named machine.
const BackendHost = domain.BackendHost

// BackendKubernetes runs each session in a pod of its own, with its own
// containers and workload identity. A worker update leaves the pods of
// running sessions alone.
const BackendKubernetes = domain.BackendKubernetes

// SnapshotStore says where a core worker keeps snapshots of its sessions'
// directories, in S3-compatible storage: Endpoint, Region, Bucket, Prefix,
// PathStyle and the list Exclude of what a snapshot leaves out. Credentials
// come from the standard AWS environment and credential chain. The zero
// value keeps no snapshots. It is set as coreworker.Config.Snapshots and
// with the --snapshot-* flags of p7 worker, p7 session and p7 pods.
type SnapshotStore = snapshot.Config

// Placement says where a session runs.
type Placement = domain.Placement

// ResolvedInput records what an input resolved to when a session opened.
type ResolvedInput = domain.ResolvedInput

// Turn is the result of one turn of a session.
type Turn = domain.TurnResult

// TurnFailure says why a turn did not end normally.
type TurnFailure = domain.TurnFailure

// Stop reasons of a turn. The first five are ACP's.
const (
	StopEndTurn         = "end_turn"
	StopMaxTokens       = "max_tokens"
	StopMaxTurnRequests = "max_turn_requests"
	StopRefusal         = "refusal"
	StopCancelled       = "cancelled"
	// StopInterrupted: the session's processes went away during the turn.
	StopInterrupted = domain.StopInterrupted
	// StopFailed: the agent answered the prompt with an error.
	StopFailed = domain.StopFailed
)

// SessionEvent is one thing that happened in a session.
type SessionEvent = domain.SessionEvent

// EventKind names what a session event reports.
type EventKind = domain.EventKind

// Kinds of session event.
const (
	EventPrompt     = domain.EventPrompt
	EventMessage    = domain.EventMessage
	EventThought    = domain.EventThought
	EventToolCall   = domain.EventToolCall
	EventToolUpdate = domain.EventToolUpdate
	EventPlan       = domain.EventPlan
	EventUsage      = domain.EventUsage
	EventTurnEnd    = domain.EventTurnEnd
)

// ToolEvent is a tool call or an update to one.
type ToolEvent = domain.ToolEvent

// PlanEntry is one item of an agent's plan.
type PlanEntry = domain.PlanEntry

// Source is where an input comes from. GitClone is the one source built.
type Source interface {
	input(destination string) domain.Input
}

// Input is one thing placed in a session's workspace before it starts.
type Input struct {
	Source Source
	// Destination is a path in the session's workspace, such as "/work".
	Destination string
}

// GitClone is a working clone of a repository, with history, that a session
// can commit in.
type GitClone struct {
	Repository string
	// Ref is a commit, tag or branch. Empty means the default branch. It is
	// resolved to a commit once, when the session opens.
	Ref string
	// Branch, if set, is checked out at Ref, so the session can commit and
	// push without setting anything up.
	Branch string
}

func (g GitClone) input(destination string) domain.Input {
	return domain.Input{
		Kind: domain.InputGitClone, Destination: destination,
		Repository: g.Repository, Ref: g.Ref, Branch: g.Branch,
	}
}

// TurnUsage is what the model requests of a turn used, as the harness
// reported it. Turn.Usage holds it.
type TurnUsage = domain.TurnUsage

// ModelUsage is the usage of one model in a turn.
type ModelUsage = domain.ModelUsage

// UsageEvent is the harness's usage update for one model request.
type UsageEvent = domain.UsageEvent

// Cost is an amount of money in a currency the harness names.
type Cost = domain.Cost
