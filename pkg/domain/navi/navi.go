// Package navi is the contract between navi, the platform's web UI, and an
// app's selfdev worker. navi starts, signals, queries and updates the app's
// workflows by the names here, lists them by the search attributes and memo
// fields here, and decodes the types here from their queries. It holds no
// workflow code, so navi and each app import it without importing each other.
//
// The wire strings equal those of the workflows that already run in the
// Temporal namespaces. Renaming one orphans those workflows: a change that
// renames or removes a name raises ContractVersion.
//
// The chat, escalation and handler payloads are navi.v1 protobuf messages
// (proto/navi/v1), which buf breaking guards. The issue, review and learning
// types are still Go structs here until those flows define their own
// protobuf packages in their app.
package navi

import "github.com/gfx-labs/protocol7/pkg/transport/stream"

// ContractVersion is the version of this contract. A worker writes it to
// MemoContractVersion of each workflow it starts. Adding a name or a field
// keeps it, renaming or removing one raises it. Version 2 made the chat,
// escalation and handler payloads navi.v1 messages.
const ContractVersion = 2

// Workflow types.
const (
	// ChatWorkflow is a conversation between a person and an agent session.
	ChatWorkflow = "selfdev.Chat"
	// EscalationWorkflow is a request of a chat for something outside its
	// reach, which the handler or a person answers.
	EscalationWorkflow = "selfdev.Escalation"
	// HandlerWorkflow takes escalations and decides on them.
	HandlerWorkflow = "selfdev.EscalationHandler"
	// IssueWorkflow works on one GitHub issue.
	IssueWorkflow = "selfdev.Issue"
	// IssueIntakeWorkflow is the one-time check of an issue's opener.
	IssueIntakeWorkflow = "selfdev.IssueIntake"
	// IssueScanWorkflow is the workflow of the schedule the scanner replaced.
	// It stays registered until its runs are gone.
	IssueScanWorkflow = "selfdev.IssueScan"
	// IssueScannerWorkflow finds labelled issues and starts their workflows.
	IssueScannerWorkflow = "selfdev.IssueScanner"
	// ReviewWorkflow reviews and merges one pull request.
	ReviewWorkflow = "selfdev.Review"
	// LearningWorkflow keeps one learning entry.
	LearningWorkflow = "selfdev.Learning"
)

// TaskQueue is the queue of an app's selfdev worker, where every workflow
// above runs. Each app has its own Temporal namespace, so the name is the
// same in each.
const TaskQueue = "selfdev"

// Chat signals, queries and updates.
const (
	// SignalMessage carries a Message from the person.
	SignalMessage = "message"
	// SignalStop stops the running turn. It carries an Action.
	SignalStop = "stop"
	// SignalDequeue removes one waiting message. It carries a Dequeue.
	SignalDequeue = "dequeue"
	// SignalClose ends the chat and stops its session. It carries an Action.
	SignalClose = "close"
	// SignalReport is what a chat sends its owner workflow after each turn
	// and when it ends. It carries a Report.
	SignalReport = "chat-report"
	// QueryState returns the chat's State. The issue and review workflows
	// answer a query of the same name with their own state.
	QueryState = "state"
	// UpdateArchive archives or unarchives a chat. It carries an Archive.
	UpdateArchive = "archive"
	// UpdateSpawn starts a native child and returns its chat ID after start.
	UpdateSpawn = "spawn"
)

// Escalation signals, queries and updates.
const (
	// SignalEscalation is what an escalation sends the chat that raised it on
	// each change. It carries an EscalationNotice.
	SignalEscalation = "escalation"
	// SignalEscalationHandling is sent by the handler when it takes an
	// escalation. It carries an EscalationClaim.
	SignalEscalationHandling = "escalation-handling"
	// SignalEscalationForward is sent by the handler when it gives an
	// escalation to a person. It carries an EscalationForward.
	SignalEscalationForward = "escalation-forward"
	// SignalEscalationIntake is sent to the handler workflow to take a new
	// escalation.
	SignalEscalationIntake = "escalation-intake"
	// QueryEscalation returns an escalation's EscalationState.
	QueryEscalation = "escalation"
	// UpdateEscalationResolve answers an escalation. It carries an
	// EscalationAnswer.
	UpdateEscalationResolve = "escalation-resolve"
	// UpdateEscalationDecide is the handler's decision. It carries an
	// EscalationDecision.
	UpdateEscalationDecide = "escalation-decide"
	// UpdateEscalationReview is a person's review of the handler's decision.
	// It carries an EscalationReview.
	UpdateEscalationReview = "escalation-review"
)

// Issue and scanner signals. The issue workflow answers QueryState with an
// IssueState and the scanner with a ScannerState.
const (
	// SignalIssueWake makes an issue workflow poll at once. The webhook
	// receiver sends it. It carries no data.
	SignalIssueWake = "github-event"
	// SignalScannerWake asks the scanner for a pass at once.
	SignalScannerWake = "wake"
	// SignalScannerPause stops intake.
	SignalScannerPause = "pause"
	// SignalScannerResume starts intake again with a pass at once.
	SignalScannerResume = "resume"
	// SignalScannerTemplate carries a Template for the scanner's next pass.
	SignalScannerTemplate = "update-template"
)

// Review and learning signals, queries and updates. The review workflow
// answers QueryState with a ReviewState.
const (
	// SignalRereview asks a review for a look now.
	SignalRereview = "rereview"
	// QueryLearning returns a learning's Entry.
	QueryLearning = "learning"
	// UpdateLearningDecide accepts, rejects or retires a learning.
	UpdateLearningDecide = "learning-decide"
)

// Memo fields.
const (
	// MemoTitle is the field a chat or an escalation is listed by.
	MemoTitle = "title"
	// MemoReason is an escalation's reason, in one line.
	MemoReason = "reason"
	// MemoOutcome is what the handler decided on an escalation.
	MemoOutcome = "outcome"
	// MemoLearningEntry is a learning's Entry as it was recorded.
	MemoLearningEntry = "entry"
	// MemoContractVersion is the ContractVersion of the worker that started
	// the workflow. A workflow without it predates the field and is version 1.
	MemoContractVersion = "contract"
)

// Workflow IDs that navi addresses directly.
const (
	// IssueScannerID is the workflow ID of the scanner. There is one in each
	// namespace.
	IssueScannerID = "selfdev-issues-scan"
)

// SessionSubject is the NATS subject a session's output frames are published
// on. navi reads it from the app's NATS server and never writes to it.
func SessionSubject(session string) string { return stream.Subject(session) }
