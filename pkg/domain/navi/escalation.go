package navi

import (
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain/scope"
)

// EscalationStatus is the state of an escalation. AttributeEscalationState
// holds it.
type EscalationStatus string

const (
	EscalationOpen      EscalationStatus = "open"
	EscalationHandling  EscalationStatus = "handling"
	EscalationForwarded EscalationStatus = "forwarded"
	EscalationResolved  EscalationStatus = "resolved"
	EscalationDeclined  EscalationStatus = "declined"
)

// Waiting says whether the escalation still waits for an answer.
func (s EscalationStatus) Waiting() bool {
	switch s {
	case EscalationOpen, EscalationHandling, EscalationForwarded:
		return true
	}
	return false
}

// Listed says whether navi lists the escalation as one to act on.
func (s EscalationStatus) Listed() bool { return s.Waiting() || s == EscalationDeclined }

// Outcome is what the handler decided.
type Outcome string

const (
	OutcomeResolved  Outcome = "resolved"
	OutcomeForwarded Outcome = "forwarded"
	OutcomeDeclined  Outcome = "declined"
)

// EscalationDecision is the payload of UpdateEscalationDecide.
type EscalationDecision struct {
	Outcome   Outcome   `json:"outcome"`
	Text      string    `json:"text"`
	Reasoning string    `json:"reasoning,omitempty"`
	Handler   string    `json:"handler,omitempty"`
	Learning  string    `json:"learning,omitempty"`
	At        time.Time `json:"at"`
}

// EscalationReview is the payload of UpdateEscalationReview.
type EscalationReview struct {
	Text       string    `json:"text,omitempty"`
	By         *Person   `json:"by,omitempty"`
	Restricted bool      `json:"restricted,omitempty"`
	At         time.Time `json:"at"`
}

// EscalationSession is what the handler's chat takes from the chat it serves.
type EscalationSession struct {
	Host         string       `json:"host"`
	Backend      string       `json:"backend,omitempty"`
	Model        string       `json:"model,omitempty"`
	Models       []string     `json:"models,omitempty"`
	Repositories []Repository `json:"repositories"`
	Restricted   bool         `json:"restricted,omitempty"`
	Scope        *scope.Scope `json:"scope,omitempty"`
}

// EscalationRequest is what a chat asks for.
type EscalationRequest struct {
	ID        string             `json:"id"`
	Chat      string             `json:"chat"`
	Need      string             `json:"need"`
	Why       string             `json:"why"`
	Tried     string             `json:"tried,omitempty"`
	Next      string             `json:"next,omitempty"`
	Issue     *Issue             `json:"issue,omitempty"`
	Project   string             `json:"project,omitempty"`
	StartedBy *Person            `json:"started_by,omitempty"`
	Title     string             `json:"title"`
	Session   *EscalationSession `json:"session,omitempty"`
}

// EscalationAnswer is the payload of UpdateEscalationResolve.
type EscalationAnswer struct {
	Text       string  `json:"text"`
	By         *Person `json:"by,omitempty"`
	Restricted bool    `json:"restricted,omitempty"`
}

// EscalationClaim is the payload of SignalEscalationHandling.
type EscalationClaim struct {
	Handler string `json:"handler"`
}

// EscalationForward is the payload of SignalEscalationForward.
type EscalationForward struct {
	Note      string `json:"note,omitempty"`
	Reasoning string `json:"reasoning,omitempty"`
}

// EscalationState is what QueryEscalation returns.
type EscalationState struct {
	Request   EscalationRequest   `json:"request"`
	Status    EscalationStatus    `json:"status"`
	Raised    time.Time           `json:"raised"`
	Changed   time.Time           `json:"changed"`
	Handler   string              `json:"handler,omitempty"`
	Note      string              `json:"note,omitempty"`
	Decision  *EscalationDecision `json:"decision,omitempty"`
	Review    *EscalationReview   `json:"review,omitempty"`
	Answer    *EscalationAnswer   `json:"answer,omitempty"`
	Withdrawn bool                `json:"withdrawn,omitempty"`
	Delivered bool                `json:"delivered,omitempty"`
}

// EscalationRef is what a chat keeps of an escalation it raised.
type EscalationRef struct {
	ID         string           `json:"id"`
	Title      string           `json:"title"`
	Status     EscalationStatus `json:"status"`
	Overridden bool             `json:"overridden,omitempty"`
}

// EscalationNotice is the payload of SignalEscalation.
type EscalationNotice struct {
	ID       string            `json:"id"`
	Status   EscalationStatus  `json:"status"`
	Answer   *EscalationAnswer `json:"answer,omitempty"`
	Override bool              `json:"override,omitempty"`
}
