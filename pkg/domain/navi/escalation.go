package navi

import (
	naviv1 "github.com/gfx-labs/protocol7/pkg/transport/naviapi/navi/v1"
)

// EscalationStatus is the state of an escalation. EscalationState.Status and
// AttributeEscalationState hold it.
type EscalationStatus = string

const (
	EscalationOpen      = "open"
	EscalationHandling  = "handling"
	EscalationForwarded = "forwarded"
	EscalationResolved  = "resolved"
	EscalationDeclined  = "declined"
)

// EscalationWaiting says whether an escalation in status still waits for an
// answer.
func EscalationWaiting(status EscalationStatus) bool {
	switch status {
	case EscalationOpen, EscalationHandling, EscalationForwarded:
		return true
	}
	return false
}

// EscalationListed says whether navi lists an escalation in status as one to
// act on.
func EscalationListed(status EscalationStatus) bool {
	return EscalationWaiting(status) || status == EscalationDeclined
}

// Outcome is what the handler decided. EscalationDecision.Outcome holds it.
type Outcome = string

const (
	OutcomeResolved  = "resolved"
	OutcomeForwarded = "forwarded"
	OutcomeDeclined  = "declined"
)

// EscalationDecision is the payload of UpdateEscalationDecide.
type EscalationDecision = naviv1.EscalationDecision

// EscalationReview is the payload of UpdateEscalationReview.
type EscalationReview = naviv1.EscalationReview

// EscalationSession is what the handler's chat takes from the chat it serves.
type EscalationSession = naviv1.EscalationSession

// EscalationRequest is what a chat asks for.
type EscalationRequest = naviv1.EscalationRequest

// EscalationAnswer is the payload of UpdateEscalationResolve.
type EscalationAnswer = naviv1.EscalationAnswer

// EscalationClaim is the payload of SignalEscalationHandling.
type EscalationClaim = naviv1.EscalationClaim

// EscalationForward is the payload of SignalEscalationForward.
type EscalationForward = naviv1.EscalationForward

// EscalationState is the escalation workflow's input and result, and what
// QueryEscalation returns.
type EscalationState = naviv1.EscalationState

// EscalationRef is what a chat keeps of an escalation it raised.
type EscalationRef = naviv1.EscalationRef

// EscalationNotice is the payload of SignalEscalation.
type EscalationNotice = naviv1.EscalationNotice

// HandlerIntake is the payload of SignalEscalationIntake.
type HandlerIntake = naviv1.HandlerIntake

// ResolvedRecord is what the handler remembers of an escalation it resolved
// or declined, for its loop guard.
type ResolvedRecord = naviv1.ResolvedRecord

// HandlerState is the handler workflow's input and state.
type HandlerState = naviv1.HandlerState
