// Package activity holds the activities of the core worker.
package activity

import (
	"context"
	"errors"
	"os"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/session"
)

// Error types of failures that a retry cannot fix.
const (
	ErrorSessionClosed   = "SessionClosed"
	ErrorSessionNotFound = "SessionNotFound"
)

// Sessions are the session activities of one machine.
type Sessions struct {
	manager *session.Manager
}

// NewSessions returns the session activities that run on manager.
func NewSessions(manager *session.Manager) *Sessions { return &Sessions{manager: manager} }

// Register adds the session activities to a worker under their fixed names.
func (s *Sessions) Register(w worker.Worker) {
	w.RegisterActivityWithOptions(s.Open, activity.RegisterOptions{Name: domain.ActivityOpenSession})
	w.RegisterActivityWithOptions(s.RunTurn, activity.RegisterOptions{Name: domain.ActivityRunTurn})
	w.RegisterActivityWithOptions(s.Close, activity.RegisterOptions{Name: domain.ActivityCloseSession})
}

// Open opens a session and returns its handle. The handle names this
// worker's task queue, where the session's later activities have to run.
func (s *Sessions) Open(ctx context.Context, request domain.SessionRequest) (domain.SessionHandle, error) {
	handle, err := s.manager.Open(ctx, request)
	if err != nil {
		return handle, classify(err)
	}
	handle.TaskQueue = activity.GetInfo(ctx).TaskQueue
	return handle, nil
}

// RunTurn runs one turn to its end. A request to cancel the activity asks
// the agent to stop the turn, and the turn's result is still returned. A
// worker that shuts down leaves the turn running, and the next attempt
// picks it up.
func (s *Sessions) RunTurn(ctx context.Context, request domain.TurnRequest) (domain.TurnResult, error) {
	result, err := s.manager.RunTurn(ctx, request, session.TurnControl{
		Detach: activity.GetWorkerStopChannel(ctx),
		Beat:   func() { activity.RecordHeartbeat(ctx) },
	})
	return result, classify(err)
}

// Close stops a session's processes.
func (s *Sessions) Close(ctx context.Context, request domain.CloseRequest) error {
	return classify(s.manager.Close(ctx, request))
}

func classify(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, session.ErrClosed):
		return temporal.NewNonRetryableApplicationError(err.Error(), ErrorSessionClosed, err)
	case errors.Is(err, os.ErrNotExist):
		return temporal.NewNonRetryableApplicationError(err.Error(), ErrorSessionNotFound, err)
	default:
		return err
	}
}
