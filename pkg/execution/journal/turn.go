package journal

import (
	"strings"

	"github.com/gfx-labs/protocol7/pkg/domain"
)

// Turn follows one turn through a session's events and builds its result.
type Turn struct {
	id      string
	started bool
	ended   bool
	text    strings.Builder
	result  domain.TurnResult
}

// NewTurn returns a tracker for the turn with the given ID.
func NewTurn(id string) *Turn { return &Turn{id: id, result: domain.TurnResult{Turn: id}} }

// Started reports whether the turn's prompt is in the journal.
func (t *Turn) Started() bool { return t.started }

// Ended reports whether the turn's end is in the journal.
func (t *Turn) Ended() bool { return t.ended }

// Observe takes the next event of the session.
func (t *Turn) Observe(event domain.SessionEvent) {
	if t.ended {
		return
	}
	if !t.started {
		if event.Kind == domain.EventPrompt && event.Turn == t.id {
			t.started = true
			t.result.FirstSeq = event.Seq
			t.result.LastSeq = event.Seq
		}
		return
	}
	t.result.LastSeq = event.Seq
	switch event.Kind {
	case domain.EventMessage:
		room := domain.TextLimit - t.text.Len()
		if len(event.Text) > room {
			t.result.Truncated = true
		}
		t.text.WriteString(cut(event.Text, max(room, 0)))
	case domain.EventTurnEnd:
		t.ended = true
		if event.End != nil {
			t.result.StopReason = event.End.StopReason
			t.result.Failure = event.End.Failure
		}
	}
}

// Result is the turn's result. It is complete once Ended reports true.
func (t *Turn) Result() domain.TurnResult {
	result := t.result
	result.Text = t.text.String()
	return result
}
