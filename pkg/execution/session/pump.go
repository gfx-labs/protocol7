package session

import (
	"context"
	"log/slog"
	"sync"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/acpshim"
	"github.com/gfx-labs/protocol7/pkg/execution/journal"
)

// Publisher sends journal frames to the live output stream.
type Publisher interface {
	// LastSeq is the sequence number of the last frame of a session that the
	// stream has, or 0 if it has none.
	LastSeq(ctx context.Context, session string) (int64, error)
	// Publish adds one frame. Publishing a frame twice must be harmless.
	Publish(ctx context.Context, session string, frame domain.Frame) error
}

// pump reads a session's journal from its start, follows one turn through it
// and sends the frames the stream does not have yet.
//
// The journal is the source. The stream is a copy for viewers, so a frame
// that cannot be published is kept and sent again later, and never fails the
// turn.
type pump struct {
	session   string
	dir       string
	publisher Publisher
	log       *slog.Logger

	mu      sync.Mutex
	decoder *journal.Decoder
	turn    *journal.Turn
	offset  int64
	// published is the last sequence number the stream has.
	published int64
	pending   []domain.Frame
	lastSeq   int64
}

func newPump(ctx context.Context, m *Manager, session, turn string) *pump {
	p := &pump{
		session: session, dir: m.shimDir(session), publisher: m.config.Publisher, log: m.log,
		decoder: journal.NewDecoder(session), turn: journal.NewTurn(turn),
	}
	if p.publisher == nil {
		return p
	}
	last, err := p.publisher.LastSeq(ctx, session)
	if err != nil {
		// Publish what the journal has from its start. A frame the stream
		// already has is dropped there.
		p.log.Warn("read the stream's last frame", "session", session, "error", err)
	}
	p.published = last
	return p
}

// catchUp takes the frames the journal gained since the last call.
func (p *pump) catchUp(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	frames, next, err := acpshim.ReadJournal(p.dir, p.offset)
	if err != nil {
		return err
	}
	p.offset = next
	for _, frame := range frames {
		p.take(ctx, frame)
	}
	return nil
}

// follow takes frames as the journal grows, until ctx ends. It calls ended
// once, when the turn has ended.
func (p *pump) follow(ctx context.Context, ended func()) error {
	p.mu.Lock()
	offset := p.offset
	p.mu.Unlock()
	var once sync.Once
	return acpshim.FollowJournal(ctx, p.dir, offset, func(frame domain.Frame) {
		p.mu.Lock()
		p.take(ctx, frame)
		done := p.turn.Ended()
		p.mu.Unlock()
		if done {
			once.Do(ended)
		}
	})
}

// take handles one frame. The caller holds p.mu.
func (p *pump) take(ctx context.Context, frame domain.Frame) {
	if frame.Seq <= p.lastSeq {
		return
	}
	p.lastSeq = frame.Seq
	for _, event := range p.decoder.Decode(frame) {
		p.turn.Observe(event)
	}
	if p.publisher == nil || frame.Seq <= p.published {
		return
	}
	p.pending = append(p.pending, frame)
	p.flush(ctx)
}

// flush publishes pending frames in order and stops at the first that fails.
func (p *pump) flush(ctx context.Context) {
	for len(p.pending) > 0 {
		frame := p.pending[0]
		if err := p.publisher.Publish(ctx, p.session, frame); err != nil {
			p.log.Warn("publish a journal frame", "session", p.session, "seq", frame.Seq, "error", err)
			return
		}
		p.published = frame.Seq
		p.pending = p.pending[1:]
	}
}

func (p *pump) started() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.turn.Started()
}

func (p *pump) ended() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.turn.Ended()
}

func (p *pump) result() domain.TurnResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.turn.Result()
}
