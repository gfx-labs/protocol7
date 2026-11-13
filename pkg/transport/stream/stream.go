// Package stream carries session journal frames to viewers over NATS
// JetStream. One stream holds the frames of every session, each session on
// its own subject. The journal on disk stays the source: the stream is a
// copy that a viewer on another machine can read.
package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gfx-labs/protocol7/pkg/domain"
)

const (
	// Name is the JetStream stream that holds session frames.
	Name = "P7_SESSIONS"
	// subjects matches the subject of every session.
	subjects = "p7.session.*.out"
	// retention is how long frames are kept. Nothing archives them yet, so
	// this is the only limit.
	retention = 30 * 24 * time.Hour
	// duplicateWindow is how long the server remembers message IDs. A frame
	// published again within it is dropped.
	duplicateWindow = 10 * time.Minute
)

// Subject is the subject of one session's frames. A session ID is one
// subject token: it holds no dot.
func Subject(session string) string { return "p7.session." + session + ".out" }

// Stream is a connection to the session frame stream.
type Stream struct {
	url        string
	connection *nats.Conn
	jetstream  jetstream.JetStream
	stream     jetstream.Stream
}

// New returns a stream on the NATS server at url. It is not connected:
// call Connect before any other method.
func New(url string) *Stream { return &Stream{url: url} }

var errNotConnected = errors.New("the session frame stream is not connected")

// Connect dials NATS and creates the stream if it does not exist.
func (s *Stream) Connect(ctx context.Context) error {
	connection, err := nats.Connect(s.url, nats.Name("protocol7"), nats.MaxReconnects(-1))
	if err != nil {
		return fmt.Errorf("connect to NATS at %s: %w", s.url, err)
	}
	js, err := jetstream.New(connection)
	if err != nil {
		connection.Close()
		return fmt.Errorf("open JetStream: %w", err)
	}
	created, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       Name,
		Subjects:   []string{subjects},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		MaxAge:     retention,
		Duplicates: duplicateWindow,
	})
	if err != nil {
		connection.Close()
		return fmt.Errorf("create stream %s: %w", Name, err)
	}
	s.connection, s.jetstream, s.stream = connection, js, created
	return nil
}

// Close ends the connection. Closing a stream that never connected does
// nothing.
func (s *Stream) Close() {
	if s.connection != nil {
		s.connection.Close()
	}
}

// Publish adds one frame of a session. The message ID is the session and the
// frame's sequence number, so a frame published twice is stored once while
// the server remembers the ID.
func (s *Stream) Publish(ctx context.Context, session string, frame domain.Frame) error {
	if s.stream == nil {
		return errNotConnected
	}
	payload, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("encode frame %d of session %s: %w", frame.Seq, session, err)
	}
	id := session + "/" + strconv.FormatInt(frame.Seq, 10)
	if _, err := s.jetstream.Publish(ctx, Subject(session), payload, jetstream.WithMsgID(id)); err != nil {
		return fmt.Errorf("publish frame %d of session %s: %w", frame.Seq, session, err)
	}
	return nil
}

// LastSeq is the sequence number of the last frame of a session in the
// stream, or 0 if it has none.
func (s *Stream) LastSeq(ctx context.Context, session string) (int64, error) {
	if s.stream == nil {
		return 0, errNotConnected
	}
	message, err := s.stream.GetLastMsgForSubject(ctx, Subject(session))
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read last frame of session %s: %w", session, err)
	}
	var frame domain.Frame
	if err := json.Unmarshal(message.Data, &frame); err != nil {
		return 0, fmt.Errorf("decode last frame of session %s: %w", session, err)
	}
	return frame.Seq, nil
}

// Read calls handle for every frame of a session, from its first frame on,
// and then for each new frame as it arrives, until ctx ends or handle
// returns an error. Frames arrive in order. A frame that was stored twice,
// after the server forgot its ID, is passed once.
func (s *Stream) Read(ctx context.Context, session string, handle func(domain.Frame) error) error {
	if s.stream == nil {
		return errNotConnected
	}
	consumer, err := s.jetstream.OrderedConsumer(ctx, Name, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{Subject(session)},
		DeliverPolicy:  jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return fmt.Errorf("read session %s: %w", session, err)
	}
	messages, err := consumer.Messages()
	if err != nil {
		return fmt.Errorf("read session %s: %w", session, err)
	}
	stop := context.AfterFunc(ctx, messages.Stop)
	defer stop()
	defer messages.Stop()

	var last int64
	for {
		message, err := messages.Next()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, jetstream.ErrMsgIteratorClosed) {
				return nil
			}
			return fmt.Errorf("read session %s: %w", session, err)
		}
		var frame domain.Frame
		if err := json.Unmarshal(message.Data(), &frame); err != nil {
			return fmt.Errorf("decode a frame of session %s: %w", session, err)
		}
		if frame.Seq <= last {
			continue
		}
		last = frame.Seq
		if err := handle(frame); err != nil {
			return err
		}
	}
}
