package stream_test

import (
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/transport/stream"
)

// A reader gets one session's frames from the first on, each once, then the
// frames published while it reads.
func TestAReaderGetsOneSessionsFramesOnceAndThenNewOnes(t *testing.T) {
	server, err := stream.StartEmbedded("127.0.0.1", -1, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	frames, err := stream.Connect(t.Context(), server.URL())
	if err != nil {
		t.Fatal(err)
	}
	defer frames.Close()

	publish := func(session string, seq int64) {
		t.Helper()
		frame := domain.Frame{Seq: seq, From: "agent", Msg: json.RawMessage(`{}`)}
		if err := frames.Publish(t.Context(), session, frame); err != nil {
			t.Fatal(err)
		}
	}
	publish("a", 1)
	publish("b", 1)
	publish("a", 2)
	publish("a", 2) // a worker that restarted publishes what it is unsure of again

	if last, err := frames.LastSeq(t.Context(), "a"); err != nil || last != 2 {
		t.Errorf("last frame of a: got %d, %v, want 2", last, err)
	}
	if last, err := frames.LastSeq(t.Context(), "none"); err != nil || last != 0 {
		t.Errorf("last frame of a session with no frames: got %d, %v, want 0", last, err)
	}

	done := errors.New("read enough")
	var got []int64
	err = frames.Read(t.Context(), "a", func(frame domain.Frame) error {
		got = append(got, frame.Seq)
		switch frame.Seq {
		case 2:
			publish("a", 3)
		case 3:
			return done
		}
		return nil
	})
	if !errors.Is(err, done) {
		t.Fatal(err)
	}
	if want := []int64{1, 2, 3}; !slices.Equal(got, want) {
		t.Errorf("read frames %v, want %v", got, want)
	}
}
