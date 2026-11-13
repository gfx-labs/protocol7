package acpshim

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/gfx-labs/acphost/pkg/acphost"

	"github.com/gfx-labs/protocol7/pkg/domain"
)

// ReadJournal returns the frames of the journal in dir from byte offset
// offset on, and the offset after the last of them. A journal that does not
// exist yet is empty.
func ReadJournal(dir string, offset int64) ([]domain.Frame, int64, error) {
	frames, next, err := acphost.ReadFrom(JournalPath(dir), offset)
	if errors.Is(err, os.ErrNotExist) {
		return nil, offset, nil
	}
	if err != nil {
		return nil, offset, fmt.Errorf("read journal in %s: %w", dir, err)
	}
	out := make([]domain.Frame, len(frames))
	for index, frame := range frames {
		out[index] = fromHostFrame(frame)
	}
	return out, next, nil
}

// FollowJournal calls handle for each frame appended to the journal in dir
// after byte offset offset, until ctx ends.
func FollowJournal(ctx context.Context, dir string, offset int64, handle func(domain.Frame)) error {
	err := acphost.FollowJournal(ctx, JournalPath(dir), offset, func(frame acphost.Frame) {
		handle(fromHostFrame(frame))
	})
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("follow journal in %s: %w", dir, err)
	}
	return nil
}

// Summarize condenses the journal in dir into text that can start a new
// conversation when the harness cannot load the old one.
func Summarize(dir string) string {
	frames, _, err := acphost.ReadJournal(JournalPath(dir), 0)
	if err != nil {
		return ""
	}
	return acphost.Summarize(frames, 0)
}

func fromHostFrame(frame acphost.Frame) domain.Frame {
	return domain.Frame{Seq: frame.Seq, T: frame.T, From: frame.From, Msg: frame.Msg}
}
