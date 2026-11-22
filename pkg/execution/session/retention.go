package session

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/gfx-labs/protocol7/pkg/execution/acpshim"
)

// DefaultRetention is how long the directory of a closed session stays on
// disk when Config.Retention names none. The workspace of a session is a clone
// of each repository and what the build left in it, often gigabytes, so a
// machine that keeps them all fills its disk. A day leaves the journal and the
// files readable for a person who comes back to a chat the next morning.
const DefaultRetention = 24 * time.Hour

// retention is how long a closed session's directory stays: Config.Retention,
// the default if that is zero, and none (keep for ever) if it is negative.
func (m *Manager) retention() (keep time.Duration, enabled bool) {
	switch {
	case m.config.Retention < 0:
		return 0, false
	case m.config.Retention == 0:
		return DefaultRetention, true
	}
	return m.config.Retention, true
}

func (m *Manager) now() time.Time {
	if m.config.Now != nil {
		return m.config.Now()
	}
	return time.Now()
}

// closedAt is when the session was closed. A record from before the time was
// kept was written for the last time at its close, so the file's age tells.
func (m *Manager) closedAt(rec record) time.Time {
	if !rec.ClosedAt.IsZero() {
		return rec.ClosedAt
	}
	if info, err := os.Stat(filepath.Join(m.sessionDir(rec.ID), recordFile)); err == nil {
		return info.ModTime()
	}
	return time.Time{}
}

// purgeIfDue removes the files of a closed session whose retention has run
// out, and keeps its record, as a tombstone: the session is still closed, so
// that opening its ID again is refused as it was, and State still says so. A
// session that is open is never touched. It says whether it removed them.
func (m *Manager) purgeIfDue(ctx context.Context, rec record) (bool, error) {
	keep, enabled := m.retention()
	if !enabled || !rec.Closed || rec.Purged {
		return false, nil
	}
	closed := m.closedAt(rec)
	if closed.IsZero() || m.now().Sub(closed) < keep {
		return false, nil
	}
	dir := m.sessionDir(rec.ID)
	// Close stopped the shim. Stop it again, so that no process has the files
	// open that are removed.
	if err := acpshim.Stop(ctx, m.shimDir(rec.ID)); err != nil {
		return false, fmt.Errorf("purge session %s: %w", rec.ID, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, fmt.Errorf("purge session %s: %w", rec.ID, err)
	}
	for _, entry := range entries {
		if entry.Name() == recordFile {
			continue
		}
		if err := removeAll(filepath.Join(dir, entry.Name())); err != nil {
			return false, fmt.Errorf("purge session %s: %w", rec.ID, err)
		}
	}
	// The tombstone is written last: a purge that stops half way is done again
	// at the next sweep.
	if err := m.save(record{ID: rec.ID, Closed: true, ClosedAt: closed, Purged: true}); err != nil {
		return false, err
	}
	m.log.Info("closed session removed from disk", "session", rec.ID, "closed", closed, "retention", keep)
	return true, nil
}

// Sweep removes the files of every closed session whose retention has run
// out, and returns how many it removed. It goes on past a session that fails
// and returns the first failure.
func (m *Manager) Sweep(ctx context.Context) (int, error) {
	if _, enabled := m.retention(); !enabled {
		return 0, nil
	}
	entries, err := os.ReadDir(filepath.Join(m.config.Root, "sessions"))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("list sessions: %w", err)
	}
	removed := 0
	var first error
	for _, entry := range entries {
		if ctx.Err() != nil {
			return removed, ctx.Err()
		}
		if !entry.IsDir() || checkID(entry.Name()) != nil {
			continue
		}
		rec, err := m.load(entry.Name())
		if err != nil {
			// A directory with no readable record is not known to be closed.
			continue
		}
		done, err := m.purgeIfDue(ctx, rec)
		if err != nil {
			m.log.Warn("remove a closed session from disk failed", "session", rec.ID, "error", err)
			if first == nil {
				first = err
			}
		}
		if done {
			removed++
		}
	}
	return removed, first
}

// Retain sweeps now and then every interval, until ctx ends. A worker runs it
// for as long as it serves sessions.
func (m *Manager) Retain(ctx context.Context, interval time.Duration) {
	if _, enabled := m.retention(); !enabled {
		return
	}
	for {
		if _, err := m.Sweep(ctx); err != nil && ctx.Err() == nil {
			m.log.Warn("sweep of closed sessions failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// removeAll removes path like os.RemoveAll, and also a tree with directories
// that are not writable, which a Go module cache leaves.
func removeAll(path string) error {
	if err := os.RemoveAll(path); err == nil {
		return nil
	}
	_ = filepath.WalkDir(path, func(p string, entry fs.DirEntry, err error) error {
		if err == nil && entry.IsDir() {
			_ = os.Chmod(p, 0o700)
		}
		return nil
	})
	return os.RemoveAll(path)
}
