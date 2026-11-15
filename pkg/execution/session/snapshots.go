package session

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/gfx-labs/protocol7/pkg/execution/acpshim"
	"github.com/gfx-labs/protocol7/pkg/execution/snapshot"
)

// snapshotTimeout bounds one snapshot written after a turn.
const snapshotTimeout = 10 * time.Minute

// snapshotBeat is how often a turn's activity heartbeats while its snapshot
// is written or restored.
const snapshotBeat = 2 * time.Second

// harnessDirName is where a session's harness keeps its conversation, when
// snapshots are on, so that the snapshot holds it.
const harnessDirName = "harness"

// SetSnapshots makes the manager keep a snapshot of each session's directory
// in store: after a session opens and after each of its turns, and on
// SnapshotAll. A session whose directory is not on this machine is restored
// from its snapshot when it is first used. Call it before the manager is
// used. Without it the manager keeps no snapshots.
//
// The session's directory holds its workspace, its shim's journal and its
// record. A harness that has a flag for the directory of its conversations
// (Harness.StoreFlag) is told to keep them there too.
func (m *Manager) SetSnapshots(store *snapshot.Store) { m.snapshots = store }

// harnessDir is where the session's harness keeps its conversation.
func (m *Manager) harnessDir(id string) (string, error) {
	return filepath.Abs(filepath.Join(m.sessionDir(id), harnessDirName))
}

// snapshotLock serializes writing a session's snapshot with deleting it.
func (m *Manager) snapshotLock(id string) func() {
	m.locksMu.Lock()
	l := m.snapshotLocks[id]
	if l == nil {
		l = &sync.Mutex{}
		if m.snapshotLocks == nil {
			m.snapshotLocks = map[string]*sync.Mutex{}
		}
		m.snapshotLocks[id] = l
	}
	m.locksMu.Unlock()
	l.Lock()
	return l.Unlock
}

// snapshotExclude is what a snapshot leaves out of a session's directory,
// besides the store's own list: what only a running shim has.
func snapshotExclude() []string {
	var exclude []string
	for _, name := range acpshim.RuntimeFiles() {
		exclude = append(exclude, path.Join(shimDirName, name))
	}
	return exclude
}

// Snapshot writes the snapshot of a session now, unless nothing changed
// since the last. A session that is closed, or is not on this machine, has
// none written.
func (m *Manager) Snapshot(ctx context.Context, id string) error {
	if m.snapshots == nil {
		return nil
	}
	// Close deletes the snapshot under the same lock, after it marks the
	// session closed, so a snapshot that is being written is deleted when
	// it is done, and one that starts later sees the mark.
	defer m.snapshotLock(id)()
	rec, err := m.load(id)
	if isMissing(err) || (err == nil && rec.Closed) {
		return nil
	}
	if err != nil {
		return err
	}
	result, err := m.snapshots.Save(ctx, id, m.sessionDir(id), snapshotExclude()...)
	if err != nil {
		return err
	}
	if len(result.Skipped) > 0 {
		m.log.Warn("left out of the snapshot: not a file, a directory or a link inside the session", "session", id, "entries", result.Skipped)
	}
	if result.Uploaded {
		m.log.Info("snapshot written", "session", id, "bytes", result.Bytes)
	}
	return nil
}

// snapshotAfter writes a snapshot, and logs a failure. The work it follows
// has happened, and the next snapshot tries again. beat, if it is not nil, is
// called every two seconds while the snapshot is written, because the caller
// is an activity that has to heartbeat, and a snapshot of a large workspace
// takes longer than its heartbeat timeout.
func (m *Manager) snapshotAfter(ctx context.Context, id, after string, beat func()) {
	if m.snapshots == nil {
		return
	}
	// The caller may be cancelled and the work is done all the same.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), snapshotTimeout)
	defer cancel()
	defer heartbeats(beat)()
	if err := m.Snapshot(ctx, id); err != nil {
		m.log.Warn("snapshot of the session failed, the previous one stays", "session", id, "after", after, "error", err)
	}
}

// heartbeats calls beat every two seconds until the returned function is
// called, which waits for the last call. A nil beat does nothing.
func heartbeats(beat func()) (stop func()) {
	if beat == nil {
		return func() {}
	}
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(snapshotBeat)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				beat()
			case <-done:
				return
			}
		}
	}()
	return func() { close(done); <-stopped }
}

// SnapshotAll writes the snapshot of every session on this machine. A worker
// does it when it shuts down. It goes on past a session that fails, and
// returns the first failure.
func (m *Manager) SnapshotAll(ctx context.Context) error {
	if m.snapshots == nil {
		return nil
	}
	entries, err := os.ReadDir(filepath.Join(m.config.Root, "sessions"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("list sessions: %w", err)
	}
	var first error
	for _, entry := range entries {
		if !entry.IsDir() || checkID(entry.Name()) != nil {
			continue
		}
		if err := m.Snapshot(ctx, entry.Name()); err != nil {
			m.log.Warn("snapshot of the session failed", "session", entry.Name(), "error", err)
			if first == nil {
				first = err
			}
		}
	}
	return first
}

// Restore brings back a session's directory from its snapshot, if this
// machine has no record of the session and a snapshot exists. It reports
// whether it did. The harness and the shim do not run afterwards: the next
// turn starts them, and the harness loads the conversation.
func (m *Manager) Restore(ctx context.Context, id string) (bool, error) {
	if m.snapshots == nil {
		return false, nil
	}
	if err := checkID(id); err != nil {
		return false, err
	}
	// Two callers for one session must not restore twice: the second would
	// replace the directory the first is already using.
	m.restoring.Lock()
	defer m.restoring.Unlock()
	if _, err := m.load(id); !isMissing(err) {
		return false, err
	}
	restored, err := m.snapshots.Restore(ctx, id, m.sessionDir(id), func(dir string) error {
		// A process ID in the record belongs to the shim of the machine that
		// is gone. Another shim may be given the same one.
		file := filepath.Join(dir, recordFile)
		content, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read the record in the snapshot: %w", err)
		}
		var rec record
		if err := json.Unmarshal(content, &rec); err != nil {
			return fmt.Errorf("decode the record in the snapshot: %w", err)
		}
		rec.ShimPid = 0
		content, err = json.MarshalIndent(rec, "", "  ")
		if err != nil {
			return err
		}
		return os.WriteFile(file, append(content, '\n'), 0o600)
	})
	if err != nil {
		return false, err
	}
	if restored {
		m.log.Info("session restored from its snapshot", "session", id)
	}
	return restored, nil
}
