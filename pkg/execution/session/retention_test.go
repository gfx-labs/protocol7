package session_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/session"
)

// clock is a clock that a test moves.
type clock struct{ nanos atomic.Int64 }

func newClock() *clock {
	c := &clock{}
	c.nanos.Store(time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC).UnixNano())
	return c
}
func (c *clock) now() time.Time          { return time.Unix(0, c.nanos.Load()).UTC() }
func (c *clock) advance(d time.Duration) { c.nanos.Add(int64(d)) }

// filled puts a workspace's worth of files in a session: a file, and a
// directory that is not writable, as a Go module cache leaves.
func filled(t *testing.T, f *fixture, id string) string {
	t.Helper()
	work, err := session.Workspace(f.root, id)
	if err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(work, "go", "pkg", "mod", "example.com@v1")
	if err := os.MkdirAll(cache, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cache, "lib.go"), []byte("package lib\n"), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(cache, 0o555); err != nil {
		t.Fatal(err)
	}
	return work
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// The directory of a closed session is removed once its retention has run out,
// the files and the shim's directory with it, and not before. Its record stays,
// so that the session is still closed and its ID is not opened again.
func TestAClosedSessionsDirectoryIsRemovedAfterItsRetention(t *testing.T) {
	c := newClock()
	f := openWith(t, func(config *session.Config) { config.Retention, config.Now = time.Hour, c.now })
	work := filled(t, f, "s1")
	if _, err := f.turn(t.Context(), "t1", "hello", session.TurnControl{}); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(work)

	// Open, and for however long: not removed.
	c.advance(1000 * time.Hour)
	if removed, err := f.manager.Sweep(t.Context()); err != nil || removed != 0 {
		t.Fatalf("sweep of an open session: removed %d, %v", removed, err)
	}
	if !exists(work) {
		t.Fatal("an open session lost its workspace")
	}

	if err := f.manager.Close(t.Context(), domain.CloseRequest{Session: "s1"}); err != nil {
		t.Fatal(err)
	}
	if !exists(work) {
		t.Fatal("close removed the workspace, which stays for the retention")
	}
	c.advance(59 * time.Minute)
	if removed, err := f.manager.Sweep(t.Context()); err != nil || removed != 0 || !exists(work) {
		t.Fatalf("sweep inside the retention: removed %d, %v, workspace there %v", removed, err, exists(work))
	}

	c.advance(2 * time.Minute)
	if removed, err := f.manager.Sweep(t.Context()); err != nil || removed != 1 {
		t.Fatalf("sweep after the retention: removed %d, %v", removed, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "session.json" {
		t.Errorf("left in the session's directory: %v, want only the record", entries)
	}
	state, err := f.manager.State("s1")
	if err != nil || state != (domain.SessionState{Session: "s1", Exists: true, Closed: true}) {
		t.Errorf("state after the removal: %+v, %v", state, err)
	}
	if _, err := f.manager.Open(t.Context(), domain.SessionRequest{ID: "s1"}); !errors.Is(err, session.ErrClosed) {
		t.Errorf("open of a removed session: %v, want ErrClosed", err)
	}
	if removed, err := f.manager.Sweep(t.Context()); err != nil || removed != 0 {
		t.Errorf("a second sweep: removed %d, %v", removed, err)
	}
	// A close that repeats does no harm.
	if err := f.manager.Close(t.Context(), domain.CloseRequest{Session: "s1"}); err != nil {
		t.Errorf("close of a removed session: %v", err)
	}
}

// A close that comes after the retention removes the files itself, and Close
// of a session that is open does not.
func TestCloseRemovesWhatIsDue(t *testing.T) {
	c := newClock()
	f := openWith(t, func(config *session.Config) { config.Retention, config.Now = time.Hour, c.now })
	work := filled(t, f, "s1")
	if err := f.manager.Close(t.Context(), domain.CloseRequest{Session: "s1"}); err != nil {
		t.Fatal(err)
	}
	if !exists(work) {
		t.Fatal("the first close removed the workspace")
	}
	c.advance(2 * time.Hour)
	if err := f.manager.Close(context.Background(), domain.CloseRequest{Session: "s1"}); err != nil {
		t.Fatal(err)
	}
	if exists(work) {
		t.Error("a close after the retention left the workspace")
	}
}

// With a negative retention nothing is removed, and with none named the
// retention is a day.
func TestRetentionCanBeOffAndDefaultsToADay(t *testing.T) {
	c := newClock()
	off := openWith(t, func(config *session.Config) { config.Retention, config.Now = -1, c.now })
	work := filled(t, off, "s1")
	if err := off.manager.Close(t.Context(), domain.CloseRequest{Session: "s1"}); err != nil {
		t.Fatal(err)
	}
	c.advance(1000 * time.Hour)
	if removed, err := off.manager.Sweep(t.Context()); err != nil || removed != 0 || !exists(work) {
		t.Errorf("a negative retention: removed %d, %v, workspace there %v", removed, err, exists(work))
	}

	c = newClock()
	def := openWith(t, func(config *session.Config) { config.Now = c.now })
	work = filled(t, def, "s1")
	if err := def.manager.Close(t.Context(), domain.CloseRequest{Session: "s1"}); err != nil {
		t.Fatal(err)
	}
	c.advance(session.DefaultRetention - time.Minute)
	if _, err := def.manager.Sweep(t.Context()); err != nil || !exists(work) {
		t.Errorf("before the default retention: %v, workspace there %v", err, exists(work))
	}
	c.advance(2 * time.Minute)
	if removed, err := def.manager.Sweep(t.Context()); err != nil || removed != 1 || exists(work) {
		t.Errorf("after the default retention: removed %d, %v, workspace there %v", removed, err, exists(work))
	}
}

// A session closed before the time of the close was kept is removed by the age
// of its record, and a directory with no record, which may be a session that
// is being opened, is left alone.
func TestSweepReadsTheAgeOfARecordWithoutACloseTime(t *testing.T) {
	c := newClock()
	f := openWith(t, func(config *session.Config) { config.Retention, config.Now = time.Hour, c.now })
	work := filled(t, f, "s1")
	if err := f.manager.Close(t.Context(), domain.CloseRequest{Session: "s1"}); err != nil {
		t.Fatal(err)
	}
	record := filepath.Join(filepath.Dir(work), "session.json")
	content, err := os.ReadFile(record)
	if err != nil {
		t.Fatal(err)
	}
	// The record as an earlier build wrote it.
	var fields map[string]any
	if err := json.Unmarshal(content, &fields); err != nil {
		t.Fatal(err)
	}
	delete(fields, "closed_at")
	if content, err = json.Marshal(fields); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(record, content, 0o600); err != nil {
		t.Fatal(err)
	}
	stray := filepath.Join(f.root, "sessions", "stray", "work")
	if err := os.MkdirAll(stray, 0o755); err != nil {
		t.Fatal(err)
	}
	old := c.now().Add(-3 * time.Hour)
	if err := os.Chtimes(record, old, old); err != nil {
		t.Fatal(err)
	}
	if removed, err := f.manager.Sweep(t.Context()); err != nil || removed != 1 || exists(work) {
		t.Errorf("removed %d, %v, workspace there %v", removed, err, exists(work))
	}
	if !exists(stray) {
		t.Error("a directory with no record was removed")
	}
}

// The worker's timer, Retain, removes what falls due while it runs, and stops
// with its context.
func TestRetainRemovesAClosedSessionWhenItsRetentionRunsOut(t *testing.T) {
	c := newClock()
	f := openWith(t, func(config *session.Config) { config.Retention, config.Now = time.Hour, c.now })
	work := filled(t, f, "s1")
	if err := f.manager.Close(t.Context(), domain.CloseRequest{Session: "s1"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.manager.Retain(ctx, 10*time.Millisecond)
	}()
	time.Sleep(50 * time.Millisecond)
	if !exists(work) {
		t.Fatal("removed before the retention ran out")
	}
	c.advance(2 * time.Hour)
	deadline := time.Now().Add(10 * time.Second)
	for exists(work) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if exists(work) {
		t.Error("the timer did not remove the workspace")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("Retain did not stop with its context")
	}
}
