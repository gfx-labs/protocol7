package session_test

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/acpshim"
	"github.com/gfx-labs/protocol7/pkg/execution/session"
	"github.com/gfx-labs/protocol7/pkg/execution/snapshot"
	"github.com/gfx-labs/protocol7/pkg/execution/snapshot/snapshottest"
)

// A snapshot that is being written when the session is closed does not
// outlive the close: Close deletes the snapshot after the write is done.
func TestASnapshotBeingWrittenDuringCloseIsDeleted(t *testing.T) {
	s3 := snapshottest.Start(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	store, err := snapshot.New(snapshot.Config{Endpoint: s3.URL, Bucket: snapshottest.Bucket, PathStyle: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	f := open(t)
	f.manager.SetSnapshots(store)
	ctx := context.Background()

	// A change to write, and an upload that takes a while.
	if err := os.WriteFile(filepath.Join(f.root, "sessions", "s1", "work", "file"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	s3.WriteDelay.Store(int64(300 * time.Millisecond))
	snapshotted := make(chan error, 1)
	go func() { snapshotted <- f.manager.Snapshot(ctx, "s1") }()
	// The snapshot has read the record and is uploading.
	time.Sleep(150 * time.Millisecond)

	if err := f.manager.Close(ctx, domain.CloseRequest{Session: "s1"}); err != nil {
		t.Fatal(err)
	}
	if err := <-snapshotted; err != nil {
		t.Fatal(err)
	}
	found, err := store.Restore(ctx, "s1", filepath.Join(t.TempDir(), "x"), nil)
	if err != nil || found {
		t.Errorf("the snapshot is there after the close: %v, %v", found, err)
	}
	// And a snapshot after the close writes nothing.
	if err := f.manager.Snapshot(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	if found, err := store.Restore(ctx, "s1", filepath.Join(t.TempDir(), "x"), nil); err != nil || found {
		t.Errorf("a closed session was snapshotted: %v, %v", found, err)
	}
}

// The snapshot after a turn takes longer than an activity's heartbeat
// timeout allows, so the turn heartbeats while it is written.
func TestATurnHeartbeatsWhileItsSnapshotIsWritten(t *testing.T) {
	s3 := snapshottest.Start(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	store, err := snapshot.New(snapshot.Config{Endpoint: s3.URL, Bucket: snapshottest.Bucket, PathStyle: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	f := open(t)
	f.manager.SetSnapshots(store)
	s3.WriteDelay.Store(int64(time.Second))

	var beats atomic.Int32
	started := time.Now()
	result, err := f.turn(context.Background(), "t1", "hello", session.TurnControl{Beat: func() { beats.Add(1) }})
	if err != nil || result.StopReason != "end_turn" {
		t.Fatalf("turn: %+v, %v", result, err)
	}
	elapsed := time.Since(started)
	// Five writes of a second each, with a beat every two seconds.
	if elapsed < 4*time.Second || beats.Load() < 2 {
		t.Errorf("the turn took %s and heartbeat %d times, want a slow snapshot with a beat every two seconds", elapsed, beats.Load())
	}
	found, err := store.Restore(context.Background(), "s1", filepath.Join(t.TempDir(), "x"), nil)
	if err != nil || !found {
		t.Errorf("the snapshot was not written: %v, %v", found, err)
	}
}

// A turn on a machine that lost the session's disk restores the snapshot
// first, and heartbeats while it does, because a large restore takes longer
// than the activity's heartbeat timeout.
func TestATurnHeartbeatsWhileItsSessionIsRestored(t *testing.T) {
	s3 := snapshottest.Start(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	store, err := snapshot.New(snapshot.Config{Endpoint: s3.URL, Bucket: snapshottest.Bucket, PathStyle: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	f := open(t)
	f.manager.SetSnapshots(store)
	ctx := context.Background()
	if err := f.manager.Snapshot(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	// The machine's disk is lost.
	if err := acpshim.Stop(ctx, filepath.Join(f.root, "sessions", "s1", "shim")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(f.root, "sessions", "s1")); err != nil {
		t.Fatal(err)
	}
	// Reads of a second each: a look at the object and the download.
	s3.ReadDelay.Store(int64(2 * time.Second))

	var beats atomic.Int32
	started := time.Now()
	result, err := f.turn(ctx, "t1", "hello", session.TurnControl{Beat: func() { beats.Add(1) }})
	if err != nil || result.StopReason != "end_turn" {
		t.Fatalf("turn: %+v, %v", result, err)
	}
	s3.ReadDelay.Store(0)
	// Two reads of two seconds make a restore of four, with a beat every two.
	if elapsed := time.Since(started); elapsed < 3*time.Second || beats.Load() < 1 {
		t.Errorf("the turn took %s and heartbeat %d times, want a slow restore with a beat every two seconds", elapsed, beats.Load())
	}
}
