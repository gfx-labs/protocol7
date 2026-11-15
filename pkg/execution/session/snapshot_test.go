package session_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain"
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
