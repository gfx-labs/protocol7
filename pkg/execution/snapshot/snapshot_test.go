package snapshot

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"

	"github.com/gfx-labs/protocol7/pkg/execution/snapshot/snapshottest"
)

func newStore(t *testing.T, fake *snapshottest.Server, exclude ...string) *Store {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	store, err := New(Config{Endpoint: fake.URL, Bucket: snapshottest.Bucket, Prefix: "p7/", PathStyle: true, Exclude: exclude}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func write(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func TestARoundTripKeepsModesLinksAndTimes(t *testing.T) {
	fake := snapshottest.Start(t)
	store := newStore(t, fake, "node_modules", "work/target")
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "session")
	long := strings.Repeat("d", 120)
	write(t, filepath.Join(root, "work/repo/main.go"), "package main\n", 0o644)
	write(t, filepath.Join(root, "work/repo/run.sh"), "#!/bin/sh\n", 0o755)
	write(t, filepath.Join(root, "work/repo/secret"), "s", 0o600)
	write(t, filepath.Join(root, "work", long, long+".txt"), "long", 0o644)
	write(t, filepath.Join(root, "work/repo/node_modules/dep/index.js"), "x", 0o644)
	write(t, filepath.Join(root, "work/target/debug/bin"), "x", 0o755)
	write(t, filepath.Join(root, "shim/journal.ndjson"), "{}\n", 0o600)
	if err := os.MkdirAll(filepath.Join(root, "work/empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("main.go", filepath.Join(root, "work/repo/link")); err != nil {
		t.Fatal(err)
	}
	// Left out: a link that points out, and a socket.
	if err := os.Symlink("/etc/passwd", filepath.Join(root, "work/repo/outside")); err != nil {
		t.Fatal(err)
	}
	socket, err := newSocket(filepath.Join(root, "shim", "a.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer socket()
	stamp := time.Date(2026, 3, 4, 5, 6, 7, 123456789, time.UTC)
	if err := os.Chtimes(filepath.Join(root, "work/repo/main.go"), stamp, stamp); err != nil {
		t.Fatal(err)
	}

	result, err := store.Save(ctx, "s1", root)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Uploaded || result.Bytes == 0 {
		t.Fatalf("Save = %+v, want an upload", result)
	}
	if len(result.Skipped) != 2 {
		t.Errorf("skipped %v, want the outside link and the socket", result.Skipped)
	}

	dest := filepath.Join(t.TempDir(), "restored")
	var prepared string
	found, err := store.Restore(ctx, "s1", dest, func(dir string) error { prepared = dir; return nil })
	if err != nil || !found {
		t.Fatalf("Restore = %v, %v", found, err)
	}
	if prepared == "" || prepared == dest {
		t.Errorf("prepare ran on %q, want the directory before it is renamed", prepared)
	}
	if got := read(t, filepath.Join(dest, "work/repo/main.go")); got != "package main\n" {
		t.Errorf("main.go = %q", got)
	}
	if got := read(t, filepath.Join(dest, "work", long, long+".txt")); got != "long" {
		t.Errorf("long path = %q", got)
	}
	for path, want := range map[string]os.FileMode{
		"work/repo/run.sh": 0o755, "work/repo/secret": 0o600, "work/repo/main.go": 0o644, "work/empty": 0o755 | os.ModeDir,
	} {
		info, err := os.Lstat(filepath.Join(dest, path))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode() != want {
			t.Errorf("%s has mode %v, want %v", path, info.Mode(), want)
		}
	}
	if target, err := os.Readlink(filepath.Join(dest, "work/repo/link")); err != nil || target != "main.go" {
		t.Errorf("link = %q, %v", target, err)
	}
	info, err := os.Stat(filepath.Join(dest, "work/repo/main.go"))
	if err != nil || !info.ModTime().Equal(stamp) {
		t.Errorf("modification time %v, %v, want %v", info.ModTime(), err, stamp)
	}
	for _, left := range []string{"work/repo/node_modules", "work/target", "work/repo/outside", "shim/a.sock"} {
		if _, err := os.Lstat(filepath.Join(dest, left)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s was restored: %v", left, err)
		}
	}
	// The restored tree is the one that was saved, so saving it again, as
	// another process would, uploads nothing.
	again, err := newStore(t, fake).Save(ctx, "s1", dest)
	if err != nil {
		t.Fatal(err)
	}
	if again.Uploaded {
		t.Error("a restored tree was uploaded again")
	}
}

func TestAnUnchangedTreeIsNotUploadedAgain(t *testing.T) {
	fake := snapshottest.Start(t)
	store := newStore(t, fake)
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "session")
	write(t, filepath.Join(root, "work/a"), "one", 0o644)

	first, err := store.Save(ctx, "s1", root)
	if err != nil || !first.Uploaded {
		t.Fatalf("first Save = %+v, %v", first, err)
	}
	second, err := store.Save(ctx, "s1", root)
	if err != nil || second.Uploaded {
		t.Fatalf("second Save = %+v, %v, want nothing uploaded", second, err)
	}
	// A new process finds the fingerprint on the object.
	third, err := newStore(t, fake).Save(ctx, "s1", root)
	if err != nil || third.Uploaded {
		t.Fatalf("Save by a new store = %+v, %v, want nothing uploaded", third, err)
	}
	write(t, filepath.Join(root, "work/a"), "two!", 0o644)
	changed, err := store.Save(ctx, "s1", root)
	if err != nil || !changed.Uploaded {
		t.Fatalf("Save after a change = %+v, %v, want an upload", changed, err)
	}
}

func TestAFailedUploadKeepsThePreviousSnapshot(t *testing.T) {
	fake := snapshottest.Start(t)
	store := newStore(t, fake)
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "session")
	write(t, filepath.Join(root, "work/a"), "old", 0o644)
	if _, err := store.Save(ctx, "s1", root); err != nil {
		t.Fatal(err)
	}

	write(t, filepath.Join(root, "work/a"), "new", 0o644)
	fake.FailCopy.Store(true)
	if _, err := store.Save(ctx, "s1", root); err == nil {
		t.Fatal("Save succeeded while the copy failed")
	}
	fake.FailCopy.Store(false)

	dest := filepath.Join(t.TempDir(), "restored")
	if found, err := store.Restore(ctx, "s1", dest, nil); err != nil || !found {
		t.Fatalf("Restore = %v, %v", found, err)
	}
	if got := read(t, filepath.Join(dest, "work/a")); got != "old" {
		t.Errorf("restored %q, want the earlier snapshot's %q", got, "old")
	}
	// The failed save did not count as a snapshot, so the next one writes.
	retry, err := store.Save(ctx, "s1", root)
	if err != nil || !retry.Uploaded {
		t.Fatalf("Save after the failure = %+v, %v, want an upload", retry, err)
	}
}

func TestRestoreWithNoSnapshotDoesNothing(t *testing.T) {
	store := newStore(t, snapshottest.Start(t))
	dest := filepath.Join(t.TempDir(), "dir")
	write(t, filepath.Join(dest, "keep"), "k", 0o644)
	found, err := store.Restore(context.Background(), "none", dest, nil)
	if err != nil || found {
		t.Fatalf("Restore = %v, %v, want false", found, err)
	}
	if read(t, filepath.Join(dest, "keep")) != "k" {
		t.Error("the directory was changed")
	}
}

func TestADamagedSnapshotLeavesTheDirectoryAlone(t *testing.T) {
	fake := snapshottest.Start(t)
	store := newStore(t, fake)
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "session")
	write(t, filepath.Join(root, "work/big"), strings.Repeat("0123456789", 100000), 0o644)
	write(t, filepath.Join(root, "work/small"), "x", 0o644)
	if _, err := store.Save(ctx, "s1", root); err != nil {
		t.Fatal(err)
	}
	// Cut the object short.
	object, err := store.client.GetObject(ctx, snapshottest.Bucket, store.key("s1"), minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	whole, err := io.ReadAll(object)
	if err != nil {
		t.Fatal(err)
	}
	cut := whole[:len(whole)-len(whole)/3]
	if _, err := store.client.PutObject(ctx, snapshottest.Bucket, store.key("s1"), bytes.NewReader(cut), int64(len(cut)), minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}

	dest := filepath.Join(t.TempDir(), "dir")
	write(t, filepath.Join(dest, "keep"), "k", 0o644)
	if found, err := store.Restore(ctx, "s1", dest, nil); err == nil || found {
		t.Fatalf("Restore = %v, %v, want an error", found, err)
	}
	if read(t, filepath.Join(dest, "keep")) != "k" {
		t.Error("the directory was replaced by a half-written one")
	}
	left, _ := filepath.Glob(filepath.Join(filepath.Dir(dest), ".restore-*"))
	if len(left) != 0 {
		t.Errorf("left %v", left)
	}
}

func TestDeleteRemovesTheSnapshot(t *testing.T) {
	store := newStore(t, snapshottest.Start(t))
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "session")
	write(t, filepath.Join(root, "a"), "a", 0o644)
	if _, err := store.Save(ctx, "s1", root); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "s1"); err != nil {
		t.Errorf("deleting twice: %v", err)
	}
	if found, err := store.Restore(ctx, "s1", filepath.Join(t.TempDir(), "x"), nil); err != nil || found {
		t.Errorf("Restore = %v, %v, want no snapshot", found, err)
	}
	// Saved again from the same tree, it is written again.
	if result, err := store.Save(ctx, "s1", root); err != nil || !result.Uploaded {
		t.Errorf("Save = %+v, %v", result, err)
	}
}

// archiveOf makes a zstd-compressed tar of the headers, each with a body.
func archiveOf(t *testing.T, entries ...tar.Header) io.Reader {
	t.Helper()
	var buffer bytes.Buffer
	compressor, err := zstd.NewWriter(&buffer)
	if err != nil {
		t.Fatal(err)
	}
	archive := tar.NewWriter(compressor)
	for _, header := range entries {
		header := header
		header.Size = int64(len(header.Xattrs["body"]))
		body := header.Xattrs["body"]
		header.Xattrs = nil
		if header.Mode == 0 {
			header.Mode = 0o644
		}
		if err := archive.WriteHeader(&header); err != nil {
			t.Fatal(err)
		}
		if _, err := archive.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if err := compressor.Close(); err != nil {
		t.Fatal(err)
	}
	return &buffer
}

func file(name, body string) tar.Header {
	return tar.Header{Name: name, Typeflag: tar.TypeReg, Xattrs: map[string]string{"body": body}}
}

func TestExtractRefusesEntriesThatLeaveTheWorkspace(t *testing.T) {
	cases := map[string][]tar.Header{
		"dot dot":            {file("../escape", "x")},
		"dot dot inside":     {file("a/../../escape", "x")},
		"absolute":           {file("/tmp/escape", "x")},
		"absolute link":      {{Name: "l", Typeflag: tar.TypeSymlink, Linkname: "/etc"}},
		"link out":           {{Name: "a/l", Typeflag: tar.TypeSymlink, Linkname: "../../x"}},
		"link through a dir": {{Name: "a/l", Typeflag: tar.TypeSymlink, Linkname: "b/../../x"}},
		"through a link": {
			{Name: "l", Typeflag: tar.TypeSymlink, Linkname: "."},
			file("l/escape", "x"),
		},
		"hard link": {{Name: "h", Typeflag: tar.TypeLink, Linkname: "/etc/passwd"}},
		"device":    {{Name: "d", Typeflag: tar.TypeChar}},
	}
	for name, entries := range cases {
		t.Run(name, func(t *testing.T) {
			dest := t.TempDir()
			err := extract(archiveOf(t, entries...), dest)
			if !errors.Is(err, ErrUnsafeArchive) {
				t.Fatalf("extract: %v, want ErrUnsafeArchive", err)
			}
			if _, err := os.Lstat(filepath.Join(filepath.Dir(dest), "escape")); err == nil {
				t.Error("a file was written outside the directory")
			}
		})
	}
}

func TestExtractAcceptsLinksThatStayInside(t *testing.T) {
	dest := t.TempDir()
	err := extract(archiveOf(t,
		file("a/b/f", "x"),
		tar.Header{Name: "a/b/up", Typeflag: tar.TypeSymlink, Linkname: "../f2"},
		tar.Header{Name: "a/b/same", Typeflag: tar.TypeSymlink, Linkname: "./f"},
		tar.Header{Name: "top", Typeflag: tar.TypeSymlink, Linkname: "a/b/f"},
	), dest)
	if err != nil {
		t.Fatal(err)
	}
}

func TestALinkThatLeavesIsLeftOutOfTheArchive(t *testing.T) {
	for name, escapes := range map[string]bool{
		"main.go": false, "../x": false, "../../x": true, "/abs": true, "sub/../x": true, "./x": false, "": true,
	} {
		if got := linkEscapes("work/l", name); got != escapes {
			t.Errorf("linkEscapes(work/l, %q) = %v, want %v", name, got, escapes)
		}
	}
	// "../x" from a link at the top leaves, and from one level down does not.
	if linkEscapes("a/l", "../x") || !linkEscapes("l", "../x") || !linkEscapes("a/l", "../../x") {
		t.Error("the depth of the link is not counted")
	}
}

func TestExcludePatterns(t *testing.T) {
	cases := []struct {
		name     string
		patterns []string
		want     bool
	}{
		{"work/repo/node_modules", []string{"node_modules"}, true},
		{"node_modules", []string{"node_modules"}, true},
		{"work/repo/src", []string{"node_modules"}, false},
		{"work/target", []string{"work/target"}, true},
		{"other/work/target", []string{"work/target"}, false},
		{"work/a.log", []string{"*.log"}, true},
		{"work/x", []string{""}, false},
	}
	for _, c := range cases {
		if got := excluded(c.name, c.patterns); got != c.want {
			t.Errorf("excluded(%q, %v) = %v, want %v", c.name, c.patterns, got, c.want)
		}
	}
}

func TestConfigValidation(t *testing.T) {
	if (Config{}).Enabled() {
		t.Error("the empty configuration keeps snapshots")
	}
	if err := (Config{Prefix: "x"}).Validate(); err == nil {
		t.Error("a prefix with no bucket is accepted")
	}
	for _, endpoint := range []string{"ftp://x", "http://x/path"} {
		if err := (Config{Bucket: "b", Endpoint: endpoint}).Validate(); err == nil {
			t.Errorf("endpoint %q is accepted", endpoint)
		}
	}
	for endpoint, secure := range map[string]bool{"http://seaweedfs-s3.seaweedfs:8333": false, "https://s3.example.com": true, "s3.example.com:9000": true, "": true} {
		_, got, err := (Config{Bucket: "b", Endpoint: endpoint}).endpoint()
		if err != nil || got != secure {
			t.Errorf("endpoint %q: secure = %v, %v, want %v", endpoint, got, err, secure)
		}
	}
}

// A snapshot larger than one part goes up in several, and comes back whole.
func TestALargeSnapshotIsUploadedInParts(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 40 MB")
	}
	fake := snapshottest.Start(t)
	store := newStore(t, fake)
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "session")
	// Random, so that compression does not make it small.
	random := make([]byte, 40<<20)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(root, "work/blob"), string(random), 0o644)
	result, err := store.Save(ctx, "big", root)
	if err != nil || !result.Uploaded || result.Bytes < partSize*2 {
		t.Fatalf("Save = %+v, %v, want an upload of more than two parts", result, err)
	}
	dest := filepath.Join(t.TempDir(), "restored")
	if found, err := store.Restore(ctx, "big", dest, nil); err != nil || !found {
		t.Fatalf("Restore = %v, %v", found, err)
	}
	if got := read(t, filepath.Join(dest, "work/blob")); got != string(random) {
		t.Error("the restored file differs")
	}
}
