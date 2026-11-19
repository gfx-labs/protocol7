package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// stores runs a test against the directory store and, when SeaweedFS is
// available, the S3 store.
func stores(t *testing.T, test func(t *testing.T, blobs Blobs)) {
	t.Run("dir", func(t *testing.T) {
		blobs, err := NewDir(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		test(t, blobs)
	})
	t.Run("s3", func(t *testing.T) {
		blobs, err := NewS3(testS3(t))
		if err != nil {
			t.Fatal(err)
		}
		test(t, blobs)
	})
}

func digestOf(data string) string {
	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:])
}

func TestBlobs(t *testing.T) {
	stores(t, func(t *testing.T, blobs Blobs) {
		ctx := context.Background()
		if _, err := blobs.Stat(ctx, "a/b"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("stat of a missing key: %v", err)
		}
		if _, err := blobs.Get(ctx, "a/b"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("get of a missing key: %v", err)
		}
		if err := blobs.Put(ctx, "a/b", strings.NewReader("hello"), 5); err != nil {
			t.Fatal(err)
		}
		if err := blobs.Put(ctx, "a/c/d", strings.NewReader(""), 0); err != nil {
			t.Fatal(err)
		}
		reader, err := blobs.Get(ctx, "a/b")
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(reader)
		reader.Close()
		if string(data) != "hello" {
			t.Fatalf("got %q", data)
		}
		info, err := blobs.Stat(ctx, "a/b")
		if err != nil || info.Size != 5 || time.Since(info.Modified) > time.Hour {
			t.Fatalf("stat: %+v, %v", info, err)
		}
		infos, err := blobs.List(ctx, "a/")
		if err != nil || len(infos) != 2 || infos[0].Key != "a/b" || infos[1].Key != "a/c/d" {
			t.Fatalf("list: %+v, %v", infos, err)
		}
		if err := blobs.Delete(ctx, "a/b"); err != nil {
			t.Fatal(err)
		}
		if err := blobs.Delete(ctx, "a/b"); err != nil {
			t.Fatalf("delete of a missing key: %v", err)
		}
		if _, err := blobs.Stat(ctx, "a/b"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("stat after delete: %v", err)
		}
	})
}

func TestSaveAndLookup(t *testing.T) {
	stores(t, func(t *testing.T, blobs Blobs) {
		ctx := context.Background()
		store := &Store{Blobs: blobs, TempDir: t.TempDir()}
		first, err := store.Save(ctx, "selfdev", "ui/mockup.png", "chat1", strings.NewReader("one"))
		if err != nil {
			t.Fatal(err)
		}
		if first.Digest != digestOf("one") || first.Size != 3 {
			t.Fatalf("saved %+v", first)
		}
		second, err := store.Save(ctx, "selfdev", "ui/mockup.png", "chat2", strings.NewReader("two"))
		if err != nil {
			t.Fatal(err)
		}
		newest, err := store.Lookup(ctx, "selfdev", "ui/mockup.png")
		if err != nil || newest.Digest != second.Digest || newest.By != "chat2" {
			t.Fatalf("lookup: %+v, %v", newest, err)
		}
		// The earlier digest still reads by its reference.
		reader, err := store.Open(ctx, first.Digest)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(reader)
		reader.Close()
		if string(data) != "one" {
			t.Fatalf("earlier digest holds %q", data)
		}
		versions, err := store.Versions(ctx, "selfdev", "ui/mockup.png")
		if err != nil || len(versions) != 2 || versions[0].Digest != second.Digest || versions[1].Digest != first.Digest {
			t.Fatalf("versions: %+v, %v", versions, err)
		}
		// Saving the first bytes again moves the name back to them, without a copy.
		again, err := store.Save(ctx, "selfdev", "ui/mockup.png", "chat3", strings.NewReader("one"))
		if err != nil {
			t.Fatal(err)
		}
		versions, _ = store.Versions(ctx, "selfdev", "ui/mockup.png")
		if len(versions) != 2 || versions[0].Digest != again.Digest {
			t.Fatalf("versions after saving again: %+v", versions)
		}
		if _, err := store.Lookup(ctx, "selfdev", "missing"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("lookup of a missing name: %v", err)
		}
		if _, err := store.Open(ctx, digestOf("never saved")); !errors.Is(err, ErrNotFound) {
			t.Fatalf("open of a missing digest: %v", err)
		}
		if _, err := store.Save(ctx, "selfdev", "../escape", "", strings.NewReader("x")); err == nil {
			t.Fatal("a name that climbs was saved")
		}
		if _, err := store.Save(ctx, "sha256", "x", "", strings.NewReader("x")); err == nil {
			t.Fatal("the blob prefix was taken as a project")
		}
	})
}

func TestList(t *testing.T) {
	stores(t, func(t *testing.T, blobs Blobs) {
		ctx := context.Background()
		store := &Store{Blobs: blobs, TempDir: t.TempDir()}
		for _, name := range []string{"ui/a.png", "ui/b.png", "report.md"} {
			if _, err := store.Save(ctx, "selfdev", name, "", strings.NewReader(name)); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := store.Save(ctx, "other", "ui/c.png", "", strings.NewReader("c")); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Save(ctx, "selfdev", "ui/a.png", "", strings.NewReader("a2")); err != nil {
			t.Fatal(err)
		}
		entries, err := store.List(ctx, "selfdev", "ui/")
		if err != nil || len(entries) != 2 {
			t.Fatalf("list: %+v, %v", entries, err)
		}
		if entries[0].Name != "ui/a.png" || entries[0].Versions != 2 || entries[0].Kept != "newest" || entries[0].Digest != digestOf("a2") {
			t.Fatalf("first entry: %+v", entries[0])
		}
		all, err := store.List(ctx, "selfdev", "")
		if err != nil || len(all) != 3 {
			t.Fatalf("list of all: %+v, %v", all, err)
		}
	})
}

func TestSizeLimit(t *testing.T) {
	blobs, _ := NewDir(t.TempDir())
	store := &Store{Blobs: blobs, MaxSize: 10, TempDir: t.TempDir()}
	if _, err := store.Save(context.Background(), "selfdev", "big", "", strings.NewReader(strings.Repeat("x", 11))); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got %v", err)
	}
	if _, err := store.Save(context.Background(), "selfdev", "fits", "", strings.NewReader(strings.Repeat("x", 10))); err != nil {
		t.Fatal(err)
	}
}

func TestFetchChecksTheDigest(t *testing.T) {
	blobs, _ := NewDir(t.TempDir())
	store := &Store{Blobs: blobs, TempDir: t.TempDir()}
	version, err := store.Save(context.Background(), "selfdev", "x", "", strings.NewReader("real"))
	if err != nil {
		t.Fatal(err)
	}
	if err := blobs.Put(context.Background(), blobKey(version.Digest), strings.NewReader("fake"), 4); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Fetch(context.Background(), version.Digest); err == nil || !strings.Contains(err.Error(), "do not match") {
		t.Fatalf("got %v", err)
	}
}

func TestSweep(t *testing.T) {
	stores(t, func(t *testing.T, blobs Blobs) {
		ctx := context.Background()
		now := time.Now().UTC()
		store := &Store{Blobs: blobs, TempDir: t.TempDir(), Retention: 24 * time.Hour, Now: func() time.Time { return now }}
		save := func(name, data string) Version {
			version, err := store.Save(ctx, "selfdev", name, "", strings.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			return version
		}
		oldOne, oldTwo := save("a", "a1"), save("a", "a2")
		pinned := save("b", "b1")
		save("b", "b2")
		if err := store.Pin(ctx, "selfdev", "b", pinned.Digest, true); err != nil {
			t.Fatal(err)
		}
		save("c", "c1")
		// Nothing is old yet.
		if result, err := store.Sweep(ctx); err != nil || result != (SweepResult{}) {
			t.Fatalf("early sweep: %+v, %v", result, err)
		}
		// Later, the index entries have aged out, but not the blobs' own
		// clock, which the store's does not move. So the sweep removes
		// versions, and blobs only once the bucket's clock agrees.
		now = now.Add(48 * time.Hour)
		store.Now = func() time.Time { return now }
		result, err := store.Sweep(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if result.Versions != 1 {
			// a1 is the one old version that is neither newest nor pinned.
			t.Fatalf("sweep removed %d versions, want 1", result.Versions)
		}
		versions, _ := store.Versions(ctx, "selfdev", "a")
		if len(versions) != 1 || versions[0].Digest != oldTwo.Digest {
			t.Fatalf("a keeps %+v", versions)
		}
		versions, _ = store.Versions(ctx, "selfdev", "b")
		if len(versions) != 2 {
			t.Fatalf("b keeps %+v, want the pinned one and the newest", versions)
		}
		if result.Blobs != 1 {
			t.Fatalf("sweep removed %d blobs, want only a1's", result.Blobs)
		}
		if _, err := store.Stat(ctx, oldOne.Digest); !errors.Is(err, ErrNotFound) {
			t.Fatalf("a1 is still there: %v", err)
		}
		for _, digest := range []string{oldTwo.Digest, pinned.Digest, digestOf("b2"), digestOf("c1")} {
			if _, err := store.Stat(ctx, digest); err != nil {
				t.Fatalf("%s was removed: %v", digest, err)
			}
		}
		// The newest digest of a name is never swept, however old.
		now = now.Add(1000 * time.Hour)
		store.Now = func() time.Time { return now }
		if _, err := store.Sweep(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Lookup(ctx, "selfdev", "c"); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Stat(ctx, digestOf("c1")); err != nil {
			t.Fatal(err)
		}
	})
}

func TestWorkspaceSaveAndGet(t *testing.T) {
	stores(t, func(t *testing.T, blobs Blobs) {
		ctx := context.Background()
		store := &Store{Blobs: blobs, TempDir: t.TempDir()}
		workspace := t.TempDir()
		write := func(name, data string, mode os.FileMode) {
			path := filepath.Join(workspace, name)
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(data), mode); err != nil {
				t.Fatal(err)
			}
		}
		write("shots/home.png", "png-bytes", 0o644)
		write("ref/src/main.go", "package main", 0o644)
		write("ref/run.sh", "#!/bin/sh", 0o755)
		write("ref/.git/config", "secret", 0o644)
		if err := os.Symlink("/etc/passwd", filepath.Join(workspace, "ref", "passwd")); err != nil {
			t.Fatal(err)
		}
		root, err := os.OpenRoot(workspace)
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()

		file, err := store.SaveFrom(ctx, root, "shots/home.png", "selfdev", "home.png", "chat")
		if err != nil || file.Dir || file.Size != 9 || file.Ref != "p7://sha256:"+digestOf("png-bytes")+"#home.png" || !file.Changed {
			t.Fatalf("save file: %+v, %v", file, err)
		}
		dir, err := store.SaveFrom(ctx, root, "ref", "selfdev", "ref-code", "chat")
		if err != nil || !dir.Dir {
			t.Fatalf("save dir: %+v, %v", dir, err)
		}
		// The same directory is the same digest.
		again, err := store.SaveFrom(ctx, root, "ref", "selfdev", "ref-code-2", "chat")
		if err != nil || again.Digest != dir.Digest {
			t.Fatalf("a directory saved twice: %+v vs %+v, %v", again, dir, err)
		}

		ref, _ := ParseRef(file.Ref)
		got, err := store.GetTo(ctx, root, "in/home.png", ref)
		if err != nil || got.Files != 1 || got.Path != "in/home.png" {
			t.Fatalf("get file: %+v, %v", got, err)
		}
		if data, _ := os.ReadFile(filepath.Join(workspace, "in/home.png")); string(data) != "png-bytes" {
			t.Fatalf("file holds %q", data)
		}
		// Into a directory, under the name.
		got, err = store.GetTo(ctx, root, "in", ref)
		if err != nil || got.Path != "in/home.png" {
			t.Fatalf("get into a directory: %+v, %v", got, err)
		}

		dirRef, _ := ParseRef(dir.Ref)
		got, err = store.GetTo(ctx, root, "copy", dirRef)
		if err != nil || !got.Dir || got.Files != 2 {
			t.Fatalf("get dir: %+v, %v", got, err)
		}
		if data, _ := os.ReadFile(filepath.Join(workspace, "copy/src/main.go")); string(data) != "package main" {
			t.Fatalf("unpacked file holds %q", data)
		}
		if info, err := os.Stat(filepath.Join(workspace, "copy/run.sh")); err != nil || info.Mode()&0o111 == 0 {
			t.Fatalf("run.sh lost its executable bit: %v %v", info, err)
		}
		for _, absent := range []string{"copy/.git", "copy/passwd"} {
			if _, err := os.Lstat(filepath.Join(workspace, absent)); err == nil {
				t.Fatalf("%s was saved", absent)
			}
		}
		if _, err := store.GetTo(ctx, root, "in/home.png", dirRef); err == nil {
			t.Fatal("a directory was unpacked over a file")
		}
	})
}

func TestWorkspaceRefusesEscapes(t *testing.T) {
	blobs, _ := NewDir(t.TempDir())
	store := &Store{Blobs: blobs, TempDir: t.TempDir()}
	ctx := context.Background()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(workspace, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret"), filepath.Join(workspace, "secret-link")); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(workspace)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	for _, p := range []string{"link/secret", "secret-link", "link"} {
		if _, err := store.SaveFrom(ctx, root, p, "selfdev", "x", ""); err == nil {
			t.Errorf("saved %s, which leaves the workspace", p)
		}
	}
	version, err := store.Save(ctx, "selfdev", "payload", "", strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetTo(ctx, root, "link/planted", Ref{Digest: version.Digest}); err == nil {
		t.Error("wrote through a link that leaves the workspace")
	}
	if _, err := os.Stat(filepath.Join(outside, "planted")); err == nil {
		t.Error("a file was planted outside")
	}
	for _, given := range []string{"../x", "a/../../x", "/etc/passwd", ""} {
		if _, err := WorkspacePath(workspace, given); err == nil {
			t.Errorf("WorkspacePath accepted %q", given)
		}
	}
	if got, err := WorkspacePath(workspace, filepath.Join(workspace, "a", "b")); err != nil || got != "a/b" {
		t.Errorf("an absolute path in the workspace: %q, %v", got, err)
	}
	if got, err := WorkspacePath(workspace, "./a//b/"); err != nil || got != "a/b" {
		t.Errorf("a clumsy path: %q, %v", got, err)
	}
}

func TestUnzipRefusesHostileArchives(t *testing.T) {
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	for name, entry := range map[string]string{"climb": "../evil", "absolute": "/evil", "nested climb": "a/../../evil"} {
		var buffer bytes.Buffer
		archive := newZip(&buffer, map[string]string{entry: "x"})
		if _, _, err := unzip(root, "out", archive, 1<<20); err == nil {
			t.Errorf("%s: unpacked %q", name, entry)
		}
	}
	var buffer bytes.Buffer
	archive := newZip(&buffer, map[string]string{"big": strings.Repeat("x", 100)})
	if _, _, err := unzip(root, "out", archive, 50); !errors.Is(err, ErrTooLarge) {
		t.Errorf("a big archive: %v", err)
	}
}

// A name may be the directory of another (a and a/b). The S3 store may keep
// both. The directory store cannot, and says so instead of reading a directory
// as a damaged index.
func TestANameThatIsTheDirectoryOfAnother(t *testing.T) {
	stores(t, func(t *testing.T, blobs Blobs) {
		ctx := context.Background()
		store := &Store{Blobs: blobs, TempDir: t.TempDir()}
		if _, err := store.Save(ctx, "selfdev", "a/b", "", strings.NewReader("b")); err != nil {
			t.Fatal(err)
		}
		_, err := store.Save(ctx, "selfdev", "a", "", strings.NewReader("a"))
		if err != nil && (strings.Contains(err.Error(), "damaged") || !strings.Contains(err.Error(), "directory store")) {
			t.Fatalf("a misleading error: %v", err)
		}
		if _, err := store.Lookup(ctx, "selfdev", "a"); err != nil && !errors.Is(err, ErrNotFound) {
			t.Fatalf("lookup of a: %v", err)
		}
		if _, err := store.Lookup(ctx, "selfdev", "a/b"); err != nil {
			t.Fatalf("a/b was lost: %v", err)
		}
	})
}
