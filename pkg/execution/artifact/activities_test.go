package artifact

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func TestActivities(t *testing.T) {
	blobs, _ := NewDir(t.TempDir())
	stateDir := t.TempDir()
	workspace := filepath.Join(stateDir, "sessions", "abc123", "work")
	if err := os.MkdirAll(filepath.Join(workspace, "out"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "out", "report.md"), []byte("# report"), 0o644); err != nil {
		t.Fatal(err)
	}
	activities := Activities{Store: &Store{Blobs: blobs, TempDir: t.TempDir()}, StateDir: stateDir}
	ctx := context.Background()

	saved, err := activities.Save(ctx, SaveRequest{Session: "abc123", Project: "selfdev", Path: "out/report.md", Name: "reports/one.md"})
	if err != nil || saved.Digest != digestOf("# report") {
		t.Fatalf("save: %+v, %v", saved, err)
	}
	// The absolute path that the agent sees works too.
	if _, err := activities.Save(ctx, SaveRequest{Session: "abc123", Project: "selfdev", Path: filepath.Join(workspace, "out"), Name: "reports/dir"}); err != nil {
		t.Fatalf("save of an absolute path: %v", err)
	}
	listed, err := activities.List(ctx, ListRequest{Project: "selfdev", Prefix: "reports/"})
	if err != nil || len(listed.Entries) != 2 {
		t.Fatalf("list: %+v, %v", listed, err)
	}
	// By name, and by reference, into another session's workspace.
	other := filepath.Join(stateDir, "sessions", "other", "work")
	if err := os.MkdirAll(other, 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := activities.Get(ctx, GetRequest{Session: "other", Project: "selfdev", Name: "reports/one.md", Path: "got.md"})
	if err != nil || got.Files != 1 {
		t.Fatalf("get by name: %+v, %v", got, err)
	}
	if data, _ := os.ReadFile(filepath.Join(other, "got.md")); string(data) != "# report" {
		t.Fatalf("got %q", data)
	}
	if _, err := activities.Get(ctx, GetRequest{Session: "other", Ref: saved.Ref, Path: "again.md"}); err != nil {
		t.Fatalf("get by ref: %v", err)
	}

	// What no retry fixes is final.
	for name, call := range map[string]func() error{
		"missing name": func() error {
			_, err := activities.Get(ctx, GetRequest{Session: "other", Project: "selfdev", Name: "nope", Path: "x"})
			return err
		},
		"missing digest": func() error {
			_, err := activities.Get(ctx, GetRequest{Session: "other", Ref: "p7://sha256:" + digestOf("never"), Path: "x"})
			return err
		},
		"bad ref": func() error {
			_, err := activities.Get(ctx, GetRequest{Session: "other", Ref: "http://x", Path: "x"})
			return err
		},
		"no ref": func() error { _, err := activities.Get(ctx, GetRequest{Session: "other", Path: "x"}); return err },
		"climb": func() error {
			_, err := activities.Save(ctx, SaveRequest{Session: "abc123", Project: "selfdev", Path: "../x", Name: "x"})
			return err
		},
		"bad session": func() error {
			_, err := activities.Save(ctx, SaveRequest{Session: "../x", Project: "selfdev", Path: "x", Name: "x"})
			return err
		},
		"bad name": func() error {
			_, err := activities.Save(ctx, SaveRequest{Session: "abc123", Project: "selfdev", Path: "out", Name: "../x"})
			return err
		},
		"missing file": func() error {
			_, err := activities.Save(ctx, SaveRequest{Session: "abc123", Project: "selfdev", Path: "nothing", Name: "x"})
			return err
		},
	} {
		err := call()
		var application *temporal.ApplicationError
		if !errors.As(err, &application) || !application.NonRetryable() {
			t.Errorf("%s: %v is not a final error", name, err)
		}
	}
	if _, err := (Activities{}).List(ctx, ListRequest{Project: "selfdev"}); err == nil {
		t.Error("an unconfigured activity answered")
	}
}

// The activities run through Temporal's codecs under their registered names,
// and an error that no retry fixes arrives as one.
func TestActivitiesThroughTemporal(t *testing.T) {
	blobs, _ := NewDir(t.TempDir())
	stateDir := t.TempDir()
	workspace := filepath.Join(stateDir, "sessions", "abc123", "work")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	activities := Activities{Store: &Store{Blobs: blobs, TempDir: t.TempDir()}, StateDir: stateDir}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivityWithOptions(activities.Save, activity.RegisterOptions{Name: ActivitySave})
	env.RegisterActivityWithOptions(activities.Get, activity.RegisterOptions{Name: ActivityGet})
	env.RegisterActivityWithOptions(activities.List, activity.RegisterOptions{Name: ActivityList})

	value, err := env.ExecuteActivity(ActivitySave, SaveRequest{Session: "abc123", Project: "selfdev", Path: "a.txt", Name: "notes/a.txt"})
	if err != nil {
		t.Fatal(err)
	}
	var saved SaveResult
	if err := value.Get(&saved); err != nil || saved.Digest != digestOf("hello") {
		t.Fatalf("save: %+v, %v", saved, err)
	}
	value, err = env.ExecuteActivity(ActivityList, ListRequest{Project: "selfdev"})
	if err != nil {
		t.Fatal(err)
	}
	var listed ListResult
	if err := value.Get(&listed); err != nil || len(listed.Entries) != 1 || listed.Entries[0].Digest != saved.Digest || listed.Entries[0].Saved.IsZero() {
		t.Fatalf("list: %+v, %v", listed, err)
	}
	if _, err = env.ExecuteActivity(ActivityGet, GetRequest{Session: "abc123", Project: "selfdev", Name: "missing", Path: "x"}); err == nil || !strings.Contains(err.Error(), "no artifact named") {
		t.Fatalf("get of a missing name: %v", err)
	}
}
