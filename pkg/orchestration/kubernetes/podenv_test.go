package kubernetes

import (
	"reflect"
	"strings"
	"testing"
)

func TestAPoolReplacesTheEnvironmentOfTheTemplate(t *testing.T) {
	container := func() map[string]any {
		return map[string]any{"env": []any{
			map[string]any{"name": "GH_TOKEN", "value": "pdp-proxied-github-chat"},
			map[string]any{"name": "KEEP", "value": "1"},
			map[string]any{"name": "FROM", "valueFrom": map[string]any{"fieldRef": map[string]any{"fieldPath": "metadata.name"}}},
		}}
	}
	replaced := container()
	if err := replaceEnv(replaced, map[string]string{"GH_TOKEN": "pdp-proxied-github-research", "FROM": "x"}); err != nil {
		t.Fatal(err)
	}
	got := map[string]map[string]any{}
	for _, variable := range replaced["env"].([]any) {
		named := variable.(map[string]any)
		got[named["name"].(string)] = named
	}
	if got["GH_TOKEN"]["value"] != "pdp-proxied-github-research" || got["KEEP"]["value"] != "1" {
		t.Errorf("env %v, want GH_TOKEN replaced and KEEP kept", got)
	}
	if _, has := got["FROM"]["valueFrom"]; has || got["FROM"]["value"] != "x" {
		t.Errorf("FROM is %v, want a plain value in place of the reference", got["FROM"])
	}

	for name, want := range map[string]string{"GH_TOKEN_TYPO": "no variable", EnvPool: "the pool's own", EnvTaskQueue: "the pool's own"} {
		err := replaceEnv(container(), map[string]string{name: "x"})
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("replacing %s: error %v, want one that says %q", name, err, want)
		}
	}
}

func TestAPoolWithoutSnapshotsLosesTheSnapshotFlagsOfTheTemplate(t *testing.T) {
	container := map[string]any{
		"args": []any{"p7", "session", "--state-dir", "/var/lib/p7", "--snapshot-bucket", "p7-snapshots", "--snapshot-endpoint=http://s3", "--snapshot-region", "us-east-1", "--snapshot-path-style"},
		"env":  []any{map[string]any{"name": "P7_SNAPSHOT_BUCKET", "value": "b"}, map[string]any{"name": "KEEP", "value": "1"}},
	}
	dropSnapshots(container)
	want := []any{"p7", "session", "--state-dir", "/var/lib/p7"}
	if got := container["args"].([]any); !reflect.DeepEqual(got, want) {
		t.Errorf("args %v, want %v", got, want)
	}
	env := container["env"].([]any)
	if len(env) != 1 || env[0].(map[string]any)["name"] != "KEEP" {
		t.Errorf("env %v, want only KEEP", env)
	}
}
