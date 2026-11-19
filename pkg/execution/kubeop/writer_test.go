package kubeop

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteReplacesTheFilesAndStaysInTheWorkspace(t *testing.T) {
	state := t.TempDir()
	workspace := filepath.Join(state, "sessions", "s1", "work")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatal(err)
	}
	writer := Writer{StateDir: state}
	credentials := Credentials{Token: "tok1", Server: "https://10.43.0.1:443", CA: "-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----\n"}
	result, err := writer.Write(context.Background(), WriteRequest{Session: "s1", Credentials: credentials})
	if err != nil {
		t.Fatal(err)
	}
	if result.Kubeconfig != filepath.Join(workspace, Dir, "kubeconfig") {
		t.Errorf("kubeconfig at %s", result.Kubeconfig)
	}
	config, _ := os.ReadFile(result.Kubeconfig)
	for _, want := range []string{`server: "https://10.43.0.1:443"`, "tokenFile: " + `"` + result.TokenFile + `"`, "current-context: selfdev-operator"} {
		if !strings.Contains(string(config), want) {
			t.Errorf("kubeconfig lacks %q:\n%s", want, config)
		}
	}
	if strings.Contains(string(config), "tok1") {
		t.Error("the kubeconfig holds the token itself")
	}
	// A renewal replaces the token and leaves no temporary file.
	credentials.Token = "tok2"
	if _, err := writer.Write(context.Background(), WriteRequest{Session: "s1", Credentials: credentials}); err != nil {
		t.Fatal(err)
	}
	token, _ := os.ReadFile(result.TokenFile)
	info, _ := os.Stat(result.TokenFile)
	if string(token) != "tok2" || info.Mode().Perm() != 0o600 {
		t.Errorf("token %q mode %v", token, info.Mode())
	}
	entries, _ := os.ReadDir(filepath.Join(workspace, Dir))
	if len(entries) != 3 {
		t.Errorf("%d files in %s", len(entries), Dir)
	}
	for name, request := range map[string]WriteRequest{
		"a session that is not there": {Session: "nope", Credentials: credentials},
		"a path as the session":       {Session: "../../etc", Credentials: credentials},
		"a plain-text server":         {Session: "s1", Credentials: Credentials{Token: "t", Server: "http://x", CA: credentials.CA}},
		"a token with a newline":      {Session: "s1", Credentials: Credentials{Token: "a\nb", Server: credentials.Server, CA: credentials.CA}},
		"no CA":                       {Session: "s1", Credentials: Credentials{Token: "t", Server: credentials.Server}},
	} {
		if _, err := writer.Write(context.Background(), request); err == nil {
			t.Errorf("%s: written", name)
		}
	}
}
