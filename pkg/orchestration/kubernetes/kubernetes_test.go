package kubernetes_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/orchestration/kubernetes"
)

// Starting a session's pod posts the template with the session's name,
// labels, owner and environment, keeps what the template set, and a second
// start of the same session is not an error. Deleting a pod that is gone is
// not an error either.
func TestStartAndDeleteASessionPod(t *testing.T) {
	var posted map[string]any
	created := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			t.Errorf("%s %s sent %q, want the token", r.Method, r.URL.Path, r.Header.Get("Authorization"))
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/namespaces/p7/pods":
			if created {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"message":"exists"}`))
				return
			}
			body, _ := io.ReadAll(r.Body)
			if err := json.Unmarshal(body, &posted); err != nil {
				t.Fatal(err)
			}
			created = true
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/namespaces/p7/pods/p7-session-s1":
			if !created {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"message":"not found"}`))
				return
			}
			created = false
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	dir := t.TempDir()
	token, template := filepath.Join(dir, "token"), filepath.Join(dir, "pod.json")
	if err := os.WriteFile(token, []byte("tok\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	pod := `{"metadata":{"labels":{"app":"p7-session"},"resourceVersion":"9"},"spec":{"serviceAccountName":"p7-session",
		"containers":[{"name":"forward"},{"name":"session","image":"img","env":[{"name":"P7_SESSION","value":"stale"},{"name":"KEEP","value":"1"}]}]}}`
	if err := os.WriteFile(template, []byte(pod), 0o600); err != nil {
		t.Fatal(err)
	}
	pods, err := kubernetes.New(kubernetes.Config{Namespace: "p7", TemplateFile: template, SessionContainer: "session", APIServer: server.URL, TokenFile: token}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	request := domain.PodRequest{Session: "s1", Owner: "chat-x", OpenTimeout: 10 * time.Minute}
	handle, err := pods.Start(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if handle.Name != "p7-session-s1" || handle.TaskQueue != "p7-session-s1" {
		t.Errorf("handle %+v", handle)
	}
	metadata := posted["metadata"].(map[string]any)
	labels := metadata["labels"].(map[string]any)
	if metadata["name"] != "p7-session-s1" || metadata["resourceVersion"] != nil || labels["app"] != "p7-session" ||
		labels[kubernetes.LabelSession] != "s1" || metadata["annotations"].(map[string]any)[kubernetes.AnnotationOwner] != "chat-x" ||
		metadata["annotations"].(map[string]any)[kubernetes.AnnotationOpenTimeout] != "10m0s" {
		t.Errorf("metadata %v", metadata)
	}
	session := posted["spec"].(map[string]any)["containers"].([]any)[1].(map[string]any)
	env := map[string]string{}
	for _, variable := range session["env"].([]any) {
		named := variable.(map[string]any)
		if _, seen := env[named["name"].(string)]; seen {
			t.Errorf("%s set twice", named["name"])
		}
		env[named["name"].(string)] = named["value"].(string)
	}
	if env[kubernetes.EnvSession] != "s1" || env[kubernetes.EnvTaskQueue] != "p7-session-s1" || env["KEEP"] != "1" {
		t.Errorf("env %v", env)
	}

	if _, err := pods.Start(ctx, request); err != nil {
		t.Errorf("a second start: %v", err)
	}
	for range 2 {
		if err := pods.Delete(ctx, request); err != nil {
			t.Errorf("delete: %v", err)
		}
	}
}
