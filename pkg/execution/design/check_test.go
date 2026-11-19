package design_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gfx-labs/protocol7/pkg/execution/design"
)

// A session's board is read from the file server in its pod when it has
// one, and from the file server beside the core worker when its pod's name
// does not resolve, which is what a session of the host backend has.
func TestFilesAreReadFromTheSessionsPodOrTheHost(t *testing.T) {
	var asked []string
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked = append(asked, r.URL.RequestURI())
		_, _ = io.WriteString(w, "from the host")
	}))
	defer host.Close()
	pod := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked = append(asked, "pod "+r.URL.RequestURI())
		_, _ = io.WriteString(w, "from the pod")
	}))
	defer pod.Close()
	// The pod's host name is the session's, which the test resolves itself.
	podClient := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, _, _ := net.SplitHostPort(address)
			if host == "p7-session-live.test" {
				address = pod.Listener.Addr().String()
			} else if host == "p7-session-gone.test" {
				return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
			}
			return (&net.Dialer{}).DialContext(ctx, network, address)
		},
	}}
	files := design.FilesClient{URL: host.URL, SessionURL: "http://p7-session-{session}.test:8788", HTTP: podClient}
	for session, want := range map[string]string{"live": "from the pod", "gone": "from the host"} {
		content, status, err := files.Read(t.Context(), session, "design/a/board.json")
		if err != nil || status != http.StatusOK || string(content) != want {
			t.Errorf("session %s: read %q, status %d, error %v, want %q", session, content, status, err, want)
		}
	}
	if len(asked) != 2 {
		t.Errorf("the servers were asked %v, want one request each", asked)
	}
	if !files.Configured() || (design.FilesClient{}).Configured() || !(design.FilesClient{SessionURL: "http://x-{session}"}).Configured() {
		t.Error("Configured is wrong")
	}
}

// The files of a session of a pool are in the pod of its worker, which
// {pod} names. A session with no pod named is looked for in the pod named for
// it, as before.
func TestFilesOfAPoolSessionAreReadFromTheWorkersPod(t *testing.T) {
	var dialed []string
	var asked []string
	pod := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked = append(asked, r.URL.Path)
		_, _ = io.WriteString(w, "from the pod")
	}))
	defer pod.Close()
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, _, _ := net.SplitHostPort(address)
			dialed = append(dialed, host)
			return (&net.Dialer{}).DialContext(ctx, network, pod.Listener.Addr().String())
		},
	}}
	files := design.FilesClient{SessionURL: "http://{pod}.p7-session:8788", HTTP: client}
	if _, _, err := files.ForPod("p7-pool-0123456789abcdef-1").Read(t.Context(), "child", "design/a/board.json"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := files.Read(t.Context(), "alone", "design/a/board.json"); err != nil {
		t.Fatal(err)
	}
	if len(dialed) != 2 || dialed[0] != "p7-pool-0123456789abcdef-1.p7-session" || dialed[1] != "p7-session-alone.p7-session" {
		t.Errorf("dialed %v", dialed)
	}
	if len(asked) != 2 || asked[0] != "/sessions/child/files/design/a/board.json" {
		t.Errorf("asked %v: the session's own directory is read in the shared pod", asked)
	}
}
