package cli

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gfx-labs/protocol7/pkg/execution/design"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// p7-worker files serves a session's design board from the state directory:
// the board checks, its page is fetched, and stopping the process ends it.
func TestFilesServesABoard(t *testing.T) {
	state := t.TempDir()
	workspace, err := p7.HostWorkspace(state, "s1")
	if err != nil {
		t.Fatal(err)
	}
	board := filepath.Join(workspace, "app", "design", "home")
	if err := os.MkdirAll(board, 0o755); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		design.ManifestName: `{"title":"Home","artboards":[{"id":"a","page":"index.html","width":800,"height":600}]}`,
		"index.html":        "<h1>home</h1>",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(board, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	listen := freeAddress(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	command := &filesCommand{Listen: listen, StateDir: state}
	go func() {
		code := 0
		if err := command.Run(ctx, slog.New(slog.DiscardHandler)); err != nil {
			code = 1
		}
		done <- code
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
	}()
	waitListening(t, listen, done)

	client := design.FilesClient{URL: "http://" + listen}
	view, err := client.Check(ctx, "s1", "app/design/home")
	if err != nil {
		t.Fatal(err)
	}
	if view.Problem != "" || view.Title != "Home" || len(view.Artboards) != 1 {
		t.Fatalf("view %+v", view)
	}
	response, err := client.Get(ctx, "files", "s1", "app/design/home/index.html", "")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	page, _ := io.ReadAll(response.Body)
	if response.StatusCode != 200 || string(page) != files["index.html"] {
		t.Fatalf("page %d %q", response.StatusCode, page)
	}
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	return address
}

func waitListening(t *testing.T, address string, done <-chan int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case code := <-done:
			t.Fatalf("p7-worker files exited with %d", code)
		default:
		}
		if conn, err := net.Dial("tcp", address); err == nil {
			conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("nothing listens on %s", address)
}
