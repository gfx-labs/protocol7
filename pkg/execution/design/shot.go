package design

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

// shotTimeout bounds one screenshot: Chromium's start, the page's load and
// its scripts.
const shotTimeout = 30 * time.Second

// shoot renders a page of a session's workspace at a viewport size with
// headless Chromium and returns a JPEG. Chromium loads the page from this
// server, so it gets the same files and sandbox as the page beside the chat.
func (f *Files) shoot(w http.ResponseWriter, r *http.Request) {
	if f.config.Chromium == "" {
		http.Error(w, "screenshots are not set up on this server", http.StatusNotFound)
		return
	}
	root, name, ok := f.workspace(w, r)
	if !ok {
		return
	}
	info, err := root.Stat(name)
	root.Close()
	if err != nil {
		http.Error(w, "no such page", http.StatusNotFound)
		return
	}
	if info.IsDir() {
		name += "/index.html"
	}
	width, errWidth := strconv.Atoi(r.URL.Query().Get("width"))
	height, errHeight := strconv.Atoi(r.URL.Query().Get("height"))
	if errWidth != nil || errHeight != nil || width < MinSize || width > MaxSize || height < MinSize || height > MaxSize {
		http.Error(w, fmt.Sprintf("width and height must be %d to %d", MinSize, MaxSize), http.StatusBadRequest)
		return
	}

	select {
	case f.shots <- struct{}{}:
		defer func() { <-f.shots }()
	case <-r.Context().Done():
		return
	}
	scratch, err := os.MkdirTemp("", "shot")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer os.RemoveAll(scratch)
	output := filepath.Join(scratch, "shot.jpeg")
	page := f.self + "/sessions/" + url.PathEscape(r.PathValue("session")) + "/files/" + (&url.URL{Path: name}).EscapedPath()

	ctx, cancel := context.WithTimeout(r.Context(), shotTimeout)
	defer cancel()
	command := exec.CommandContext(ctx, f.config.Chromium,
		"--headless", "--disable-gpu", "--hide-scrollbars", "--no-first-run", "--mute-audio",
		// The container gives Chromium no user namespaces for its own sandbox.
		// The page is still held to an opaque origin by the response's CSP.
		"--no-sandbox",
		// A container's /dev/shm is 64 MiB, which a large page can fill.
		"--disable-dev-shm-usage",
		"--user-data-dir="+filepath.Join(scratch, "profile"),
		fmt.Sprintf("--window-size=%d,%d", width, height),
		// Time for the page's scripts and fonts after the load event.
		"--virtual-time-budget=3000",
		"--screenshot="+output,
		page,
	)
	command.Dir = scratch
	command.Env = append(os.Environ(), "HOME="+scratch)
	if out, err := command.CombinedOutput(); err != nil {
		f.logger.Warn("screenshot", "page", name, "error", err, "output", string(out))
		http.Error(w, "Chromium could not render the page: "+err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("Cache-Control", "no-store")
	http.ServeFile(w, r, output)
}
