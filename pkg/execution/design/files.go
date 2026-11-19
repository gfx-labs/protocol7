package design

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/gfx-labs/protocol7/pkg/p7"
)

// FilesConfig configures the file server.
type FilesConfig struct {
	// Listen is the address the server listens on.
	Listen string
	// StateDir is the core worker's state directory, which holds the
	// sessions. The design server only reads it. (The artifact activities, which run in the same process, also write a session's workspace: protocol7 pkg/execution/artifact.)
	StateDir string
	// Chromium is the headless Chromium command that renders screenshots.
	// Empty turns screenshots off.
	Chromium string
}

// Files serves the files of each session's workspace, and a stream of
// their changes, to the web UI. It runs beside the core worker, on the
// machine that holds the workspaces. It has no authentication: anything
// that reaches it reads every workspace, so it listens where only the web
// UI connects.
//
// Hidden files and directories, such as .git, are not served, and a
// symbolic link that leads out of the workspace is not followed.
type Files struct {
	config FilesConfig
	logger *slog.Logger
	http   *http.Server
	stop   context.CancelFunc
	failed chan error
	// self is the server's own base URL.
	self string
	// shots bounds the Chromium processes that run at once.
	shots chan struct{}
}

// NewFiles returns a file server. It does not listen until Start.
func NewFiles(config FilesConfig, logger *slog.Logger) (*Files, error) {
	if config.StateDir == "" {
		return nil, errors.New("files: a state directory is required")
	}
	return &Files{config: config, logger: logger, failed: make(chan error, 1), shots: make(chan struct{}, 2)}, nil
}

// Start listens and serves in the background.
func (f *Files) Start() error {
	listener, err := net.Listen("tcp", f.config.Listen)
	if err != nil {
		return fmt.Errorf("files: listen on %s: %w", f.config.Listen, err)
	}
	streams, stop := context.WithCancel(context.Background())
	f.stop = stop
	f.http = &http.Server{
		Handler:           f.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return streams },
	}
	// Chromium loads a page to shoot from this server, over loopback.
	f.self = "http://" + listener.Addr().String()
	if tcp, ok := listener.Addr().(*net.TCPAddr); ok && tcp.IP.IsUnspecified() {
		f.self = fmt.Sprintf("http://127.0.0.1:%d", tcp.Port)
	}
	f.logger.Info("session files are served", "address", listener.Addr().String(), "state_dir", f.config.StateDir)
	go func() {
		if err := f.http.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
			f.failed <- fmt.Errorf("files: %w", err)
		}
	}()
	return nil
}

// Failed receives an error if the server stops serving without being asked.
func (f *Files) Failed() <-chan error { return f.failed }

// Stop ends the change streams and closes the listener.
func (f *Files) Stop(ctx context.Context) error {
	if f.http == nil {
		return nil
	}
	f.stop()
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := f.http.Shutdown(shutdownCtx); err != nil {
		return f.http.Close()
	}
	return nil
}

// Handler is the server's routes:
//
//	GET /sessions/{session}/files/{path...}  one file of the workspace
//	GET /sessions/{session}/changes/{path...}  server-sent events, one
//	    "change" event for each burst of changes under a directory
//	GET /sessions/{session}/shot/{path...}?width=&height=  a JPEG of a page
func (f *Files) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /sessions/{session}/files/{path...}", f.serveFile)
	mux.HandleFunc("GET /sessions/{session}/changes/{path...}", f.watch)
	mux.HandleFunc("GET /sessions/{session}/shot/{path...}", f.shoot)
	return mux
}

// maxFileSize bounds one served file. A design page and its assets are far
// smaller.
const maxFileSize = 32 << 20

// workspace opens the session's workspace and checks the path in it.
func (f *Files) workspace(w http.ResponseWriter, r *http.Request) (*os.Root, string, bool) {
	dir, err := p7.HostWorkspace(f.config.StateDir, r.PathValue("session"))
	if err != nil {
		http.Error(w, "no such session", http.StatusNotFound)
		return nil, "", false
	}
	name, err := PagePath(r.PathValue("path"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return nil, "", false
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		http.Error(w, "no such session", http.StatusNotFound)
		return nil, "", false
	}
	return root, name, true
}

func (f *Files) serveFile(w http.ResponseWriter, r *http.Request) {
	root, name, ok := f.workspace(w, r)
	if !ok {
		return
	}
	defer root.Close()
	file, err := root.Open(name)
	if err == nil {
		var info fs.FileInfo
		if info, err = file.Stat(); err == nil && info.IsDir() {
			// A directory is its index page.
			file.Close()
			name = path.Join(name, "index.html")
			file, err = root.Open(name)
		}
	}
	if err != nil {
		http.Error(w, "no such file", http.StatusNotFound)
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		http.Error(w, "no such file", http.StatusNotFound)
		return
	}
	if info.Size() > maxFileSize {
		http.Error(w, "the file is larger than 32 MiB", http.StatusRequestEntityTooLarge)
		return
	}
	// The agent rewrites files in place, so nothing is cached.
	w.Header().Set("Cache-Control", "no-store")
	http.ServeContent(w, r, name, info.ModTime(), file)
}

// settle is how long a burst of changes must be quiet before it is
// reported. An agent's edit is often several writes and a rename.
const settle = 150 * time.Millisecond

// watch streams the changes under a directory of the workspace. The page
// reloads the board on each event.
func (f *Files) watch(w http.ResponseWriter, r *http.Request) {
	root, name, ok := f.workspace(w, r)
	if !ok {
		return
	}
	dir := filepath.Join(root.Name(), filepath.FromSlash(name))
	// The root's Stat refuses a symbolic link that leads out of the workspace.
	info, err := root.Stat(name)
	root.Close()
	if err != nil || !info.IsDir() {
		http.Error(w, "no such directory", http.StatusNotFound)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming is not supported", http.StatusInternalServerError)
		return
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		f.logger.Error("watch", "dir", dir, "error", err)
		http.Error(w, "the directory cannot be watched: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer watcher.Close()
	if err := addTree(watcher, dir); err != nil {
		http.Error(w, "the directory cannot be watched: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	send := func(event, data string) bool {
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	if !send("ready", "{}") {
		return
	}

	quiet := time.NewTimer(time.Hour)
	quiet.Stop()
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	changed := false
	for {
		select {
		case <-r.Context().Done():
			return
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			if hidden(dir, event.Name) {
				continue
			}
			// fsnotify watches one directory at a time. A new directory
			// gets its own watch.
			if event.Has(fsnotify.Create) {
				if info, err := os.Lstat(event.Name); err == nil && info.IsDir() {
					_ = addTree(watcher, event.Name)
				}
			}
			changed = true
			quiet.Reset(settle)
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			f.logger.Warn("watch", "dir", dir, "error", err)
		case <-quiet.C:
			if changed && !send("change", "{}") {
				return
			}
			changed = false
		case <-ping.C:
			if !send("ping", "{}") {
				return
			}
		}
	}
}

// addTree watches dir and every directory under it that is not hidden. It
// does not follow symbolic links.
func addTree(watcher *fsnotify.Watcher, dir string) error {
	return filepath.WalkDir(dir, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return nil
		}
		if name != dir && strings.HasPrefix(entry.Name(), ".") {
			return filepath.SkipDir
		}
		return watcher.Add(name)
	})
}

// hidden says whether name, under dir, is or is inside a hidden file or
// directory.
func hidden(dir, name string) bool {
	relative, err := filepath.Rel(dir, name)
	if err != nil {
		return true
	}
	for part := range strings.SplitSeq(filepath.ToSlash(relative), "/") {
		if strings.HasPrefix(part, ".") && part != "." {
			return true
		}
	}
	return false
}
