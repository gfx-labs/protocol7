package design

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

// FilesClient reads session files from the file server beside the core
// worker.
type FilesClient struct {
	// URL is the file server's base URL, such as http://p7-worker:8788.
	URL string
	// SessionURL is the base URL of the file server in a session's own pod
	// on the kubernetes backend, with {session} for the session's ID and {pod} for the name of its pod, such
	// as http://p7-session-{session}.p7-session:8788. A session of the host
	// backend has no such pod, so its name does not resolve and it is read
	// from URL.
	SessionURL string
	// Pod is the name of the pod that holds the session, when it is not
	// p7-session-<session>: a session of a pool is in the pod of its
	// worker. It replaces {pod} in SessionURL.
	Pod string
	// HTTP is the client that asks. Nil is http.DefaultClient.
	HTTP *http.Client
}

// Configured says whether there is a file server to ask.
func (c FilesClient) Configured() bool { return c.URL != "" || c.SessionURL != "" }

// FileURL is the URL of a route of the file server: kind is "files" or
// "changes", name a path in the session's workspace.
func (c FilesClient) FileURL(kind, session, name string) string {
	return fileURL(c.URL, kind, session, name)
}

func fileURL(base, kind, session, name string) string {
	return strings.TrimSuffix(base, "/") + "/sessions/" + url.PathEscape(session) + "/" + kind + "/" + (&url.URL{Path: name}).EscapedPath()
}

// ForPod is the client for a session whose pod has this name. An empty name
// is the pod named for the session.
func (c FilesClient) ForPod(pod string) FilesClient {
	c.Pod = pod
	return c
}

// Get asks the session's file server for a route and returns the response,
// which the caller closes. query is "" or starts with "?". The server is the
// one in the session's pod if the session has one, and else the one beside
// the core worker.
func (c FilesClient) Get(ctx context.Context, kind, session, name, query string) (*http.Response, error) {
	var bases []string
	if c.SessionURL != "" {
		pod := c.Pod
		if pod == "" {
			pod = "p7-session-" + session
		}
		bases = append(bases, strings.ReplaceAll(strings.ReplaceAll(c.SessionURL, "{pod}", pod), "{session}", session))
	}
	if c.URL != "" {
		bases = append(bases, c.URL)
	}
	if len(bases) == 0 {
		return nil, errors.New("there is no file server")
	}
	for i, base := range bases {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL(base, kind, session, name)+query, nil)
		if err != nil {
			return nil, err
		}
		httpClient := c.HTTP
		if httpClient == nil {
			httpClient = http.DefaultClient
		}
		response, err := httpClient.Do(request)
		var missing *net.DNSError
		if err != nil && i < len(bases)-1 && errors.As(err, &missing) && missing.IsNotFound {
			continue
		}
		return response, err
	}
	panic("unreachable")
}

// Read reads one file of up to 1 MiB and returns it with the HTTP status.
// A status other than 200 comes with the server's message as the content.
func (c FilesClient) Read(ctx context.Context, session, name string) ([]byte, int, error) {
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	response, err := c.Get(readCtx, "files", session, name, "")
	if err != nil {
		return nil, 0, fmt.Errorf("read %s from the file server: %w", name, err)
	}
	defer response.Body.Close()
	content, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	return content, response.StatusCode, err
}

// View is a board as the page draws it and as the agent is told about it.
type View struct {
	Dir       string   `json:"dir"`
	Title     string   `json:"title"`
	Artboards []Placed `json:"artboards"`
	// Problem says why the board cannot be drawn in full: a board.json that
	// is missing or does not parse, or a page that does not exist. The page
	// shows it over the board.
	Problem string `json:"problem,omitempty"`
}

// Check reads the board in dir of a session's workspace and checks it and
// every page it names. A board that cannot be drawn is a View with its
// Problem set. The error is for a file server that cannot be reached.
func (c FilesClient) Check(ctx context.Context, session, dir string) (View, error) {
	dir, err := BoardPath(dir)
	if err != nil {
		return View{Problem: err.Error()}, nil
	}
	view := View{Dir: dir}
	content, status, err := c.Read(ctx, session, path.Join(dir, ManifestName))
	switch {
	case err != nil:
		return view, err
	case status == http.StatusNotFound:
		view.Problem = fmt.Sprintf("%s has no %s.", dir, ManifestName)
		return view, nil
	case status != http.StatusOK:
		view.Problem = fmt.Sprintf("%s could not be read: %s", ManifestName, strings.TrimSpace(string(content)))
		return view, nil
	}
	board, err := Parse(content)
	view.Title = board.Title
	if err != nil {
		view.Problem = err.Error()
		return view, nil
	}
	view.Artboards = Layout(board)
	var missing []string
	for _, artboard := range board.Artboards {
		page, _ := PagePath(artboard.Page)
		_, status, err := c.Read(ctx, session, path.Join(dir, page))
		if err != nil {
			return view, err
		}
		if status != http.StatusOK {
			missing = append(missing, fmt.Sprintf("artboard %q: %s does not exist", artboard.ID, page))
		}
	}
	view.Problem = strings.Join(missing, "\n")
	return view, nil
}
