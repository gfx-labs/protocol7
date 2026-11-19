// Package githubapi makes GitHub REST calls with an installation token. The
// token is a file that another process keeps (selfdev github-token), or a
// placeholder that PDP swaps for the token (see TokenSource). The review and
// issue activities share it.
package githubapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"go.temporal.io/sdk/temporal"
)

// ErrorRefused is the error type of a request that GitHub or an activity's own
// rules refused, and that trying again cannot change.
const ErrorRefused = "Refused"

// Client calls GitHub's API.
type Client struct {
	// TokenFile is what New was given: the file of an installation token, or
	// "placeholder:<value>" (ParseTokenSource). Source is what is used.
	TokenFile string
	// Source gives the bearer token on every call, since a file is replaced
	// before the token expires.
	Source TokenSource
	// APIURL is GitHub's API. Empty is https://api.github.com.
	APIURL string
	http   atomic.Pointer[http.Client]
}

// New makes a client. tokenFile is a file, or "placeholder:<value>" for a
// process that holds no token and calls through PDP (HTTPS_PROXY). It reads
// nothing until a call. The client's transport is Go's default, which sends
// every call through the proxy of the environment unless NO_PROXY names the
// host.
func New(tokenFile, apiURL string) *Client {
	return NewWithSource(ParseTokenSource(tokenFile), apiURL, tokenFile)
}

// NewWithSource makes a client with a token source. name is only what
// Client.TokenFile reports.
func NewWithSource(source TokenSource, apiURL, name string) *Client {
	if apiURL == "" {
		apiURL = "https://api.github.com"
	}
	c := &Client{TokenFile: name, Source: source, APIURL: apiURL}
	c.http.Store(&http.Client{Timeout: 30 * time.Second})
	return c
}

// NewThroughPDP makes a client that sends placeholder as its bearer token and
// holds no token. Until UseHTTP gives it a client whose calls go through PDP,
// every call fails without leaving the process: the placeholder must never
// reach GitHub itself. It is for a process that reaches PDP by its own SPIFFE
// identity (protocol7's pkg/transport/egress) and has no forwarder.
func NewThroughPDP(placeholder, apiURL string) *Client {
	c := NewWithSource(PlaceholderToken(placeholder), apiURL, PlaceholderPrefix+placeholder)
	c.http.Store(&http.Client{Transport: noRoute{}})
	return c
}

// UseHTTP sets the client that makes the calls, such as one that sends them
// through PDP.
func (c *Client) UseHTTP(client *http.Client) { c.http.Store(client) }

// noRoute refuses a request, so that a client that goes through PDP never
// sends its placeholder anywhere before it has a route.
type noRoute struct{}

func (noRoute) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("githubapi: the route through PDP is not open yet")
}

// StatusError is a response that was not a success.
type StatusError struct {
	Code    int
	Message string
	// Retry is set for what may pass: a server error or a rate limit.
	Retry bool
}

func (e *StatusError) Error() string { return fmt.Sprintf("GitHub answered %d: %s", e.Code, e.Message) }

// IsStatus says whether err is a response with the code.
func IsStatus(err error, code int) bool {
	var status *StatusError
	return errors.As(err, &status) && status.Code == code
}

// Settle turns a response that trying again cannot change into an error that
// Temporal does not retry.
func Settle(err error) error {
	var status *StatusError
	if errors.As(err, &status) && !status.Retry {
		return temporal.NewNonRetryableApplicationError(status.Error(), ErrorRefused, err)
	}
	return err
}

// Do makes one API call. path starts with a slash. A response that is not a
// success is a *StatusError. result may be nil.
func (c *Client) Do(ctx context.Context, method, path string, body, result any) error {
	response, err := c.send(ctx, method, path, body)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	content, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		return err
	}
	if err := statusError(response, content); err != nil {
		return err
	}
	if result != nil {
		return json.Unmarshal(content, result)
	}
	return nil
}

// tailWindow is how many of the last bytes of a response Tail keeps at least.
const tailWindow = 512 << 10

// Tail makes a GET call whose response is text, such as a job's log, and
// returns the last lines of it, at most lines of at most width bytes each,
// and whether lines came before them. The response is read to its end, but
// only a window of its last bytes is kept, so a log of any size costs the
// same memory.
func (c *Client) Tail(ctx context.Context, path string, lines, width int) (tail []string, cut bool, err error) {
	response, err := c.send(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, false, err
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		content, _ := io.ReadAll(io.LimitReader(response.Body, 1<<16))
		return nil, false, statusError(response, content)
	}
	window := max(tailWindow, lines*(width+1))
	var kept []byte
	buffer := make([]byte, 32<<10)
	for {
		n, readErr := response.Body.Read(buffer)
		kept = append(kept, buffer[:n]...)
		if len(kept) > 2*window {
			kept, cut = append([]byte(nil), kept[len(kept)-window:]...), true
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, false, readErr
		}
	}
	if len(kept) > window {
		kept, cut = kept[len(kept)-window:], true
	}
	text := strings.TrimRight(string(kept), "\r\n")
	all := strings.Split(text, "\n")
	if cut && len(all) > 0 {
		// The first line is cut at the window's edge.
		all = all[1:]
	}
	if len(all) > lines {
		cut = true
		all = all[len(all)-lines:]
	}
	for i, line := range all {
		line = strings.TrimRight(line, "\r")
		if len(line) > width {
			// Cut on a character boundary.
			cut := width
			for cut > 0 && !utf8.RuneStart(line[cut]) {
				cut--
			}
			line = line[:cut] + "..."
		}
		all[i] = line
	}
	return all, cut, nil
}

func (c *Client) send(ctx context.Context, method, path string, body any) (*http.Response, error) {
	source := c.Source
	if source == nil {
		source = ParseTokenSource(c.TokenFile)
	}
	token, err := source.Token()
	if err != nil {
		return nil, err
	}
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.APIURL+path, reader)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	return c.http.Load().Do(request)
}

// statusError is nil for a success.
func statusError(response *http.Response, content []byte) error {
	if response.StatusCode < 300 {
		return nil
	}
	var failure struct {
		Message string `json:"message"`
	}
	_ = json.Unmarshal(content, &failure)
	return &StatusError{
		Code: response.StatusCode, Message: failure.Message,
		Retry: response.StatusCode >= 500 || response.StatusCode == http.StatusTooManyRequests ||
			response.StatusCode == http.StatusForbidden && response.Header.Get("X-RateLimit-Remaining") == "0",
	}
}
