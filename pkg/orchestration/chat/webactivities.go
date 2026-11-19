package chat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Names of the activities behind the web tools. They are methods of
// WebActivities, which the selfdev worker registers when it is given a
// Parallel placeholder.
const (
	ActivityWebSearch  = "ChatWebSearch"
	ActivityWebExtract = "ChatWebExtract"
)

// The bounds of the web tools, as the parallel-web script had them.
const (
	// MaxExtractURLs is the most pages web_extract reads in one call.
	MaxExtractURLs = 20
	// MaxSearchQueries is the most keyword queries web_search sends.
	MaxSearchQueries = 5
	// SearchChars and ExtractChars are what Parallel is asked to return in
	// all, so that a call does not fill the agent's context.
	SearchChars  = 20000
	ExtractChars = 30000
	// WebResultBytes bounds the text given to the agent, beyond Parallel's
	// own bound.
	WebResultBytes = 40000
)

// DefaultParallelURL is Parallel's API.
const DefaultParallelURL = "https://api.parallel.ai/v1"

// WebConfig configures the web activities.
type WebConfig struct {
	// Placeholder is what the client sends as its x-api-key. PDP swaps it for
	// the Parallel key. Empty offers no web tool.
	Placeholder string
	// URL is Parallel's API. Empty is DefaultParallelURL.
	URL string
	// HTTP sends the requests, through PDP. Nil is http.DefaultClient.
	HTTP *http.Client
}

// WebRequest is one call of a web tool.
type WebRequest struct {
	Chat      string   `json:"chat"`
	Objective string   `json:"objective"`
	Queries   []string `json:"queries,omitempty"`
	URLs      []string `json:"urls,omitempty"`
}

// WebResult is the text given to the agent.
type WebResult struct {
	Text string `json:"text"`
}

// WebActivities are the Parallel calls of the chat's web tools.
type WebActivities struct {
	config WebConfig
}

// NewWebActivities makes the activities.
func NewWebActivities(config WebConfig) *WebActivities {
	if config.URL == "" {
		config.URL = DefaultParallelURL
	}
	if config.HTTP == nil {
		config.HTTP = http.DefaultClient
	}
	return &WebActivities{config: config}
}

type webPage struct {
	URL         string   `json:"url"`
	Title       string   `json:"title"`
	PublishDate *string  `json:"publish_date"`
	Excerpts    []string `json:"excerpts"`
}

type webResponse struct {
	Results  []webPage `json:"results"`
	Errors   []json.RawMessage
	Warnings []json.RawMessage
}

// ChatWebSearch searches the web for Objective with keyword Queries. The
// objective stands in when no query is given, since Parallel needs one.
func (a *WebActivities) ChatWebSearch(ctx context.Context, r WebRequest) (WebResult, error) {
	objective := strings.TrimSpace(r.Objective)
	if objective == "" {
		return WebResult{}, refuse("a search needs an objective")
	}
	var queries []string
	for _, query := range r.Queries {
		if query = strings.TrimSpace(query); query != "" {
			queries = append(queries, query)
		}
	}
	if len(queries) > MaxSearchQueries {
		return WebResult{}, refuse("at most %d queries, got %d", MaxSearchQueries, len(queries))
	}
	if len(queries) == 0 {
		queries = []string{objective}
	}
	body := map[string]any{"objective": objective, "search_queries": queries, "mode": "basic", "max_chars_total": SearchChars}
	var response webResponse
	if err := a.post(ctx, "search", body, &response); err != nil {
		return WebResult{}, err
	}
	return WebResult{Text: render(response)}, nil
}

// ChatWebExtract reads up to MaxExtractURLs pages, with excerpts that
// Objective focuses.
func (a *WebActivities) ChatWebExtract(ctx context.Context, r WebRequest) (WebResult, error) {
	objective := strings.TrimSpace(r.Objective)
	if objective == "" {
		return WebResult{}, refuse("an extract needs an objective")
	}
	if len(r.URLs) == 0 || len(r.URLs) > MaxExtractURLs {
		return WebResult{}, refuse("give between 1 and %d URLs, got %d", MaxExtractURLs, len(r.URLs))
	}
	for _, address := range r.URLs {
		parsed, err := url.Parse(address)
		if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" {
			return WebResult{}, refuse("%q is not an http or https URL", address)
		}
	}
	body := map[string]any{"objective": objective, "urls": r.URLs, "max_chars_total": ExtractChars}
	var response webResponse
	if err := a.post(ctx, "extract", body, &response); err != nil {
		return WebResult{}, err
	}
	return WebResult{Text: render(response)}, nil
}

// post sends one request to Parallel. A 4xx answer is final: a 403 is PDP's
// refusal, and trying again does not change it.
func (a *WebActivities) post(ctx context.Context, endpoint string, body, result any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encode the Parallel request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(a.config.URL, "/")+"/"+endpoint, bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("make the Parallel request: %w", err)
	}
	request.Header.Set("x-api-key", a.config.Placeholder)
	request.Header.Set("Content-Type", "application/json")
	response, err := a.config.HTTP.Do(request)
	if err != nil {
		return fmt.Errorf("call Parallel: %w", err)
	}
	defer response.Body.Close()
	content, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	if err != nil {
		return fmt.Errorf("read Parallel's response: %w", err)
	}
	if response.StatusCode >= 300 {
		message := clip(string(content), 500)
		if response.StatusCode < 500 && response.StatusCode != http.StatusTooManyRequests {
			return refuse("Parallel answered %d: %s", response.StatusCode, message)
		}
		return fmt.Errorf("parallel answered %d: %s", response.StatusCode, message)
	}
	if err := json.Unmarshal(content, result); err != nil {
		return fmt.Errorf("decode Parallel's response: %w", err)
	}
	return nil
}

// render writes Parallel's results as text: each page's title, URL, date and
// excerpts, then the errors and warnings.
func render(response webResponse) string {
	var out strings.Builder
	if len(response.Results) == 0 {
		out.WriteString("No results.\n")
	}
	for i, page := range response.Results {
		fmt.Fprintf(&out, "[%d] %s\n%s\n", i+1, page.Title, page.URL)
		if page.PublishDate != nil && *page.PublishDate != "" {
			fmt.Fprintf(&out, "Published: %s\n", *page.PublishDate)
		}
		for _, excerpt := range page.Excerpts {
			out.WriteString(strings.TrimSpace(excerpt) + "\n")
		}
		out.WriteString("\n")
	}
	for _, failure := range response.Errors {
		fmt.Fprintf(&out, "Error: %s\n", failure)
	}
	for _, warning := range response.Warnings {
		fmt.Fprintf(&out, "Warning: %s\n", warning)
	}
	return clip(out.String(), WebResultBytes)
}
