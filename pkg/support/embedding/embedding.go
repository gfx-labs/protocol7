// Package embedding is a client for an OpenAI-compatible /v1/embeddings
// server, such as llama.cpp's llama-server running Qwen3-Embedding, and the
// vector helpers that go with it.
//
// Qwen3-Embedding is asymmetric: a query is written with a task instruction
// and a document is embedded as it is. Client.EmbedQuery and
// Client.EmbedDocuments do that, and Cosine compares the results.
//
//	client, err := embedding.New(embedding.Config{
//		URL:     "http://embeddings.embeddings.svc:8080",
//		Model:   "qwen3-embedding-0.6b",
//		Timeout: 5 * time.Second,
//	})
//	if err != nil { ... }
//	query, err := client.EmbedQuery(ctx, "Find the note about this failure", text)
//
// A command takes its settings from a --config file by embedding Settings in
// its kong struct with the prefix "embedding-".
package embedding

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Settings are a command's flags for the embedding server. Embed them with
// `embed:"" prefix:"embedding-"`, so that the settings file names them
// embedding-url, embedding-model and embedding-timeout.
type Settings struct {
	URL     string        `name:"url" help:"Base URL of the OpenAI-compatible embeddings server, such as http://embeddings.embeddings.svc:8080. Empty turns embeddings off."`
	Model   string        `name:"model" help:"Model name sent with each request."`
	Timeout time.Duration `name:"timeout" default:"5s" help:"Bound on one request to the embeddings server."`
}

// Enabled says whether the settings name a server.
func (s Settings) Enabled() bool { return s.URL != "" }

// Config is the settings as New takes them.
func (s Settings) Config() Config {
	return Config{URL: s.URL, Model: s.Model, Timeout: s.Timeout}
}

// Config says which server to ask and how long to wait.
type Config struct {
	// URL is the server's base URL. The client posts to <URL>/v1/embeddings.
	URL string
	// Model is the model name sent with each request.
	Model string
	// Timeout bounds one request, including reading its response.
	Timeout time.Duration
}

// maxResponseBytes bounds what the client reads of a response.
const maxResponseBytes = 64 << 20

// Client asks one embeddings server. It is safe for concurrent use.
type Client struct {
	endpoint string
	model    string
	http     *http.Client
}

// New checks the config and returns a client.
func New(config Config) (*Client, error) {
	if config.Model == "" {
		return nil, errors.New("embedding: no model name")
	}
	if config.Timeout <= 0 {
		return nil, errors.New("embedding: the timeout must be positive")
	}
	base, err := url.Parse(config.URL)
	if err != nil {
		return nil, fmt.Errorf("embedding: parse the server URL: %w", err)
	}
	if (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, fmt.Errorf("embedding: the server URL %q is not an http or https URL with a host", config.URL)
	}
	endpoint := base.JoinPath("v1", "embeddings")
	return &Client{endpoint: endpoint.String(), model: config.Model, http: &http.Client{Timeout: config.Timeout}}, nil
}

// Model is the model name the client sends.
func (c *Client) Model() string { return c.model }

type request struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type response struct {
	Data []struct {
		Index     int       `json:"index"`
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

// Embed returns one vector per text, in the order of the texts. It sends the
// texts as they are. Use EmbedQuery for a query and EmbedDocuments for the
// texts to search.
func (c *Client) Embed(ctx context.Context, texts ...string) ([]Vector, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	body, err := json.Marshal(request{Model: c.model, Input: texts})
	if err != nil {
		return nil, fmt.Errorf("embedding: encode the request: %w", err)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("embedding: build the request: %w", err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpResponse, err := c.http.Do(httpRequest)
	if err != nil {
		return nil, fmt.Errorf("embedding: ask %s: %w", c.endpoint, err)
	}
	defer httpResponse.Body.Close()
	data, err := io.ReadAll(io.LimitReader(httpResponse.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("embedding: read the response of %s: %w", c.endpoint, err)
	}
	if httpResponse.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embedding: %s answered %s: %s", c.endpoint, httpResponse.Status, snippet(data))
	}
	var parsed response
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("embedding: decode the response of %s: %w", c.endpoint, err)
	}
	if len(parsed.Data) != len(texts) {
		return nil, fmt.Errorf("embedding: asked for %d vectors and got %d", len(texts), len(parsed.Data))
	}
	vectors := make([]Vector, len(texts))
	for _, item := range parsed.Data {
		if item.Index < 0 || item.Index >= len(texts) || vectors[item.Index] != nil {
			return nil, fmt.Errorf("embedding: the response has an unexpected index %d", item.Index)
		}
		if len(item.Embedding) == 0 {
			return nil, fmt.Errorf("embedding: the response has an empty vector at index %d", item.Index)
		}
		vectors[item.Index] = Vector(item.Embedding)
	}
	return vectors, nil
}

// snippet is the start of an error body, for an error message.
func snippet(data []byte) string {
	text := strings.Join(strings.Fields(string(data)), " ")
	if len(text) > 200 {
		text = text[:200] + "..."
	}
	return text
}

// FormatQuery writes a query in the form Qwen3-Embedding expects: the task
// the query serves, then the query.
func FormatQuery(task, query string) string {
	return "Instruct: " + task + "\nQuery:" + query
}

// EmbedQuery embeds a query with the task instruction.
func (c *Client) EmbedQuery(ctx context.Context, task, query string) (Vector, error) {
	vectors, err := c.Embed(ctx, FormatQuery(task, query))
	if err != nil {
		return nil, err
	}
	return vectors[0], nil
}

// EmbedDocuments embeds the texts to be searched, with no instruction.
func (c *Client) EmbedDocuments(ctx context.Context, documents ...string) ([]Vector, error) {
	return c.Embed(ctx, documents...)
}

// Vector is an embedding. As JSON it is a string, the little-endian float32
// values in base64, which is a third the size of a list of numbers.
type Vector []float32

// MarshalText implements encoding.TextMarshaler.
func (v Vector) MarshalText() ([]byte, error) {
	raw := make([]byte, 4*len(v))
	for i, value := range v {
		binary.LittleEndian.PutUint32(raw[4*i:], math.Float32bits(value))
	}
	text := make([]byte, base64.StdEncoding.EncodedLen(len(raw)))
	base64.StdEncoding.Encode(text, raw)
	return text, nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (v *Vector) UnmarshalText(text []byte) error {
	raw := make([]byte, base64.StdEncoding.DecodedLen(len(text)))
	n, err := base64.StdEncoding.Decode(raw, text)
	if err != nil {
		return fmt.Errorf("embedding: decode a vector: %w", err)
	}
	if n%4 != 0 {
		return fmt.Errorf("embedding: a vector of %d bytes is not float32 values", n)
	}
	out := make(Vector, n/4)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[4*i:]))
	}
	*v = out
	return nil
}

// Cosine is the cosine similarity of two vectors, between -1 and 1. It
// refuses vectors of different lengths, empty vectors and vectors with no
// length, which have no direction.
func Cosine(a, b Vector) (float64, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("embedding: cosine of vectors of %d and %d dimensions", len(a), len(b))
	}
	if len(a) == 0 {
		return 0, errors.New("embedding: cosine of empty vectors")
	}
	var dot, normA, normB float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		normA += x * x
		normB += y * y
	}
	if normA == 0 || normB == 0 {
		return 0, errors.New("embedding: cosine of a zero vector")
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB)), nil
}
