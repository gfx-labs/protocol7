package embedding_test

import (
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gfx-labs/protocol7/pkg/support/embedding"
)

func newClient(t *testing.T, handler http.HandlerFunc) *embedding.Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := embedding.New(embedding.Config{URL: server.URL + "/", Model: "m", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestQueryAndDocumentsAreSentAsQwen3Expects(t *testing.T) {
	var path string
	var got struct {
		Model string
		Input []string
	}
	client := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &got); err != nil {
			t.Error(err)
		}
		// A server may answer out of order: the client puts the vectors back.
		io.WriteString(w, `{"data":[{"index":1,"embedding":[0,1]},{"index":0,"embedding":[1,0]}]}`)
	})
	vectors, err := client.EmbedDocuments(t.Context(), "first", "second")
	if err != nil {
		t.Fatal(err)
	}
	if path != "/v1/embeddings" || got.Model != "m" || strings.Join(got.Input, "|") != "first|second" {
		t.Errorf("sent %s %+v", path, got)
	}
	if vectors[0][0] != 1 || vectors[1][1] != 1 {
		t.Errorf("vectors = %v, want them in the order of the texts", vectors)
	}
	if _, err := client.EmbedQuery(t.Context(), "find it", "what failed"); err == nil {
		t.Fatal("two vectors for one query must be an error")
	}
	if got.Input[0] != "Instruct: find it\nQuery:what failed" {
		t.Errorf("query = %q", got.Input[0])
	}
}

func TestServerFailureIsAnError(t *testing.T) {
	client := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "model is loading", http.StatusServiceUnavailable)
	})
	_, err := client.Embed(t.Context(), "text")
	if err == nil || !strings.Contains(err.Error(), "model is loading") {
		t.Fatalf("err = %v, want the server's reason", err)
	}
}

func TestVectorJSONRoundTrip(t *testing.T) {
	in := embedding.Vector{0.25, -1.5, 3}
	data, err := json.Marshal(struct{ V embedding.Vector }{in})
	if err != nil {
		t.Fatal(err)
	}
	var out struct{ V embedding.Vector }
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.V) != 3 || out.V[0] != 0.25 || out.V[1] != -1.5 || out.V[2] != 3 {
		t.Errorf("got %v from %s", out.V, data)
	}
}

func TestCosine(t *testing.T) {
	got, err := embedding.Cosine(embedding.Vector{1, 1}, embedding.Vector{2, 0})
	if err != nil || math.Abs(got-math.Sqrt2/2) > 1e-6 {
		t.Errorf("cosine = %v, %v", got, err)
	}
	for name, pair := range map[string][2]embedding.Vector{
		"length": {{1}, {1, 2}},
		"empty":  {{}, {}},
		"zero":   {{0, 0}, {1, 1}},
	} {
		if _, err := embedding.Cosine(pair[0], pair[1]); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}
