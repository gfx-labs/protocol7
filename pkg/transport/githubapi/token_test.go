package githubapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gfx-labs/protocol7/pkg/transport/githubapi"
)

func TestParseTokenSource(t *testing.T) {
	if got := githubapi.ParseTokenSource("/run/github/token"); got != githubapi.FileToken("/run/github/token") {
		t.Errorf("a path is a file, got %#v", got)
	}
	if got := githubapi.ParseTokenSource("placeholder:pdp-proxied-github-chat"); got != githubapi.PlaceholderToken("pdp-proxied-github-chat") {
		t.Errorf("a placeholder spec is a placeholder, got %#v", got)
	}
	if _, err := githubapi.PlaceholderToken(" ").Token(); err == nil {
		t.Error("an empty placeholder must be refused")
	}
}

func TestFileTokenIsReadOnEveryCall(t *testing.T) {
	file := filepath.Join(t.TempDir(), "token")
	source := githubapi.FileToken(file)
	if _, err := source.Token(); err == nil {
		t.Fatal("a missing file must be an error")
	}
	for _, want := range []string{"one", "two"} {
		if err := os.WriteFile(file, []byte(want+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if got, err := source.Token(); err != nil || got != want {
			t.Errorf("Token() = %q, %v, want %q", got, err, want)
		}
	}
}

// A placeholder client holds no token, so what it sends is the placeholder, and
// it sends it through the proxy of the environment: that proxy is PDP in a
// session pod. The target's name is not a loopback one, which Go never proxies.
func TestPlaceholderClientSendsThePlaceholderThroughTheProxy(t *testing.T) {
	var seen *http.Request
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer proxy.Close()
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("http_proxy", proxy.URL)
	t.Setenv("NO_PROXY", "localhost,127.0.0.1,10.43.0.1,temporal-frontend,.svc,.cluster.local")
	t.Setenv("no_proxy", os.Getenv("NO_PROXY"))

	client := githubapi.New("placeholder:pdp-proxied-github-chat", "http://api.github.test")
	var result struct{ OK bool }
	if err := client.Do(context.Background(), http.MethodGet, "/repos/gfx-labs/protocol7", nil, &result); err != nil {
		t.Fatal(err)
	}
	if !result.OK || seen == nil {
		t.Fatalf("the call did not reach the proxy: %+v, %v", result, seen)
	}
	if got := seen.Header.Get("Authorization"); got != "Bearer pdp-proxied-github-chat" {
		t.Errorf("Authorization = %q, want the placeholder", got)
	}
	if seen.Host != "api.github.test" {
		t.Errorf("the proxy was asked for %q, want api.github.test", seen.Host)
	}

	// The file client of the selfdev Deployment is unchanged.
	file := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(file, []byte("real\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := githubapi.New(file, "http://api.github.test").Do(context.Background(), http.MethodGet, "/x", nil, &result); err != nil {
		t.Fatal(err)
	}
	if got := seen.Header.Get("Authorization"); got != "Bearer real" {
		t.Errorf("Authorization = %q, want the file's token", got)
	}
}

// A client that goes through PDP sends its placeholder only on its route,
// never before the route exists.
func TestThroughPDPSendsThePlaceholderOnlyOnItsRoute(t *testing.T) {
	var seen atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Store(r.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	client := githubapi.NewThroughPDP("pdp-proxied-github-navi", server.URL)

	err := client.Do(context.Background(), http.MethodGet, "/x", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "not open yet") {
		t.Fatalf("a call before the route is open must fail, got %v", err)
	}
	if seen.Load() != nil {
		t.Fatalf("a request left before the route was open: %v", seen.Load())
	}
	client.UseHTTP(server.Client())
	if err := client.Do(context.Background(), http.MethodGet, "/x", nil, nil); err != nil {
		t.Fatal(err)
	}
	if seen.Load() != "Bearer pdp-proxied-github-navi" {
		t.Fatalf("got %v", seen.Load())
	}
}
