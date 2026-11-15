// Package snapshottest is an in-process S3 server for tests of code that
// keeps snapshots. It is gofakes3 with a memory backend and one bucket.
package snapshottest

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// Bucket is the one bucket of the server.
const Bucket = "snapshots"

// Server is a running fake S3 server.
type Server struct {
	// URL is its endpoint, plain HTTP.
	URL string
	// FailCopy, when set, makes every copy request fail, which is what a
	// worker that died between its upload and its copy leaves behind.
	FailCopy atomic.Bool
	// WriteDelay, in nanoseconds, is how long each request that writes
	// waits before it is served, to make an upload slow.
	WriteDelay atomic.Int64
	// ReadDelay, in nanoseconds, is how long each request that reads waits
	// before it is served, to make a restore slow.
	ReadDelay atomic.Int64
	// Requests counts the requests the server got.
	Requests atomic.Int64
}

// Start runs a server until the test ends.
func Start(t testing.TB) *Server {
	t.Helper()
	backend := s3mem.New()
	if err := backend.CreateBucket(Bucket); err != nil {
		t.Fatal(err)
	}
	fake := &Server{}
	s3 := gofakes3.New(backend).Server()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fake.Requests.Add(1)
		if delay := fake.ReadDelay.Load(); delay > 0 && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
			time.Sleep(time.Duration(delay))
		}
		if delay := fake.WriteDelay.Load(); delay > 0 && r.Method != http.MethodGet && r.Method != http.MethodHead {
			time.Sleep(time.Duration(delay))
		}
		if fake.FailCopy.Load() && r.Header.Get("X-Amz-Copy-Source") != "" {
			http.Error(w, "the store went away", http.StatusInternalServerError)
			return
		}
		if r.Header.Get("X-Amz-Content-Sha256") == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" && r.URL.Query().Has("partNumber") {
			// Over plain HTTP the client sends a body in signed chunks, as
			// MinIO and SeaweedFS accept. gofakes3 decodes them for a whole
			// object but would store those of a part as they are.
			decoded, size, err := decodeChunks(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			r.Body, r.ContentLength = io.NopCloser(bytes.NewReader(decoded)), size
			r.Header.Set("Content-Length", strconv.FormatInt(size, 10))
			r.Header.Del("X-Amz-Decoded-Content-Length")
			r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
		}
		s3.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	fake.URL = server.URL
	return fake
}

// decodeChunks reads the aws-chunked encoding: "<hex size>;chunk-signature=<sig>\r\n<data>\r\n"
// for each chunk, ending with a chunk of size 0. It does not check signatures.
func decodeChunks(r io.Reader) ([]byte, int64, error) {
	reader := bufio.NewReader(r)
	var out bytes.Buffer
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, 0, err
		}
		sizeText, _, _ := strings.Cut(strings.TrimSpace(line), ";")
		size, err := strconv.ParseInt(sizeText, 16, 64)
		if err != nil {
			return nil, 0, err
		}
		if size == 0 {
			return out.Bytes(), int64(out.Len()), nil
		}
		if _, err := io.CopyN(&out, reader, size); err != nil {
			return nil, 0, err
		}
		if _, err := reader.Discard(2); err != nil {
			return nil, 0, err
		}
	}
}
