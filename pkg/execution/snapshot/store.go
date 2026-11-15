// Package snapshot keeps a copy of a session's directory in S3-compatible
// object storage, so that a session whose machine or pod is lost can go on
// on another. The copy is one object per session, a tar stream compressed
// with zstd. It is written after each turn and when the worker shuts down,
// and read when a worker starts for a session whose directory is not there.
package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Config says where snapshots are kept. The zero value keeps none.
//
// Credentials are not part of it. They come from the standard AWS
// environment and credential chain: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
// and AWS_SESSION_TOKEN, a shared credentials file (AWS_SHARED_CREDENTIALS_FILE
// and AWS_PROFILE), a web identity token (AWS_WEB_IDENTITY_TOKEN_FILE) or the
// instance or container role. They are read again every few minutes, so a
// credential file that something else rewrites stays current.
type Config struct {
	// Endpoint is the URL of the store, such as http://seaweedfs-s3.seaweedfs:8333.
	// Empty is Amazon S3. A URL without a scheme is https.
	Endpoint string `json:"endpoint,omitempty"`
	// Region is the bucket's region. Empty lets the client find it, which
	// is "us-east-1" for most stores that are not Amazon's.
	Region string `json:"region,omitempty"`
	// Bucket holds the snapshots. It must exist. A zero Bucket turns
	// snapshots off.
	Bucket string `json:"bucket,omitempty"`
	// Prefix is put in front of every key: <Prefix>/<session>/workspace.tar.zst.
	Prefix string `json:"prefix,omitempty"`
	// PathStyle puts the bucket in the path of a request, not in the host
	// name. Stores that are not Amazon's mostly need it.
	PathStyle bool `json:"path_style,omitempty"`
	// Exclude lists what a snapshot leaves out, for what can be rebuilt. A
	// pattern with no slash matches the name of a file or directory at any
	// depth ("node_modules"). One with a slash matches the path from the
	// session's directory ("work/repo/target"). The syntax is path.Match's.
	Exclude []string `json:"exclude,omitempty"`
}

// DefaultExclude is what the commands leave out unless told otherwise:
// dependency and build caches that a build brings back.
var DefaultExclude = []string{"node_modules", "go-build"}

// Enabled reports whether the configuration keeps snapshots.
func (c Config) Enabled() bool { return c.Bucket != "" }

// Validate checks the configuration without contacting the store.
func (c Config) Validate() error {
	if !c.Enabled() {
		if c.Endpoint != "" || c.Region != "" || c.Prefix != "" || c.PathStyle {
			return errors.New("snapshots: a bucket is required")
		}
		return nil
	}
	if _, _, err := c.endpoint(); err != nil {
		return err
	}
	return nil
}

// endpoint splits the endpoint URL into the host and whether it is TLS.
func (c Config) endpoint() (host string, secure bool, err error) {
	if c.Endpoint == "" {
		return "s3.amazonaws.com", true, nil
	}
	raw := c.Endpoint
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return "", false, fmt.Errorf("snapshots: endpoint %q is not a URL", c.Endpoint)
	}
	if strings.Trim(parsed.Path, "/") != "" {
		return "", false, fmt.Errorf("snapshots: endpoint %q has a path, and the client takes only a host", c.Endpoint)
	}
	switch parsed.Scheme {
	case "http":
		return parsed.Host, false, nil
	case "https":
		return parsed.Host, true, nil
	}
	return "", false, fmt.Errorf("snapshots: endpoint %q: the scheme must be http or https", c.Endpoint)
}

const (
	// ObjectName is the name of a session's snapshot under its prefix.
	ObjectName = "workspace.tar.zst"
	// metaFingerprint is the user metadata that holds the fingerprint of
	// the tree that the object was written from.
	metaFingerprint = "p7-fingerprint"
	// partSize is the size of one part of the multipart upload, and the
	// most memory an upload holds. With it a snapshot may be 160 GB.
	partSize = 16 << 20
	// maxCopy is the largest object one copy request takes. Above it the
	// copy is made in parts.
	maxCopy = 5 << 30
	// credentialLifetime is how long retrieved credentials are used before
	// the sources are read again.
	credentialLifetime = 5 * time.Minute
)

// Store reads and writes snapshots.
type Store struct {
	config Config
	client *minio.Client
	log    *slog.Logger

	mu    sync.Mutex
	locks map[string]*sync.Mutex
	// last is the fingerprint of the tree that each session's snapshot
	// was written from, or restored into.
	last map[string]string
}

// New returns a store. It makes no request.
func New(config Config, logger *slog.Logger) (*Store, error) {
	if !config.Enabled() {
		return nil, errors.New("snapshots: a bucket is required")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	host, secure, _ := config.endpoint()
	lookup := minio.BucketLookupAuto
	if config.PathStyle {
		lookup = minio.BucketLookupPath
	}
	client, err := minio.New(host, &minio.Options{
		Creds:        credentials.New(&chainProvider{aws: host == "s3.amazonaws.com" || strings.HasSuffix(host, ".amazonaws.com")}),
		Secure:       secure,
		Region:       config.Region,
		BucketLookup: lookup,
	})
	if err != nil {
		return nil, fmt.Errorf("snapshots: %w", err)
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	config.Prefix = strings.Trim(config.Prefix, "/")
	return &Store{config: config, client: client, log: logger, locks: map[string]*sync.Mutex{}, last: map[string]string{}}, nil
}

// chainProvider is the AWS credential chain, read again every few minutes.
// The client's own chain reads each source once and keeps what it found.
type chainProvider struct {
	credentials.Expiry
	// aws is true for Amazon's endpoint, the only one that has instance and
	// container roles to ask.
	aws bool
}

func (p *chainProvider) RetrieveWithCredContext(cc *credentials.CredContext) (credentials.Value, error) {
	sources := []credentials.Provider{
		&credentials.EnvAWS{},
		&credentials.FileAWSCredentials{},
	}
	if p.aws {
		sources = append(sources, &credentials.IAM{})
	}
	value, err := (&credentials.Chain{Providers: sources}).RetrieveWithCredContext(cc)
	if err == nil {
		p.SetExpiration(time.Now().Add(credentialLifetime), 0)
	}
	return value, err
}

func (p *chainProvider) Retrieve() (credentials.Value, error) {
	return p.RetrieveWithCredContext(&credentials.CredContext{})
}

func (s *Store) key(session string) string {
	return path.Join(s.config.Prefix, session, ObjectName)
}

func (s *Store) tempKey(session string) string { return s.key(session) + ".tmp" }

// lock serializes the operations on one session.
func (s *Store) lock(session string) func() {
	s.mu.Lock()
	l := s.locks[session]
	if l == nil {
		l = &sync.Mutex{}
		s.locks[session] = l
	}
	s.mu.Unlock()
	l.Lock()
	return l.Unlock
}

func (s *Store) setLast(session, fingerprint string) {
	s.mu.Lock()
	s.last[session] = fingerprint
	s.mu.Unlock()
}

func (s *Store) getLast(session string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fingerprint, ok := s.last[session]
	return fingerprint, ok
}

func notFound(err error) bool {
	if err == nil {
		return false
	}
	response := minio.ToErrorResponse(err)
	return response.Code == "NoSuchKey" || response.Code == "NotFound" || response.StatusCode == http.StatusNotFound
}

// Result says what Save did.
type Result struct {
	// Uploaded is false when the tree was as the last snapshot left it.
	Uploaded bool
	// Bytes is the size of the compressed object that was written.
	Bytes int64
	// Skipped lists what was left out because it is not a file, a directory
	// or a link that stays inside the tree.
	Skipped []string
}

// Save writes a snapshot of the directory root as session's, unless it is
// as the last snapshot left it. The tree is written to a temporary object
// that is then copied over the snapshot, so an upload that fails leaves the
// snapshot that was there. exclude is added to the configuration's list.
func (s *Store) Save(ctx context.Context, session, root string, exclude ...string) (Result, error) {
	var result Result
	defer s.lock(session)()
	t, err := walk(root, append(append([]string(nil), s.config.Exclude...), exclude...))
	if err != nil {
		return result, fmt.Errorf("snapshot of session %s: %w", session, err)
	}
	result.Skipped = t.skipped
	last, known := s.getLast(session)
	if !known {
		// A worker that did not write the snapshot may have: the object says
		// what tree it was made from.
		info, err := s.client.StatObject(ctx, s.config.Bucket, s.key(session), minio.StatObjectOptions{})
		switch {
		case err == nil:
			last = info.UserMetadata[metaFingerprintHeader]
		case !notFound(err):
			return result, fmt.Errorf("snapshot of session %s: look for the earlier snapshot: %w", session, err)
		}
	}
	if last == t.fingerprint {
		s.setLast(session, last)
		return result, nil
	}

	temporary, final := s.tempKey(session), s.key(session)
	pipeReader, pipeWriter := io.Pipe()
	go func() { pipeWriter.CloseWithError(writeArchive(pipeWriter, t)) }()
	options := minio.PutObjectOptions{
		ContentType:  "application/zstd",
		PartSize:     partSize,
		UserMetadata: map[string]string{metaFingerprint: t.fingerprint},
	}
	uploaded, err := s.client.PutObject(ctx, s.config.Bucket, temporary, pipeReader, -1, options)
	// A failed upload stops the writer, which may be blocked on the pipe.
	pipeReader.CloseWithError(err)
	if err != nil {
		return result, fmt.Errorf("snapshot of session %s: upload: %w", session, err)
	}
	if err := s.copy(ctx, temporary, final, uploaded.Size, t.fingerprint); err != nil {
		return result, fmt.Errorf("snapshot of session %s: replace the snapshot: %w", session, err)
	}
	if err := s.client.RemoveObject(ctx, s.config.Bucket, temporary, minio.RemoveObjectOptions{}); err != nil {
		s.log.Warn("remove the temporary snapshot", "session", session, "error", err)
	}
	s.setLast(session, t.fingerprint)
	result.Uploaded, result.Bytes = true, uploaded.Size
	return result, nil
}

// metaFingerprintHeader is how the client names the user metadata that
// StatObject returns: with the header's canonical capitalization.
var metaFingerprintHeader = http.CanonicalHeaderKey(metaFingerprint)

func (s *Store) copy(ctx context.Context, from, to string, size int64, fingerprint string) error {
	destination := minio.CopyDestOptions{
		Bucket: s.config.Bucket, Object: to,
		ReplaceMetadata: true, UserMetadata: map[string]string{metaFingerprint: fingerprint},
	}
	source := minio.CopySrcOptions{Bucket: s.config.Bucket, Object: from}
	var err error
	if size > maxCopy {
		_, err = s.client.ComposeObject(ctx, destination, source)
	} else {
		_, err = s.client.CopyObject(ctx, destination, source)
	}
	return err
}

// Restore reads the snapshot of session into dest, which is replaced. It
// reports false, and does nothing, if there is no snapshot.
//
// The snapshot is extracted into a new directory beside dest. Only when the
// whole stream was read and checked is prepare called on that directory, and
// the directory renamed to dest, so that a snapshot that is cut or damaged
// leaves what was in dest. prepare may be nil.
func (s *Store) Restore(ctx context.Context, session, dest string, prepare func(dir string) error) (bool, error) {
	defer s.lock(session)()
	info, err := s.client.StatObject(ctx, s.config.Bucket, s.key(session), minio.StatObjectOptions{})
	if notFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("restore session %s: %w", session, err)
	}
	object, err := s.client.GetObject(ctx, s.config.Bucket, s.key(session), minio.GetObjectOptions{})
	if err != nil {
		return false, fmt.Errorf("restore session %s: %w", session, err)
	}
	defer object.Close()

	parent := filepath.Dir(dest)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return false, fmt.Errorf("restore session %s: %w", session, err)
	}
	fresh, err := os.MkdirTemp(parent, ".restore-")
	if err != nil {
		return false, fmt.Errorf("restore session %s: %w", session, err)
	}
	defer os.RemoveAll(fresh)
	if err := extract(object, fresh); err != nil {
		return false, fmt.Errorf("restore session %s: %w", session, err)
	}
	if prepare != nil {
		if err := prepare(fresh); err != nil {
			return false, fmt.Errorf("restore session %s: %w", session, err)
		}
	}
	old, err := os.MkdirTemp(parent, ".replaced-")
	if err != nil {
		return false, fmt.Errorf("restore session %s: %w", session, err)
	}
	defer os.RemoveAll(old)
	if err := os.Rename(dest, filepath.Join(old, "dir")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("restore session %s: move aside %s: %w", session, dest, err)
	}
	if err := os.Rename(fresh, dest); err != nil {
		_ = os.Rename(filepath.Join(old, "dir"), dest)
		return false, fmt.Errorf("restore session %s: %w", session, err)
	}
	s.setLast(session, info.UserMetadata[metaFingerprintHeader])
	return true, nil
}

// Delete removes the snapshot of session, and any upload that was left
// unfinished. A snapshot that is not there is not an error.
func (s *Store) Delete(ctx context.Context, session string) error {
	defer s.lock(session)()
	var failures []error
	for _, key := range []string{s.key(session), s.tempKey(session)} {
		if err := s.client.RemoveObject(ctx, s.config.Bucket, key, minio.RemoveObjectOptions{}); err != nil && !notFound(err) {
			failures = append(failures, err)
		}
	}
	if err := errors.Join(failures...); err != nil {
		return fmt.Errorf("delete the snapshot of session %s: %w", session, err)
	}
	s.mu.Lock()
	delete(s.last, session)
	s.mu.Unlock()
	return nil
}
