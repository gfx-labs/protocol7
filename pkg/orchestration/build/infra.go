package build

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/exporter/containerimage/exptypes"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/session/auth"
	"github.com/spiffe/go-spiffe/v2/svid/jwtsvid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"github.com/tonistiigi/fsutil"
	"google.golang.org/grpc"
)

// JWTAudience is the audience zot accepts JWT-SVIDs for.
const JWTAudience = "zot"

// RegistryUser is the user name sent with the JWT-SVID. zot ignores it and
// takes the identity from the token's sub claim.
const RegistryUser = "zot"

// Credential returns the builder's current registry password: a JWT-SVID with
// audience zot from the Workload API.
type Credential func(ctx context.Context) (string, error)

// WorkloadCredential fetches a JWT-SVID for each call. SVIDs live an hour and
// a build takes minutes, so nothing is cached here.
func WorkloadCredential(socket string) Credential {
	return func(ctx context.Context) (string, error) {
		var options []workloadapi.ClientOption
		if socket != "" {
			options = append(options, workloadapi.WithAddr(socket))
		}
		svid, err := workloadapi.FetchJWTSVID(ctx, jwtsvid.Params{Audience: JWTAudience}, options...)
		if err != nil {
			return "", fmt.Errorf("fetch a JWT-SVID for zot: %w", err)
		}
		return svid.Marshal(), nil
	}
}

// TrustingTransport returns a transport that trusts caFile on top of the
// system roots and uses no proxy, for zot and buildkitd inside the cluster.
func TrustingTransport(caFile string) (*http.Transport, error) {
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%s holds no certificate", caFile)
		}
	}
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.Proxy = nil
	base.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	return base, nil
}

// Zot is Registry on zot with the builder's JWT-SVID.
type Zot struct {
	Host       string
	Credential Credential
	Transport  http.RoundTripper
}

func (z Zot) options(ctx context.Context) ([]remote.Option, error) {
	password, err := z.Credential(ctx)
	if err != nil {
		return nil, err
	}
	return []remote.Option{
		remote.WithContext(ctx),
		remote.WithTransport(z.Transport),
		remote.WithAuth(&authn.Basic{Username: RegistryUser, Password: password}),
	}, nil
}

// Digest returns the digest of package:tag, or "" when zot has no such tag.
func (z Zot) Digest(ctx context.Context, pkg, tag string) (string, error) {
	ref, err := name.NewTag(z.Host+"/"+pkg+":"+tag, name.StrictValidation)
	if err != nil {
		return "", err
	}
	options, err := z.options(ctx)
	if err != nil {
		return "", err
	}
	descriptor, err := remote.Head(ref, options...)
	var status *transport.Error
	if errors.As(err, &status) && status.StatusCode == http.StatusNotFound {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return descriptor.Digest.String(), nil
}

// Tag points package:tag at digest.
func (z Zot) Tag(ctx context.Context, pkg, digest, tag string) error {
	ref, err := name.NewDigest(z.Host + "/" + pkg + "@" + digest)
	if err != nil {
		return err
	}
	options, err := z.options(ctx)
	if err != nil {
		return err
	}
	described, err := remote.Get(ref, options...)
	if err != nil {
		return err
	}
	target, err := name.NewTag(z.Host+"/"+pkg+":"+tag, name.StrictValidation)
	if err != nil {
		return err
	}
	return remote.Tag(target, described, options...)
}

// BuildKit is Solver on a buildkitd. The registry credential reaches
// buildkitd only through the session, for the registry host alone, and never
// sits in its files or a RUN step's environment.
type BuildKit struct {
	// Address is tcp://buildkitd.<namespace>.svc:1234.
	Address string
	// RegistryHost is the host the credential is given for.
	RegistryHost string
	Credential   Credential
	// Wake is called before each solve, such as scaling buildkitd to one.
	Wake func(ctx context.Context) error
}

// Solve builds with the dockerfile frontend and pushes by the image exporter.
func (b BuildKit) Solve(ctx context.Context, job SolveJob, progress func(string)) (string, error) {
	if b.Wake != nil {
		if err := b.Wake(ctx); err != nil {
			return "", err
		}
	}
	bk, err := client.New(ctx, b.Address)
	if err != nil {
		return "", fmt.Errorf("connect to buildkitd at %s: %w", b.Address, err)
	}
	defer bk.Close()
	source, err := fsutil.NewFS(job.Context)
	if err != nil {
		return "", err
	}
	attrs := map[string]string{"filename": job.Dockerfile}
	if job.Target != "" {
		attrs["target"] = job.Target
	}
	options := client.SolveOpt{
		Frontend:      "dockerfile.v0",
		FrontendAttrs: attrs,
		LocalMounts:   map[string]fsutil.FS{"context": source, "dockerfile": source},
		Exports: []client.ExportEntry{{
			Type:  client.ExporterImage,
			Attrs: map[string]string{"name": strings.Join(job.Refs, ","), "push": "true", "oci-mediatypes": "true"},
		}},
		Session: []session.Attachable{&sessionAuth{host: b.RegistryHost, credential: b.Credential}},
	}
	status := make(chan *client.SolveStatus)
	tail := newLogTail(40)
	done := make(chan struct{})
	go func() {
		defer close(done)
		last := time.Time{}
		for update := range status {
			for _, log := range update.Logs {
				tail.write(log.Data)
			}
			for _, vertex := range update.Vertexes {
				if time.Since(last) > 10*time.Second {
					progress(vertex.Name)
					last = time.Now()
				}
			}
		}
	}()
	response, err := bk.Solve(ctx, nil, options, status)
	<-done
	if err != nil {
		if ctx.Err() != nil || isUnavailable(err) {
			return "", err
		}
		return "", &StepError{Log: tail.String(), Err: err}
	}
	digest := response.ExporterResponse[exptypes.ExporterImageDigestKey]
	if digest == "" {
		return "", errors.New("buildkitd returned no image digest")
	}
	return digest, nil
}

func isUnavailable(err error) bool {
	text := err.Error()
	return strings.Contains(text, "Unavailable") || strings.Contains(text, "connection refused") || strings.Contains(text, "no such host")
}

// sessionAuth answers buildkitd's credential requests for the registry host
// with the JWT-SVID. zot exchanges it at its token endpoint.
type sessionAuth struct {
	auth.UnimplementedAuthServer
	host       string
	credential Credential
}

func (s *sessionAuth) Register(server *grpc.Server) { auth.RegisterAuthServer(server, s) }

func (s *sessionAuth) Credentials(ctx context.Context, request *auth.CredentialsRequest) (*auth.CredentialsResponse, error) {
	if request.Host != s.host {
		return &auth.CredentialsResponse{}, nil
	}
	password, err := s.credential(ctx)
	if err != nil {
		return nil, err
	}
	return &auth.CredentialsResponse{Username: RegistryUser, Secret: password}, nil
}

type logTail struct {
	mu    sync.Mutex
	lines []string
	keep  int
}

func newLogTail(keep int) *logTail { return &logTail{keep: keep} }

func (t *logTail) write(data []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, line := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
		t.lines = append(t.lines, line)
	}
	if len(t.lines) > t.keep {
		t.lines = t.lines[len(t.lines)-t.keep:]
	}
}

func (t *logTail) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.Join(t.lines, "\n")
}

// Scaler scales the tenant's buildkitd StatefulSet through the Kubernetes API
// with the builder's projected token. The builder's Role allows only this.
type Scaler struct {
	APIServer string
	Namespace string
	Name      string
	TokenFile string
	HTTP      *http.Client
	// IdleAfter is how long buildkitd stays up after the last solve.
	IdleAfter time.Duration

	mu       sync.Mutex
	lastUsed time.Time
}

// NewInClusterScaler reads the service account CA and the API server address.
func NewInClusterScaler(namespaceAndName string, idleAfter time.Duration) (*Scaler, error) {
	namespace, statefulSet, ok := strings.Cut(namespaceAndName, "/")
	if !ok || namespace == "" || statefulSet == "" {
		return nil, fmt.Errorf("buildkit StatefulSet %q is not namespace/name", namespaceAndName)
	}
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("not in a cluster: KUBERNETES_SERVICE_HOST is unset")
	}
	const directory = "/var/run/secrets/kubernetes.io/serviceaccount"
	httpTransport, err := TrustingTransport(directory + "/ca.crt")
	if err != nil {
		return nil, err
	}
	return &Scaler{
		APIServer: "https://" + host + ":" + port, Namespace: namespace, Name: statefulSet,
		TokenFile: directory + "/token", HTTP: &http.Client{Transport: httpTransport, Timeout: 30 * time.Second}, IdleAfter: idleAfter,
	}, nil
}

// Wake scales to one if needed and waits until a replica is ready.
func (s *Scaler) Wake(ctx context.Context) error {
	s.mu.Lock()
	s.lastUsed = time.Now()
	s.mu.Unlock()
	if err := s.scale(ctx, 1); err != nil {
		return err
	}
	for {
		ready, err := s.ready(ctx)
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// Run scales buildkitd to zero once it has been idle for IdleAfter. busy says
// whether a solve is running.
func (s *Scaler) Run(ctx context.Context, busy func() bool) error {
	// buildkitd may already be up when the builder starts, so the idle time
	// counts from now and an unused buildkitd still scales to zero.
	s.mu.Lock()
	if s.lastUsed.IsZero() {
		s.lastUsed = time.Now()
	}
	s.mu.Unlock()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		s.mu.Lock()
		idle := !s.lastUsed.IsZero() && time.Since(s.lastUsed) > s.IdleAfter
		s.mu.Unlock()
		if idle && !busy() {
			if err := s.scale(ctx, 0); err != nil {
				continue
			}
			s.mu.Lock()
			s.lastUsed = time.Time{}
			s.mu.Unlock()
		}
	}
}

func (s *Scaler) path() string {
	return "/apis/apps/v1/namespaces/" + s.Namespace + "/statefulsets/" + s.Name
}

func (s *Scaler) scale(ctx context.Context, replicas int) error {
	body, _ := json.Marshal(map[string]any{"spec": map[string]any{"replicas": replicas}})
	_, err := s.call(ctx, http.MethodPatch, s.path()+"/scale", body)
	return err
}

func (s *Scaler) ready(ctx context.Context) (bool, error) {
	content, err := s.call(ctx, http.MethodGet, s.path(), nil)
	if err != nil {
		return false, err
	}
	var state struct {
		Status struct {
			ReadyReplicas int `json:"readyReplicas"`
		} `json:"status"`
	}
	if err := json.Unmarshal(content, &state); err != nil {
		return false, err
	}
	return state.Status.ReadyReplicas > 0, nil
}

func (s *Scaler) call(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	token, err := os.ReadFile(s.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("read the service account token: %w", err)
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, s.APIServer+path, reader)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/merge-patch+json")
	}
	response, err := s.HTTP.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	content, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s: %s: %s", method, path, response.Status, strings.TrimSpace(string(content)))
	}
	return content, nil
}
