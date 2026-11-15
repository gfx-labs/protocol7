package p7_test

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/proto/spiffe/workload"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"

	"github.com/gfx-labs/protocol7/pkg/p7"
)

// The client's mutual TLS is checked against a TLS gRPC server that serves
// the two calls CheckHealth makes, with SVIDs from a CA made here. The
// client's SVID comes from a Workload API on a unix socket.
func TestTemporalClientSPIFFE(t *testing.T) {
	trustDomain := spiffeid.RequireTrustDomainFromString("example.org")
	authority := newTestAuthority(t, trustDomain)
	clientID := spiffeid.RequireFromPath(trustDomain, "/p7")
	frontendID := spiffeid.RequireFromPath(trustDomain, "/temporal/frontend")
	workloadAPI := startWorkloadAPI(t, authority.issue(t, clientID), authority.bundle)

	t.Run("accepts the named server and presents the workload's SVID", func(t *testing.T) {
		frontend := startTLSFrontend(t, authority.issue(t, frontendID), authority.bundle)
		temporalClient, closeClient, err := p7.NewTemporalClient(
			client.Options{HostPort: frontend.address},
			p7.TemporalSPIFFE{Socket: workloadAPI.address, ServerID: frontendID.String()},
		)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		if _, err := temporalClient.CheckHealth(ctx, &client.CheckHealthRequest{}); err != nil {
			t.Fatalf("health check over mutual TLS: %v", err)
		}
		peerID, version := frontend.lastPeer()
		if peerID != clientID {
			t.Errorf("the server saw client %s, want %s", peerID, clientID)
		}
		if version != tls.VersionTLS13 {
			t.Errorf("TLS version %#x, want TLS 1.3", version)
		}
		if err := closeClient(); err != nil {
			t.Fatal(err)
		}
		// Closing the client also closes its Workload API source.
		select {
		case <-workloadAPI.streamEnded:
		case <-time.After(5 * time.Second):
			t.Error("the Workload API stream is still open after the client was closed")
		}
	})

	t.Run("refuses a server with another SPIFFE ID", func(t *testing.T) {
		frontend := startTLSFrontend(t, authority.issue(t, spiffeid.RequireFromPath(trustDomain, "/other")), authority.bundle)
		temporalClient, closeClient, err := p7.NewTemporalClient(
			client.Options{HostPort: frontend.address},
			p7.TemporalSPIFFE{Socket: workloadAPI.address, ServerID: frontendID.String()},
		)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = closeClient() }()
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		if _, err := temporalClient.CheckHealth(ctx, &client.CheckHealthRequest{}); err == nil {
			t.Fatal("the client accepted a server with the wrong SPIFFE ID")
		}
		if peerID, _ := frontend.lastPeer(); !peerID.IsZero() {
			t.Errorf("a call reached the server from %s", peerID)
		}
	})
}

type testAuthority struct {
	certificate *x509.Certificate
	key         crypto.Signer
	bundle      *x509bundle.Bundle
}

func newTestAuthority(t *testing.T, trustDomain spiffeid.TrustDomain) *testAuthority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test CA"},
		URIs:                  []*url.URL{trustDomain.ID().URL()},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testAuthority{certificate: certificate, key: key, bundle: x509bundle.FromX509Authorities(trustDomain, []*x509.Certificate{certificate})}
}

func (a *testAuthority) issue(t *testing.T, id spiffeid.ID) *x509svid.SVID {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		URIs:         []*url.URL{id.URL()},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, a.certificate, key.Public(), a.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	svid, err := x509svid.ParseRaw(der, keyDER)
	if err != nil {
		t.Fatal(err)
	}
	return svid
}

// testWorkloadAPI serves one SVID and its bundle over FetchX509SVID.
type testWorkloadAPI struct {
	workload.UnimplementedSpiffeWorkloadAPIServer
	response    *workload.X509SVIDResponse
	address     string
	streamEnded chan struct{}
}

func startWorkloadAPI(t *testing.T, svid *x509svid.SVID, bundle *x509bundle.Bundle) *testWorkloadAPI {
	t.Helper()
	certificates, key, err := svid.MarshalRaw()
	if err != nil {
		t.Fatal(err)
	}
	var bundleDER []byte
	for _, authority := range bundle.X509Authorities() {
		bundleDER = append(bundleDER, authority.Raw...)
	}
	// A unix socket path may be at most 107 bytes, too few for t.TempDir.
	dir, err := os.MkdirTemp("", "p7spiffe")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "api.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	api := &testWorkloadAPI{
		response: &workload.X509SVIDResponse{Svids: []*workload.X509SVID{{
			SpiffeId: svid.ID.String(), X509Svid: certificates, X509SvidKey: key, Bundle: bundleDER,
		}}},
		address:     "unix://" + socket,
		streamEnded: make(chan struct{}, 8),
	}
	server := grpc.NewServer()
	workload.RegisterSpiffeWorkloadAPIServer(server, api)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return api
}

func (a *testWorkloadAPI) FetchX509SVID(_ *workload.X509SVIDRequest, stream grpc.ServerStreamingServer[workload.X509SVIDResponse]) error {
	defer func() { a.streamEnded <- struct{}{} }()
	if err := stream.Send(a.response); err != nil {
		return err
	}
	<-stream.Context().Done()
	return nil
}

// testFrontend is a TLS gRPC server that requires a client SVID from the
// trust domain and answers the health check and GetSystemInfo.
type testFrontend struct {
	address string

	mu      sync.Mutex
	peerID  spiffeid.ID
	version uint16
}

func startTLSFrontend(t *testing.T, svid *x509svid.SVID, bundle *x509bundle.Bundle) *testFrontend {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	frontend := &testFrontend{address: listener.Addr().String()}
	tlsConfig := tlsconfig.MTLSServerConfig(svid, bundle, tlsconfig.AuthorizeMemberOf(bundle.TrustDomain()))
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)), grpc.UnaryInterceptor(frontend.recordPeer))
	healthServer := health.NewServer()
	healthServer.SetServingStatus("temporal.api.workflowservice.v1.WorkflowService", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, healthServer)
	// GetSystemInfo answers Unimplemented, which the SDK takes as a server
	// with no extra capabilities.
	workflowservice.RegisterWorkflowServiceServer(server, workflowservice.UnimplementedWorkflowServiceServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return frontend
}

func (f *testFrontend) recordPeer(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if p, ok := peer.FromContext(ctx); ok {
		if tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo); ok && len(tlsInfo.State.PeerCertificates) > 0 {
			if id, err := x509svid.IDFromCert(tlsInfo.State.PeerCertificates[0]); err == nil {
				f.mu.Lock()
				f.peerID, f.version = id, tlsInfo.State.Version
				f.mu.Unlock()
			}
		}
	}
	return handler(ctx, request)
}

func (f *testFrontend) lastPeer() (spiffeid.ID, uint16) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.peerID, f.version
}
