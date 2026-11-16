package egress_test

import (
	"bufio"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"

	"github.com/gfx-labs/protocol7/pkg/egress"
)

// The gateway here is a stand-in for PDP: it requires a client SVID, answers
// CONNECT, and terminates the target's TLS with a certificate from its own
// interception CA, which the client is given as Config.CAFile.
func TestRequestsGoThroughTheGatewayAsTheWorkload(t *testing.T) {
	trustDomain := spiffeid.RequireTrustDomainFromString("example.org")
	authority := newAuthority(t, trustDomain)
	workloadID := spiffeid.RequireFromPath(trustDomain, "/ns/app/sa/navi")
	gatewayID := spiffeid.RequireFromPath(trustDomain, "/ns/app/sa/pdp")
	interception := newInterceptionCA(t)
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, interception.pem, 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("https goes through CONNECT with the workload's identity", func(t *testing.T) {
		gateway := startGateway(t, authority.issue(t, gatewayID), authority.bundle, interception)
		e, err := egress.NewFromSource(authority.issue(t, workloadID), authority.bundle, egress.Config{Gateway: gateway.address, GatewayID: gatewayID.String(), CAFile: caFile})
		if err != nil {
			t.Fatal(err)
		}
		response, err := e.Client(10 * time.Second).Get("https://api.example.test/repos/x?y=1")
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		if response.StatusCode != 200 || string(body) != "GET /repos/x?y=1 as "+workloadID.String() {
			t.Fatalf("got %d %q", response.StatusCode, body)
		}
		if got := gateway.connects(); len(got) != 1 || got[0] != "api.example.test:443" {
			t.Fatalf("CONNECT targets %v", got)
		}
		if gateway.version() != tls.VersionTLS13 {
			t.Errorf("TLS version %#x, want 1.3", gateway.version())
		}
	})

	t.Run("plain http goes to the gateway as an absolute URL", func(t *testing.T) {
		gateway := startGateway(t, authority.issue(t, gatewayID), authority.bundle, interception)
		e, err := egress.NewFromSource(authority.issue(t, workloadID), authority.bundle, egress.Config{Gateway: gateway.address, GatewayID: gatewayID.String()})
		if err != nil {
			t.Fatal(err)
		}
		response, err := e.Client(10 * time.Second).Get("http://store.test:8333/b/k")
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		if string(body) != "GET http://store.test:8333/b/k as "+workloadID.String() {
			t.Fatalf("got %q", body)
		}
	})

	t.Run("a host whose certificate the interception CA did not sign is refused", func(t *testing.T) {
		gateway := startGateway(t, authority.issue(t, gatewayID), authority.bundle, interception)
		// No CAFile: the interception root is not trusted.
		e, err := egress.NewFromSource(authority.issue(t, workloadID), authority.bundle, egress.Config{Gateway: gateway.address, GatewayID: gatewayID.String()})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.Client(5 * time.Second).Get("https://api.example.test/"); err == nil {
			t.Fatal("the client trusted a certificate that no trusted root signed")
		}
	})

	t.Run("a gateway with another SPIFFE ID gets no request", func(t *testing.T) {
		gateway := startGateway(t, authority.issue(t, spiffeid.RequireFromPath(trustDomain, "/other")), authority.bundle, interception)
		e, err := egress.NewFromSource(authority.issue(t, workloadID), authority.bundle, egress.Config{Gateway: gateway.address, GatewayID: gatewayID.String(), CAFile: caFile})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.Client(5 * time.Second).Get("https://api.example.test/"); err == nil {
			t.Fatal("the client used a gateway with the wrong SPIFFE ID")
		}
		if got := gateway.connects(); len(got) != 0 {
			t.Fatalf("a request reached the gateway: %v", got)
		}
	})

	t.Run("the environment's proxy setting is not used", func(t *testing.T) {
		gateway := startGateway(t, authority.issue(t, gatewayID), authority.bundle, interception)
		t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
		e, err := egress.NewFromSource(authority.issue(t, workloadID), authority.bundle, egress.Config{Gateway: gateway.address, GatewayID: gatewayID.String(), CAFile: caFile})
		if err != nil {
			t.Fatal(err)
		}
		response, err := e.Client(10 * time.Second).Get("https://api.example.test/")
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
	})
}

func TestConfigIsChecked(t *testing.T) {
	trustDomain := spiffeid.RequireTrustDomainFromString("example.org")
	authority := newAuthority(t, trustDomain)
	svid := authority.issue(t, spiffeid.RequireFromPath(trustDomain, "/w"))
	empty := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(empty, []byte("nothing"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, config := range map[string]egress.Config{
		"no port":     {Gateway: "pdp", GatewayID: "spiffe://example.org/pdp"},
		"no gateway":  {GatewayID: "spiffe://example.org/pdp"},
		"bad ID":      {Gateway: "pdp:8443", GatewayID: "pdp"},
		"no ID":       {Gateway: "pdp:8443"},
		"missing CA":  {Gateway: "pdp:8443", GatewayID: "spiffe://example.org/pdp", CAFile: filepath.Join(t.TempDir(), "none")},
		"CA no certs": {Gateway: "pdp:8443", GatewayID: "spiffe://example.org/pdp", CAFile: empty},
	} {
		if _, err := egress.NewFromSource(svid, authority.bundle, config); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// New refuses these before it waits for an SVID.
	if _, err := egress.New(t.Context(), egress.Config{Gateway: "pdp", GatewayID: "spiffe://example.org/pdp"}); err == nil {
		t.Error("New accepted a gateway with no port")
	}
	if _, err := egress.New(t.Context(), egress.Config{WorkloadAPI: "/run/x.sock", Gateway: "pdp:1", GatewayID: "spiffe://example.org/pdp"}); err == nil {
		t.Error("New accepted a Workload API address with no scheme")
	}
}

type gateway struct {
	address string

	mu      sync.Mutex
	targets []string
	tls     uint16
}

func (g *gateway) connects() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.targets...)
}

func (g *gateway) version() uint16 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.tls
}

func startGateway(t *testing.T, svid *x509svid.SVID, bundle *x509bundle.Bundle, interception *interceptionCA) *gateway {
	t.Helper()
	g := &gateway{}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peer, err := x509svid.IDFromCert(r.TLS.PeerCertificates[0])
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		g.mu.Lock()
		g.tls = r.TLS.Version
		g.mu.Unlock()
		if r.Method != http.MethodConnect {
			// A plain request names its target as an absolute URL.
			fmt.Fprintf(w, "%s %s as %s", r.Method, r.URL.String(), peer)
			return
		}
		g.mu.Lock()
		g.targets = append(g.targets, r.Host)
		g.mu.Unlock()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
		host, _, _ := net.SplitHostPort(r.Host)
		inner := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{interception.issue(t, host)}, NextProtos: []string{"http/1.1"}})
		if err := inner.Handshake(); err != nil {
			return
		}
		request, err := http.ReadRequest(bufio.NewReader(inner))
		if err != nil {
			return
		}
		body := fmt.Sprintf("%s %s as %s", request.Method, request.URL.RequestURI(), peer)
		fmt.Fprintf(inner, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(body), body)
	})}
	config := tlsconfig.MTLSServerConfig(svid, bundle, tlsconfig.AuthorizeMemberOf(bundle.TrustDomain()))
	config.NextProtos = []string{"http/1.1"}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", config)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	g.address = listener.Addr().String()
	return g
}

type authority struct {
	certificate *x509.Certificate
	key         crypto.Signer
	bundle      *x509bundle.Bundle
}

func newAuthority(t *testing.T, trustDomain spiffeid.TrustDomain) *authority {
	t.Helper()
	certificate, key := newCA(t, &x509.Certificate{Subject: pkix.Name{CommonName: "test CA"}, URIs: []*url.URL{trustDomain.ID().URL()}})
	return &authority{certificate: certificate, key: key, bundle: x509bundle.FromX509Authorities(trustDomain, []*x509.Certificate{certificate})}
}

func newCA(t *testing.T, template *x509.Certificate) (*x509.Certificate, crypto.Signer) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template.SerialNumber = big.NewInt(1)
	template.NotBefore = time.Now().Add(-time.Minute)
	template.NotAfter = time.Now().Add(time.Hour)
	template.IsCA = true
	template.BasicConstraintsValid = true
	template.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageCRLSign
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return certificate, key
}

func (a *authority) issue(t *testing.T, id spiffeid.ID) *x509svid.SVID {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: serial,
		URIs:         []*url.URL{id.URL()},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}, a.certificate, key.Public(), a.key)
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

type interceptionCA struct {
	certificate *x509.Certificate
	key         crypto.Signer
	pem         []byte
}

func newInterceptionCA(t *testing.T) *interceptionCA {
	t.Helper()
	certificate, key := newCA(t, &x509.Certificate{Subject: pkix.Name{CommonName: "interception CA"}})
	return &interceptionCA{certificate: certificate, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})}
}

func (c *interceptionCA) issue(t *testing.T, host string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		DNSNames:     []string{host},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, c.certificate, key.Public(), c.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
