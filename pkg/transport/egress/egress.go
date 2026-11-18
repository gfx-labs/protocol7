// Package egress sends a process's own HTTP requests through PDP with the
// process's SPIFFE identity, with no forwarder container in the pod.
//
// PDP accepts only mutual TLS with an SVID, and a stock HTTP client cannot
// present a client certificate to a proxy, so a pod normally runs `pdp
// forward` next to the process and points HTTPS_PROXY at it. The forwarder
// reads no HTTP: it opens an mTLS connection to PDP for each connection it
// accepts and copies bytes both ways. A Go program can do the same in its
// own http.Transport, by dialing PDP where the transport would dial its
// proxy. The proxy conversation, CONNECT included, then reaches PDP as the
// client wrote it, and PDP intercepts the target's TLS as it does for the
// forwarder.
//
//	e, err := egress.New(ctx, egress.Config{
//		Gateway:   "pdp:8443",
//		GatewayID: "spiffe://example.org/ns/p7/sa/pdp",
//		CAFile:    "/etc/ssl/egress/ca-bundle.crt",
//	})
//	if err != nil { ... }
//	defer e.Close()
//	response, err := e.Client(30*time.Second).Get("https://api.github.com/...")
//
// The identity is the process's: every request made with the client is made
// as the workload, so PDP's policy judges the process and not the caller
// inside it. Use it in a process whose own code is trusted with that
// identity, such as a web server.
package egress

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
)

const (
	// DialTimeout bounds the connection and the TLS handshake to PDP.
	DialTimeout = 10 * time.Second
	// sourceTimeout bounds the wait for this workload's first SVID.
	sourceTimeout = time.Minute
)

// Config says how to reach PDP.
type Config struct {
	// WorkloadAPI is the SPIFFE Workload API address, such as
	// unix:///spiffe-workload-api/spire-agent.sock. Empty uses
	// SPIFFE_ENDPOINT_SOCKET.
	WorkloadAPI string
	// Gateway is PDP's SPIFFE listener, host:port.
	Gateway string
	// GatewayID is the SPIFFE ID PDP must present. A server with any other
	// ID is refused before a byte of a request is sent.
	GatewayID string
	// CAFile is a PEM file of certificates to trust in addition to the
	// system roots: the root of PDP's interception CA, which signs the
	// certificate of every host reached through it. Empty trusts the system
	// roots alone, which is right when SSL_CERT_FILE already names a bundle
	// that holds it.
	CAFile string
}

// Egress is a source of this workload's identity and the means to make HTTP
// requests through PDP with it.
type Egress struct {
	gateway   string
	proxy     *url.URL
	tlsConfig *tls.Config
	roots     *x509.CertPool
	closer    func() error
}

// New opens the Workload API source, waits for the workload's first SVID and
// returns an Egress. Close it when done.
func New(ctx context.Context, config Config) (*Egress, error) {
	var options []workloadapi.X509SourceOption
	if config.WorkloadAPI != "" {
		if !strings.HasPrefix(config.WorkloadAPI, "unix://") {
			return nil, errors.New("egress: the Workload API address must start with unix://")
		}
		options = append(options, workloadapi.WithClientOptions(workloadapi.WithAddr(config.WorkloadAPI)))
	}
	// Check the rest first, so that a mistake does not wait for an SVID.
	if _, _, err := config.check(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, sourceTimeout)
	defer cancel()
	source, err := workloadapi.NewX509Source(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("egress: get an SVID from the SPIFFE Workload API: %w", err)
	}
	e, err := NewFromSource(source, source, config)
	if err != nil {
		return nil, errors.Join(err, source.Close())
	}
	e.closer = source.Close
	return e, nil
}

// NewFromSource is New with an SVID and bundle that the caller already has.
// Config.WorkloadAPI is ignored. Close does not close the sources.
func NewFromSource(svid x509svid.Source, bundle x509bundle.Source, config Config) (*Egress, error) {
	id, host, err := config.check()
	if err != nil {
		return nil, err
	}
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if config.CAFile != "" {
		pem, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("egress: %w", err)
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("egress: %s holds no certificate", config.CAFile)
		}
	}
	tlsConfig := tlsconfig.MTLSClientConfig(svid, bundle, tlsconfig.AuthorizeID(id))
	tlsConfig.MinVersion = tls.VersionTLS13
	return &Egress{
		gateway:   config.Gateway,
		proxy:     &url.URL{Scheme: "http", Host: host},
		tlsConfig: tlsConfig,
		roots:     roots,
		closer:    func() error { return nil },
	}, nil
}

func (c Config) check() (spiffeid.ID, string, error) {
	host, port, err := net.SplitHostPort(c.Gateway)
	if err != nil || host == "" || port == "" {
		return spiffeid.ID{}, "", errors.New("egress: the gateway must be host:port")
	}
	id, err := spiffeid.FromString(c.GatewayID)
	if err != nil {
		return spiffeid.ID{}, "", fmt.Errorf("egress: the gateway's SPIFFE ID %q: %w", c.GatewayID, err)
	}
	return id, c.Gateway, nil
}

// DialGateway opens an mTLS connection to PDP, as the workload. The caller
// speaks the proxy protocol on it: CONNECT, or a plain request with an
// absolute URL.
func (e *Egress) DialGateway(ctx context.Context) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, DialTimeout)
	defer cancel()
	return (&tls.Dialer{Config: e.tlsConfig}).DialContext(ctx, "tcp", e.gateway)
}

// Transport returns a new transport that sends every request through PDP. The
// address it would dial for a proxy is ignored: each connection goes to
// the gateway. It trusts the system roots and Config.CAFile for the hosts
// behind PDP, and it takes no proxy setting from the environment.
func (e *Egress) Transport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyURL(e.proxy),
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return e.DialGateway(ctx)
		},
		TLSClientConfig:       &tls.Config{RootCAs: e.roots, MinVersion: tls.VersionTLS12},
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          32,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   DialTimeout,
		ExpectContinueTimeout: time.Second,
	}
}

// Client returns an http.Client on a new Transport. timeout is the whole
// request, as http.Client.Timeout; zero is none.
func (e *Egress) Client(timeout time.Duration) *http.Client {
	return &http.Client{Transport: e.Transport(), Timeout: timeout}
}

// Close releases the Workload API source, if New opened one. Transports that
// were made stop being able to dial.
func (e *Egress) Close() error { return e.closer() }
