package temporalauth

import (
	"crypto/md5"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"go.temporal.io/server/common/rpc/encryption"
)

// TLSProvider uses live Workload API sources, not certificate files.
// Backend services and the internal frontend accept only the server identity.
type TLSProvider struct {
	svid     x509svid.Source
	bundles  x509bundle.Source
	identity spiffeid.ID
}

var _ encryption.TLSConfigProvider = (*TLSProvider)(nil)

func NewTLSProvider(svid x509svid.Source, bundles x509bundle.Source, identity spiffeid.ID) (*TLSProvider, error) {
	if identity.IsZero() {
		return nil, fmt.Errorf("Temporal server SPIFFE ID is required")
	}
	current, err := svid.GetX509SVID()
	if err != nil {
		return nil, err
	}
	if current.ID != identity {
		return nil, fmt.Errorf("Workload API identity %s does not match %s", current.ID, identity)
	}
	return &TLSProvider{svid: svid, bundles: bundles, identity: identity}, nil
}

func (p *TLSProvider) serverConfig(internal bool) *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{"h2"},
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			svid, err := p.svid.GetX509SVID()
			if err != nil {
				return nil, err
			}
			if svid.ID != p.identity {
				return nil, fmt.Errorf("Workload API server identity changed")
			}
			bundle, err := p.bundles.GetX509BundleForTrustDomain(p.identity.TrustDomain())
			if err != nil {
				return nil, err
			}
			roots := x509.NewCertPool()
			for _, authority := range bundle.X509Authorities() {
				roots.AddCert(authority)
			}
			certificate := tls.Certificate{PrivateKey: svid.PrivateKey, Leaf: svid.Certificates[0]}
			for _, cert := range svid.Certificates {
				certificate.Certificate = append(certificate.Certificate, cert.Raw)
			}
			return &tls.Config{
				MinVersion:   tls.VersionTLS13,
				NextProtos:   []string{"h2"},
				Certificates: []tls.Certificate{certificate},
				ClientAuth:   tls.RequireAndVerifyClientCert,
				ClientCAs:    roots,
				VerifyConnection: func(state tls.ConnectionState) error {
					if len(state.VerifiedChains) == 0 || len(state.VerifiedChains[0]) == 0 {
						return fmt.Errorf("verified client certificate is required")
					}
					id, err := x509svid.IDFromCert(state.VerifiedChains[0][0])
					if err != nil {
						return err
					}
					if id.TrustDomain() != p.identity.TrustDomain() || (internal && id != p.identity) {
						return fmt.Errorf("SPIFFE peer %s is not permitted on this listener", id)
					}
					return nil
				},
			}, nil
		},
	}
}

func (p *TLSProvider) GetFrontendServerConfig() (*tls.Config, error) {
	return p.serverConfig(false), nil
}

func (p *TLSProvider) GetInternodeServerConfig() (*tls.Config, error) {
	return p.serverConfig(true), nil
}

// ClientTLSConfig is shared by gRPC callers and the raw TLS frontend proxy.
func ClientTLSConfig(svid x509svid.Source, bundles x509bundle.Source, serverID spiffeid.ID) *tls.Config {
	cfg := tlsconfig.MTLSClientConfig(svid, bundles, tlsconfig.AuthorizeID(serverID))
	cfg.MinVersion = tls.VersionTLS13
	cfg.NextProtos = []string{"h2"}
	return cfg
}

func (p *TLSProvider) GetFrontendClientConfig() (*tls.Config, error) {
	return ClientTLSConfig(p.svid, p.bundles, p.identity), nil
}

func (p *TLSProvider) GetInternodeClientConfig() (*tls.Config, error) {
	return p.GetFrontendClientConfig()
}

func (*TLSProvider) GetRemoteClusterClientConfig(string) (*tls.Config, error) {
	return nil, fmt.Errorf("remote Temporal clusters are not configured")
}

func (p *TLSProvider) GetExpiringCerts(window time.Duration) (encryption.CertExpirationMap, encryption.CertExpirationMap, error) {
	svid, err := p.svid.GetX509SVID()
	if err != nil {
		return nil, nil, err
	}
	expiring, expired := make(encryption.CertExpirationMap), make(encryption.CertExpirationMap)
	now := time.Now()
	for _, cert := range svid.Certificates {
		thumbprint := encryption.CertThumbprint(md5.Sum(cert.Raw))
		data := encryption.CertExpirationData{Thumbprint: thumbprint, IsCA: cert.IsCA, DNSNames: cert.DNSNames, Expiration: cert.NotAfter}
		if cert.NotAfter.Before(now) {
			expired[thumbprint] = data
		} else if cert.NotAfter.Before(now.Add(window)) {
			expiring[thumbprint] = data
		}
	}
	return expiring, expired, nil
}
