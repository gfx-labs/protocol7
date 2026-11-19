package p7

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"go.temporal.io/sdk/client"
)

// TemporalSPIFFE turns on mutual TLS to Temporal with X.509 SVIDs from the
// SPIFFE Workload API. The zero value keeps the connection in plaintext.
type TemporalSPIFFE struct {
	// Socket is the Workload API address, such as
	// unix:///run/spire/agent-sockets/api.sock. Empty uses the
	// SPIFFE_ENDPOINT_SOCKET environment variable.
	Socket string
	// ServerID is the SPIFFE ID the Temporal frontend must present, such as
	// spiffe://example.org/temporal/frontend. Setting it turns mutual TLS on.
	ServerID string
}

// workloadAPITimeout bounds the wait for this workload's first SVID.
const workloadAPITimeout = time.Minute

// NewTemporalClient returns a lazy Temporal client made from options. With
// spiffe set, the client presents this workload's SVID, which the Workload
// API renews, and accepts only a server that presents spiffe.ServerID.
// Options without a DataConverter get DataConverter.
// Call closeClient, not the client's Close, when done: it closes the client
// and then the source of its certificates.
func NewTemporalClient(options client.Options, spiffe TemporalSPIFFE) (temporalClient client.Client, closeClient func() error, err error) {
	if options.DataConverter == nil {
		options.DataConverter = DataConverter()
	}
	if spiffe.ServerID == "" {
		if spiffe.Socket != "" {
			return nil, nil, errors.New("a SPIFFE Workload API socket is set without the Temporal server's SPIFFE ID")
		}
		temporalClient, err := client.NewLazyClient(options)
		if err != nil {
			return nil, nil, err
		}
		return temporalClient, func() error { temporalClient.Close(); return nil }, nil
	}
	serverID, err := spiffeid.FromString(spiffe.ServerID)
	if err != nil {
		return nil, nil, fmt.Errorf("Temporal server SPIFFE ID %q: %w", spiffe.ServerID, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), workloadAPITimeout)
	defer cancel()
	var sourceOptions []workloadapi.X509SourceOption
	if spiffe.Socket != "" {
		sourceOptions = append(sourceOptions, workloadapi.WithClientOptions(workloadapi.WithAddr(spiffe.Socket)))
	}
	source, err := workloadapi.NewX509Source(ctx, sourceOptions...)
	if err != nil {
		return nil, nil, fmt.Errorf("get an SVID from the SPIFFE Workload API: %w", err)
	}
	tlsConfig := tlsconfig.MTLSClientConfig(source, source, tlsconfig.AuthorizeID(serverID))
	tlsConfig.MinVersion = tls.VersionTLS13
	options.ConnectionOptions.TLS = tlsConfig
	temporalClient, err = client.NewLazyClient(options)
	if err != nil {
		return nil, nil, errors.Join(err, source.Close())
	}
	closeClient = func() error {
		temporalClient.Close()
		return source.Close()
	}
	return temporalClient, closeClient, nil
}
