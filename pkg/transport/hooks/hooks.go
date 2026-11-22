// Package hooks reads verified GitHub and Linear webhook deliveries from the
// webhook bus, a JetStream stream that the protocol7-selfdev receiver fills.
// A tenant connects with its SPIFFE identity and reads the durable consumer
// granted to it. It cannot create consumers or widen its filter.
package hooks

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
)

const (
	// Stream is the JetStream stream that holds deliveries.
	Stream = "HOOKS"

	// Header names the receiver sets on each message.
	HeaderSource       = "Hook-Source"
	HeaderEvent        = "Hook-Event"
	HeaderDelivery     = "Hook-Delivery"
	HeaderSignature    = "Hook-Signature"
	HeaderInstallation = "Hook-Installation"

	fetchBatch   = 16
	fetchMaxWait = 5 * time.Second
)

// SubscriberConfig says which bus and consumer to read and how to prove this
// workload's identity.
type SubscriberConfig struct {
	// URL is the bus address, nats://webhooks.webhooks.svc:4222.
	URL string
	// Consumer is the durable granted to this tenant, hooks-oku-review.
	Consumer string
	// SPIFFESocket is the Workload API socket. The client presents this
	// workload's SVID. Empty uses SPIFFE_ENDPOINT_SOCKET.
	SPIFFESocket string
	// ServerID is the bus's SPIFFE ID.
	ServerID string
}

// Delivery is one verified webhook delivery.
type Delivery struct {
	Source, Event, ID, Installation string
	Subject                         string
	// Signature is the provider's signature header, kept so a consumer can
	// verify the body itself if it holds the secret.
	Signature string
	Body      []byte
}

// Subscribe pulls deliveries and calls handle for each. It acknowledges a delivery
// only after handle returns nil, and leaves it for redelivery otherwise.
// It returns when ctx ends or the connection fails.
func Subscribe(ctx context.Context, config SubscriberConfig, handle func(context.Context, Delivery) error) error {
	if config.URL == "" || config.Consumer == "" || config.ServerID == "" {
		return errors.New("hooks subscriber needs URL, Consumer and ServerID")
	}
	serverID, err := spiffeid.FromString(config.ServerID)
	if err != nil {
		return fmt.Errorf("bus SPIFFE ID %q: %w", config.ServerID, err)
	}
	var sourceOptions []workloadapi.X509SourceOption
	if config.SPIFFESocket != "" {
		sourceOptions = append(sourceOptions, workloadapi.WithClientOptions(workloadapi.WithAddr(config.SPIFFESocket)))
	}
	source, err := workloadapi.NewX509Source(ctx, sourceOptions...)
	if err != nil {
		return fmt.Errorf("get an SVID from the SPIFFE Workload API: %w", err)
	}
	defer source.Close()
	tlsConfig := tlsconfig.MTLSClientConfig(source, source, tlsconfig.AuthorizeID(serverID))
	tlsConfig.MinVersion = tls.VersionTLS13
	return subscribe(ctx, config.URL, config.Consumer, tlsConfig, handle)
}

func subscribe(ctx context.Context, url, consumerName string, tlsConfig *tls.Config, handle func(context.Context, Delivery) error) error {
	connection, err := nats.Connect(url, nats.Name("protocol7-hooks"), nats.Secure(tlsConfig), nats.MaxReconnects(-1))
	if err != nil {
		return fmt.Errorf("connect to the webhook bus: %w", err)
	}
	defer connection.Close()
	js, err := jetstream.New(connection)
	if err != nil {
		return fmt.Errorf("open JetStream: %w", err)
	}
	consumer, err := js.Consumer(ctx, Stream, consumerName)
	if err != nil {
		return fmt.Errorf("look up consumer %s: %w", consumerName, err)
	}
	for ctx.Err() == nil {
		batch, err := consumer.Fetch(fetchBatch, jetstream.FetchMaxWait(fetchMaxWait))
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return fmt.Errorf("fetch from %s: %w", consumerName, err)
		}
		for msg := range batch.Messages() {
			delivery := deliveryFrom(msg)
			if err := handle(ctx, delivery); err != nil {
				_ = msg.Nak()
				continue
			}
			if err := msg.Ack(); err != nil {
				return fmt.Errorf("acknowledge delivery %s: %w", delivery.ID, err)
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && ctx.Err() == nil {
			return fmt.Errorf("fetch from %s: %w", consumerName, err)
		}
	}
	return ctx.Err()
}

func deliveryFrom(msg jetstream.Msg) Delivery {
	headers := msg.Headers()
	return Delivery{
		Source:       headers.Get(HeaderSource),
		Event:        headers.Get(HeaderEvent),
		ID:           headers.Get(HeaderDelivery),
		Installation: headers.Get(HeaderInstallation),
		Signature:    headers.Get(HeaderSignature),
		Subject:      msg.Subject(),
		Body:         msg.Data(),
	}
}

const (
	// retryMin and retryMax bound the wait between attempts of
	// SubscribeRetrying.
	retryMin = 5 * time.Second
	retryMax = 5 * time.Minute
	// retryStable is how long a subscription must run for the next wait to
	// start from the least again.
	retryStable = time.Minute
)

// SubscribeRetrying is Subscribe for a consumer that can do without the bus
// for a while: it logs a failure (no route to the bus, a refused identity, a
// consumer not granted yet) and tries again after a wait that grows from five
// seconds to five minutes, and it returns only when ctx ends. A caller that
// has other work, such as a builder that also serves the build queue, stays
// up while the bus is not reachable and takes its deliveries once it is.
func SubscribeRetrying(ctx context.Context, logger *slog.Logger, config SubscriberConfig, handle func(context.Context, Delivery) error) error {
	return retry(ctx, logger, retryMin, retryMax, func(ctx context.Context) error {
		return Subscribe(ctx, config, handle)
	})
}

func retry(ctx context.Context, logger *slog.Logger, shortest, longest time.Duration, run func(context.Context) error) error {
	wait := shortest
	for ctx.Err() == nil {
		started := time.Now()
		err := run(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if time.Since(started) >= retryStable {
			wait = shortest
		}
		logger.Warn("webhook bus: subscription ended, trying again", "error", err, "wait", wait)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		wait = min(wait*2, longest)
	}
	return nil
}
