package hooks

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"log/slog"
	"math/big"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
)

// testCA issues SVIDs whose only SAN is a SPIFFE URI, like the cluster's.
type testCA struct {
	cert   *x509.Certificate
	key    *ecdsa.PrivateKey
	bundle *x509bundle.Bundle
	serial int64
}

func newTestCA(t *testing.T, trustDomain spiffeid.TrustDomain) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{},
		URIs:                  []*url.URL{trustDomain.ID().URL()},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{cert: cert, key: key, bundle: x509bundle.FromX509Authorities(trustDomain, []*x509.Certificate{cert}), serial: 1}
}

func (ca *testCA) svid(t *testing.T, id spiffeid.ID) *x509svid.SVID {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca.serial++
	template := &x509.Certificate{
		SerialNumber: big.NewInt(ca.serial),
		URIs:         []*url.URL{id.URL()},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &x509svid.SVID{ID: id, Certificates: []*x509.Certificate{cert}, PrivateKey: key}
}

// TestSubscribeWithSPIFFEIdentity checks that nats-server's TLSMap maps the
// SPIFFE URI SAN to a user when the caller supplies the go-spiffe TLS config,
// that the granted user reads and acknowledges through its durable, and that
// an unmapped SPIFFE ID is refused.
func TestSubscribeWithSPIFFEIdentity(t *testing.T) {
	trustDomain := spiffeid.RequireTrustDomainFromString("metal.test")
	ca := newTestCA(t, trustDomain)
	serverID := spiffeid.RequireFromPath(trustDomain, "/ns/webhooks/sa/webhooks")
	tenantID := spiffeid.RequireFromPath(trustDomain, "/ns/p7-oku-review/sa/release")
	strangerID := spiffeid.RequireFromPath(trustDomain, "/ns/p7-oku/sa/default")
	const consumerName = "hooks-oku-review"

	serverTLS := tlsconfig.MTLSServerConfig(ca.svid(t, serverID), ca.bundle, tlsconfig.AuthorizeMemberOf(trustDomain))
	natsServer, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoSigs:    true,
		NoLog:     true,
		TLSConfig: serverTLS,
		TLSVerify: true,
		TLSMap:    true,
		Users: []*server.User{
			{Username: serverID.String()},
			{Username: tenantID.String(), Permissions: &server.Permissions{
				Publish: &server.SubjectPermission{Allow: []string{
					"$JS.API.CONSUMER.MSG.NEXT." + Stream + "." + consumerName,
					"$JS.API.CONSUMER.INFO." + Stream + "." + consumerName,
					"$JS.ACK." + Stream + "." + consumerName + ".>",
				}},
				Subscribe: &server.SubjectPermission{Allow: []string{"_INBOX.>"}},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	go natsServer.Start()
	t.Cleanup(func() { natsServer.Shutdown(); natsServer.WaitForShutdown() })
	if !natsServer.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats-server did not start")
	}
	url := natsServer.ClientURL()

	clientTLS := func(id spiffeid.ID) *tls.Config {
		config := tlsconfig.MTLSClientConfig(ca.svid(t, id), ca.bundle, tlsconfig.AuthorizeID(serverID))
		config.MinVersion = tls.VersionTLS13
		return config
	}

	// The receiver's own identity sets up the stream and consumer and publishes.
	admin, err := nats.Connect(url, nats.Secure(clientTLS(serverID)))
	if err != nil {
		t.Fatalf("receiver identity connect: %v", err)
	}
	defer admin.Close()
	js, err := jetstream.New(admin)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: Stream, Subjects: []string{"hooks.>"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateOrUpdateConsumer(ctx, Stream, jetstream.ConsumerConfig{
		Durable:        consumerName,
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: []string{"hooks.github.oku-trade.trade.pull_request"},
	}); err != nil {
		t.Fatal(err)
	}
	msg := nats.NewMsg("hooks.github.oku-trade.trade.pull_request")
	msg.Header.Set(HeaderDelivery, "delivery-1")
	msg.Header.Set(HeaderEvent, "pull_request")
	msg.Header.Set(nats.MsgIdHdr, "delivery-1")
	msg.Data = []byte(`{"action":"opened"}`)
	if _, err := js.PublishMsg(ctx, msg); err != nil {
		t.Fatal(err)
	}

	var got Delivery
	readCtx, stopReading := context.WithCancel(ctx)
	err = subscribe(readCtx, url, consumerName, clientTLS(tenantID), func(_ context.Context, delivery Delivery) error {
		got = delivery
		stopReading()
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("granted subscribe: %v", err)
	}
	if got.ID != "delivery-1" || got.Event != "pull_request" || string(got.Body) != `{"action":"opened"}` {
		t.Fatalf("delivery = %+v", got)
	}
	consumer, err := js.Consumer(ctx, Stream, consumerName)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		info, err := consumer.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.AckFloor.Consumer == 1 && info.NumAckPending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivery not acknowledged: ack floor %d, pending %d", info.AckFloor.Consumer, info.NumAckPending)
		}
		time.Sleep(50 * time.Millisecond)
	}

	err = subscribe(ctx, url, consumerName, clientTLS(strangerID), func(context.Context, Delivery) error { return nil })
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "authorization") {
		t.Fatalf("ungranted SPIFFE ID: got %v, want an authorization error", err)
	}
}

// A bus that cannot be reached does not end the subscriber: it logs, waits
// and tries again, and it returns, with no error, when its context ends.
func TestSubscribeRetryingSurvivesAnUnreachableBus(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	attempts := 0
	var logged strings.Builder
	logger := slog.New(slog.NewTextHandler(&logged, nil))
	done := make(chan error, 1)
	go func() {
		done <- retry(ctx, logger, time.Millisecond, 4*time.Millisecond, func(context.Context) error {
			attempts++
			if attempts == 5 {
				cancel()
			}
			return errors.New("connect to the webhook bus: dial tcp 10.43.54.202:4222: connect: operation not permitted")
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("retry ended with %v, want nil once the context ended", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("retry did not return")
	}
	if attempts != 5 {
		t.Errorf("%d attempts, want 5 (the fifth cancels)", attempts)
	}
	if got := strings.Count(logged.String(), "operation not permitted"); got != 4 {
		t.Errorf("logged the failure %d times, want 4 (not the one that the cancel caused)", got)
	}
}
