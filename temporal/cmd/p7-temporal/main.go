// p7-temporal runs Temporal with SPIFFE mTLS and config-driven authorization.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	_ "time/tzdata"

	"github.com/gfx-labs/protocol7/temporal/pkg/temporalauth"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/temporal"
	"google.golang.org/protobuf/types/known/durationpb"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	app := &cli.App{
		Name: "p7-temporal", Version: headers.ServerVersion,
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "config-file", EnvVars: []string{config.EnvKeyConfigFile}},
			&cli.StringFlag{Name: "mapping-file", EnvVars: []string{"TEMPORAL_AUTH_MAPPING_FILE"}},
			&cli.StringFlag{Name: "spiffe-id", EnvVars: []string{"TEMPORAL_SPIFFE_ID"}, Required: true},
			&cli.StringFlag{Name: "frontend-spiffe-id", EnvVars: []string{"TEMPORAL_FRONTEND_SPIFFE_ID"}},
			&cli.StringFlag{Name: "frontend-address", EnvVars: []string{"TEMPORAL_FRONTEND_ADDRESS"}, Value: "temporal-frontend:7233"},
		},
		Commands: []*cli.Command{
			{Name: "start", Action: startServer},
			{Name: "proxy", Flags: []cli.Flag{&cli.StringFlag{Name: "listen", Value: "127.0.0.1:7233"}}, Action: runProxy},
			{Name: "bootstrap", Flags: []cli.Flag{
				&cli.StringSliceFlag{Name: "namespace", Required: true},
				&cli.StringSliceFlag{Name: "search-attribute", Usage: "register Name=Type in every namespace, repeatable"},
			}, Action: bootstrap},
		},
	}
	if err := app.RunContext(ctx, os.Args); err != nil {
		slog.Error("p7-temporal stopped", "error", err)
		os.Exit(1)
	}
}

func openSource(c *cli.Context) (*workloadapi.X509Source, spiffeid.ID, error) {
	identity, err := spiffeid.FromString(c.String("spiffe-id"))
	if err != nil {
		return nil, identity, err
	}
	source, err := workloadapi.NewX509Source(c.Context, workloadapi.WithDefaultX509SVIDPicker(func(svids []*x509svid.SVID) *x509svid.SVID {
		for _, svid := range svids {
			if svid.ID == identity {
				return svid
			}
		}
		return nil
	}))
	if err != nil {
		return nil, identity, fmt.Errorf("connect SPIFFE Workload API: %w", err)
	}
	if _, err := source.GetX509SVID(); err != nil {
		_ = source.Close()
		return nil, identity, err
	}
	return source, identity, nil
}

func startServer(c *cli.Context) error {
	if c.String("mapping-file") == "" {
		return fmt.Errorf("--mapping-file is required")
	}
	file, err := os.Open(c.String("mapping-file"))
	if err != nil {
		return err
	}
	mapping, err := temporalauth.LoadConfig(file)
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	mapper, err := temporalauth.NewMapper(mapping)
	if err != nil {
		return err
	}
	var cfg *config.Config
	if c.String("config-file") != "" {
		cfg, err = config.Load(config.WithConfigFile(c.String("config-file")))
	} else {
		cfg, err = config.Load(config.WithEmbedded())
	}
	if err != nil {
		return err
	}
	source, identity, err := openSource(c)
	if err != nil {
		return err
	}
	defer source.Close()
	if identity.TrustDomain().String() != mapping.TrustDomain {
		return fmt.Errorf("server identity and mapping trust domains differ")
	}
	provider, err := temporalauth.NewTLSProvider(source, source, identity)
	if err != nil {
		return err
	}
	services := temporal.DefaultServices
	if names := os.Getenv("TEMPORAL_SERVICES"); names != "" {
		services = strings.Split(names, ",")
	}
	logger := log.NewZapLogger(log.BuildZapLogger(cfg.Log))
	server, err := temporal.NewServer(
		temporal.ForServices(services), temporal.WithConfig(cfg), temporal.WithLogger(logger),
		temporal.InterruptOn(temporal.InterruptCh()),
		temporal.WithTLSConfigFactory(provider),
		temporal.WithAuthorizer(temporalauth.NewAuthorizer()),
		temporal.WithClaimMapper(func(*config.Config) authorization.ClaimMapper { return mapper }),
	)
	if err != nil {
		return err
	}
	return server.Start()
}

func clientConfig(c *cli.Context, source *workloadapi.X509Source) (*tls.Config, error) {
	identity, err := spiffeid.FromString(c.String("frontend-spiffe-id"))
	if err != nil {
		return nil, fmt.Errorf("--frontend-spiffe-id is required: %w", err)
	}
	return temporalauth.ClientTLSConfig(source, source, identity), nil
}

// The proxy is loopback-only. Only its Workload API identity reaches Temporal.
func runProxy(c *cli.Context) error {
	host, _, err := net.SplitHostPort(c.String("listen"))
	if err != nil || !net.ParseIP(host).IsLoopback() {
		return fmt.Errorf("proxy listener must be a loopback IP address")
	}
	source, _, err := openSource(c)
	if err != nil {
		return err
	}
	defer source.Close()
	cfg, err := clientConfig(c, source)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", c.String("listen"))
	if err != nil {
		return err
	}
	defer listener.Close()
	go func() { <-c.Context.Done(); _ = listener.Close() }()
	var connections sync.WaitGroup
	defer connections.Wait()
	for {
		local, err := listener.Accept()
		if err != nil {
			if c.Context.Err() != nil {
				return nil
			}
			return err
		}
		connections.Add(1)
		go func() {
			defer connections.Done()
			defer local.Close()
			dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: cfg}
			remote, err := dialer.DialContext(c.Context, "tcp", c.String("frontend-address"))
			if err != nil {
				slog.Warn("Temporal proxy dial failed", "error", err)
				return
			}
			defer remote.Close()
			done := make(chan struct{})
			go func() {
				select {
				case <-c.Context.Done():
				case <-done:
				}
				_ = local.Close()
				_ = remote.Close()
			}()
			defer close(done)
			copied := make(chan struct{})
			go func() { _, _ = io.Copy(remote, local); _ = remote.Close(); close(copied) }()
			_, _ = io.Copy(local, remote)
			_ = local.Close()
			<-copied
		}()
	}
}

func bootstrap(c *cli.Context) error {
	attributes := make(map[string]enumspb.IndexedValueType)
	for _, value := range c.StringSlice("search-attribute") {
		name, typeName, ok := strings.Cut(value, "=")
		if !ok || name == "" {
			return fmt.Errorf("search attribute %q must be Name=Type", value)
		}
		attributeType, err := enumspb.IndexedValueTypeFromString(typeName)
		if err != nil {
			return err
		}
		if attributeType == enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED {
			return fmt.Errorf("search attribute %s needs a concrete type", name)
		}
		if previous, exists := attributes[name]; exists && previous != attributeType {
			return fmt.Errorf("search attribute %s has conflicting types", name)
		}
		attributes[name] = attributeType
	}
	source, _, err := openSource(c)
	if err != nil {
		return err
	}
	defer source.Close()
	cfg, err := clientConfig(c, source)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(c.Context, 2*time.Minute)
	defer cancel()
	connection, err := client.DialContext(ctx, client.Options{HostPort: c.String("frontend-address"), ConnectionOptions: client.ConnectionOptions{TLS: cfg}})
	if err != nil {
		return err
	}
	defer connection.Close()
	for _, name := range c.StringSlice("namespace") {
		_, err := connection.WorkflowService().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
			Namespace: name, WorkflowExecutionRetentionPeriod: durationpb.New(7 * 24 * time.Hour),
		})
		var exists *serviceerror.NamespaceAlreadyExists
		if err != nil && !errors.As(err, &exists) {
			return fmt.Errorf("create Temporal namespace %s: %w", name, err)
		}
		if len(attributes) == 0 {
			continue
		}
		operator := connection.OperatorService()
		existing, err := operator.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{Namespace: name})
		if err != nil {
			return fmt.Errorf("list search attributes of %s: %w", name, err)
		}
		missing := make(map[string]enumspb.IndexedValueType)
		for key, attributeType := range attributes {
			registeredType, registered := existing.GetCustomAttributes()[key]
			if !registered {
				registeredType, registered = existing.GetSystemAttributes()[key]
			}
			if registered && registeredType != attributeType {
				return fmt.Errorf("search attribute %s in %s is %s, requested %s", key, name, registeredType, attributeType)
			}
			if !registered {
				missing[key] = attributeType
			}
		}
		if len(missing) != 0 {
			_, err := operator.AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{Namespace: name, SearchAttributes: missing})
			if err != nil {
				return fmt.Errorf("add search attributes of %s: %w", name, err)
			}
		}
	}
	return nil
}
