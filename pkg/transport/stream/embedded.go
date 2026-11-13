package stream

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// Embedded is a NATS server with JetStream that runs inside this process.
// Local development uses it in place of a deployed NATS.
type Embedded struct {
	server   *server.Server
	storeDir string
}

// NewEmbedded configures a NATS server on host:port that keeps its JetStream
// data in storeDir. It does not listen until Start.
func NewEmbedded(host string, port int, storeDir string) (*Embedded, error) {
	natsServer, err := server.NewServer(&server.Options{
		ServerName: "p7-dev",
		Host:       host,
		Port:       port,
		JetStream:  true,
		StoreDir:   storeDir,
		NoSigs:     true,
		NoLog:      true,
	})
	if err != nil {
		return nil, fmt.Errorf("configure embedded NATS: %w", err)
	}
	return &Embedded{server: natsServer, storeDir: storeDir}, nil
}

// Start creates the store directory and runs the server, and returns once it
// accepts connections.
func (e *Embedded) Start() error {
	if err := os.MkdirAll(e.storeDir, 0o700); err != nil {
		return fmt.Errorf("create the embedded NATS store: %w", err)
	}
	go e.server.Start()
	if !e.server.ReadyForConnections(15 * time.Second) {
		e.Stop()
		return errors.New("embedded NATS did not start within 15 seconds")
	}
	return nil
}

// URL is the address clients connect to. With a port chosen by the system
// it is known only after Start.
func (e *Embedded) URL() string { return e.server.ClientURL() }

// Stop shuts the server down and waits for it.
func (e *Embedded) Stop() {
	e.server.Shutdown()
	e.server.WaitForShutdown()
}
