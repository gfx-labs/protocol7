package stream

import (
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// Embedded is a NATS server with JetStream that runs inside this process.
// Local development uses it in place of a deployed NATS.
type Embedded struct {
	server *server.Server
}

// StartEmbedded runs a NATS server on host:port that keeps its JetStream
// data in storeDir.
func StartEmbedded(host string, port int, storeDir string) (*Embedded, error) {
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
	go natsServer.Start()
	if !natsServer.ReadyForConnections(15 * time.Second) {
		natsServer.Shutdown()
		return nil, errors.New("embedded NATS did not start within 15 seconds")
	}
	return &Embedded{server: natsServer}, nil
}

// URL is the address clients connect to.
func (e *Embedded) URL() string { return e.server.ClientURL() }

// Stop shuts the server down and waits for it.
func (e *Embedded) Stop() {
	e.server.Shutdown()
	e.server.WaitForShutdown()
}
