package p7

import (
	"context"
	"errors"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/journal"
	"github.com/gfx-labs/protocol7/pkg/transport/stream"
)

// ClientConfig configures a Client.
type ClientConfig struct {
	// NATSURL is where session frames are read from.
	//
	// The plan has clients read through ticketd and never from NATS. ticketd
	// does not exist yet, so this client reads the stream itself.
	NATSURL string
}

// Client reads what protocol7 records about running work.
type Client struct {
	frames *stream.Stream
}

// NewClient connects a client.
func NewClient(ctx context.Context, config ClientConfig) (*Client, error) {
	if config.NATSURL == "" {
		return nil, errors.New("new client: a NATS URL is required")
	}
	frames, err := stream.Connect(ctx, config.NATSURL)
	if err != nil {
		return nil, err
	}
	return &Client{frames: frames}, nil
}

// Close ends the client's connections.
func (c *Client) Close() { c.frames.Close() }

// WatchSession calls handle for every event of a session, from its first
// event on, and then for each new event as it happens, until ctx ends or
// handle returns an error. A session with no events yet is waited for.
func (c *Client) WatchSession(ctx context.Context, session string, handle func(SessionEvent) error) error {
	decoder := journal.NewDecoder(session)
	return c.frames.Read(ctx, session, func(frame domain.Frame) error {
		for _, event := range decoder.Decode(frame) {
			if err := handle(event); err != nil {
				return err
			}
		}
		return nil
	})
}
