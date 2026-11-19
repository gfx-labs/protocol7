package build

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/gfx-labs/protocol7/pkg/transport/githubapi"
)

// Gateway opens an mTLS connection to PDP as this workload.
// egress.Egress satisfies it.
type Gateway interface {
	DialGateway(ctx context.Context) (net.Conn, error)
}

// Forwarder is an in-process stand-in for `pdp forward`: a loopback listener
// whose connections are copied to PDP, so that git, which cannot present a
// client certificate to a proxy, reaches PDP as the builder.
type Forwarder struct {
	listener net.Listener
	gateway  Gateway
	wait     sync.WaitGroup
}

// NewForwarder listens on a free loopback port.
func NewForwarder(gateway Gateway) (*Forwarder, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	return &Forwarder{listener: listener, gateway: gateway}, nil
}

// ProxyURL is what HTTPS_PROXY should be for a subprocess.
func (f *Forwarder) ProxyURL() string { return "http://" + f.listener.Addr().String() }

// Serve copies each accepted connection to PDP until the listener closes.
func (f *Forwarder) Serve(ctx context.Context) error {
	for {
		local, err := f.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				f.wait.Wait()
				return nil
			}
			return err
		}
		f.wait.Add(1)
		go func() {
			defer f.wait.Done()
			defer local.Close()
			remote, err := f.gateway.DialGateway(ctx)
			if err != nil {
				return
			}
			defer remote.Close()
			done := make(chan struct{}, 2)
			go func() { _, _ = io.Copy(remote, local); done <- struct{}{} }()
			go func() { _, _ = io.Copy(local, remote); done <- struct{}{} }()
			<-done
		}()
	}
}

// Close stops accepting connections.
func (f *Forwarder) Close() error { return f.listener.Close() }

// GitHubTree lists a commit's tree with the GitHub trees API.
func GitHubTree(github *githubapi.Client) func(ctx context.Context, repository, commit string) (map[string]string, error) {
	return func(ctx context.Context, repository, commit string) (map[string]string, error) {
		var tree struct {
			Truncated bool `json:"truncated"`
			Tree      []struct {
				Path string `json:"path"`
				Type string `json:"type"`
				SHA  string `json:"sha"`
			} `json:"tree"`
		}
		if err := github.Do(ctx, "GET", "/repos/"+repository+"/git/trees/"+commit+"?recursive=1", nil, &tree); err != nil {
			return nil, githubapi.Settle(err)
		}
		if tree.Truncated {
			return nil, fmt.Errorf("the tree of %s at %s is too large for one call", repository, commit)
		}
		files := make(map[string]string, len(tree.Tree))
		for _, entry := range tree.Tree {
			if entry.Type != "tree" {
				files[entry.Path] = entry.SHA
			}
		}
		return files, nil
	}
}
