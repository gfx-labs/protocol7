package snapshot

import "net"

// newSocket listens on a unix socket at path, so that a tree has one. The
// returned function closes it.
func newSocket(path string) (func(), error) {
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	return func() { _ = listener.Close() }, nil
}
