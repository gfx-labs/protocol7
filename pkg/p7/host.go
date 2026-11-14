package p7

import "github.com/gfx-labs/protocol7/pkg/execution/session"

// HostWorkspace is the directory that holds the workspace of a session on
// the host backend, for a core worker whose state directory is stateDir. A
// consumer process on the same machine, such as a file server beside the
// worker, reads what the agent wrote from here. It fails for an ID that is
// not a session ID, so the result is always inside stateDir.
func HostWorkspace(stateDir, sessionID string) (string, error) {
	return session.Workspace(stateDir, sessionID)
}
