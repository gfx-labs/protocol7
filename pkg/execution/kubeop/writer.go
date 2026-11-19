package kubeop

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.temporal.io/sdk/temporal"

	"github.com/gfx-labs/protocol7/pkg/execution/artifact"
)

// Writer places credentials in a session's workspace. It runs beside the core
// worker, which holds the workspaces.
type Writer struct {
	// StateDir is the core worker's state directory.
	StateDir string
}

// Write puts the token, the CA and a kubeconfig in the workspace's Dir. The
// kubeconfig reads the token from its file, so a renewal replaces one file and
// a kubectl that runs meanwhile sees the new token. Each file is replaced
// whole, so a reader never sees half of one.
func (w Writer) Write(ctx context.Context, request WriteRequest) (WriteResult, error) {
	credentials := request.Credentials
	switch {
	case w.StateDir == "":
		return WriteResult{}, temporal.NewNonRetryableApplicationError("the operator credential is not set up on this host", "NotConfigured", nil)
	case credentials.Token == "" || strings.ContainsAny(credentials.Token, " \r\n\t"):
		return WriteResult{}, temporal.NewNonRetryableApplicationError("the credential has no usable token", ErrorRefused, nil)
	case !strings.HasPrefix(credentials.Server, "https://") || strings.ContainsAny(credentials.Server, " \r\n\t\"'"):
		return WriteResult{}, temporal.NewNonRetryableApplicationError("the credential names no HTTPS server", ErrorRefused, nil)
	case !strings.Contains(credentials.CA, "BEGIN CERTIFICATE"):
		return WriteResult{}, temporal.NewNonRetryableApplicationError("the credential has no CA", ErrorRefused, nil)
	}
	root, dir, err := artifact.OpenWorkspace(w.StateDir, request.Session)
	if err != nil {
		return WriteResult{}, temporal.NewNonRetryableApplicationError(err.Error(), ErrorRefused, err)
	}
	defer root.Close()
	if err := root.MkdirAll(Dir, 0o700); err != nil {
		return WriteResult{}, fmt.Errorf("make %s: %w", Dir, err)
	}
	tokenFile := filepath.Join(dir, Dir, "token")
	caFile := filepath.Join(dir, Dir, "ca.crt")
	kubeconfig := filepath.Join(dir, Dir, "kubeconfig")
	files := []struct{ name, content string }{
		{"ca.crt", credentials.CA},
		{"token", credentials.Token},
		{"kubeconfig", Kubeconfig(credentials.Server, caFile, tokenFile)},
	}
	for _, file := range files {
		temporary := filepath.Join(Dir, file.name+".tmp")
		out, err := root.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			return WriteResult{}, fmt.Errorf("write %s: %w", file.name, err)
		}
		_, err = out.WriteString(file.content)
		if closeErr := out.Close(); err == nil {
			err = closeErr
		}
		if err == nil {
			err = root.Rename(temporary, filepath.Join(Dir, file.name))
		}
		if err != nil {
			_ = root.Remove(temporary)
			return WriteResult{}, fmt.Errorf("write %s: %w", file.name, err)
		}
	}
	return WriteResult{Kubeconfig: kubeconfig, TokenFile: tokenFile}, nil
}

// Kubeconfig is a kubeconfig with one cluster, user and context, all named
// Context, that reads the CA and the token from files.
func Kubeconfig(server, caFile, tokenFile string) string {
	quote := func(s string) string { return fmt.Sprintf("%q", s) }
	return strings.Join([]string{
		"apiVersion: v1",
		"kind: Config",
		"clusters:",
		"- name: " + Context,
		"  cluster:",
		"    server: " + quote(server),
		"    certificate-authority: " + quote(caFile),
		"users:",
		"- name: " + Context,
		"  user:",
		"    tokenFile: " + quote(tokenFile),
		"contexts:",
		"- name: " + Context,
		"  context:",
		"    cluster: " + Context,
		"    user: " + Context,
		"current-context: " + Context,
		"",
	}, "\n")
}
