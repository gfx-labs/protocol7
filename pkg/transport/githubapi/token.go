package githubapi

import (
	"fmt"
	"os"
	"strings"
)

// PlaceholderPrefix starts a token spec that names a placeholder instead of a
// file: "placeholder:pdp-proxied-github-chat".
const PlaceholderPrefix = "placeholder:"

// TokenSource says what the client sends as its bearer token, on each call.
type TokenSource interface {
	// Token returns the credential to send. It is called for every request, so
	// a source that is replaced while the process runs is read afresh.
	Token() (string, error)
}

// FileToken reads an installation token from a file that another process
// replaces before it expires (selfdev github-token). It is how the selfdev
// Deployment and the p7-worker pods hold a token.
type FileToken string

// Token reads the file.
func (f FileToken) Token() (string, error) {
	token, err := os.ReadFile(string(f))
	if err != nil {
		return "", fmt.Errorf("read the GitHub token: %w", err)
	}
	return strings.TrimSpace(string(token)), nil
}

// PlaceholderToken is a fixed, non-secret string that PDP replaces with the
// installation token, by the identity of the pod that sends it (the github-chat
// binding, kubernetes/pdp). The process holds no token, and the request must
// go through PDP: HTTPS_PROXY names it, and the proxy's CA is trusted.
type PlaceholderToken string

// Token returns the placeholder.
func (p PlaceholderToken) Token() (string, error) {
	if strings.TrimSpace(string(p)) == "" {
		return "", fmt.Errorf("the GitHub token placeholder is empty")
	}
	return string(p), nil
}

// ParseTokenSource reads what a --github-token-file flag holds: a file's path,
// or "placeholder:<value>" for a placeholder that PDP swaps for a token. A
// path never starts with the prefix, since it is absolute or begins with a
// directory name followed by a slash.
func ParseTokenSource(spec string) TokenSource {
	if value, ok := strings.CutPrefix(spec, PlaceholderPrefix); ok {
		return PlaceholderToken(strings.TrimSpace(value))
	}
	return FileToken(spec)
}
