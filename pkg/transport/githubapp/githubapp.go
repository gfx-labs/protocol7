// Package githubapp keeps a short-lived GitHub App installation token in a
// file.
//
// The app's private key can act on every repository the app is installed on.
// A chat's agent runs with the rights of the container it is in, so the key
// must not be in that container. This package runs in a container of its
// own, which holds the key and writes a token limited to the chat's
// repositories into a volume that the agent's container reads.
package githubapp

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Config configures a Refresher.
type Config struct {
	// AppID is the GitHub App's ID.
	AppID string
	// InstallationID is the ID of the app's installation on the organisation.
	InstallationID string
	// PrivateKeyFile is the app's private key, in PEM.
	PrivateKeyFile string
	// Repositories are the names, without the owner, that a token may act on.
	// GitHub refuses the request if the installation does not cover one.
	Repositories []string
	// TokenFile is where the token is written. Its directory must exist.
	TokenFile string
	// Permissions are the rights the token is limited to, such as
	// "contents": "write". Empty is what a chat needs (see permissions).
	// GitHub refuses a permission that the app was not granted.
	Permissions map[string]string
}

// permissions are what a chat needs: push a branch, open a pull request, and
// open, comment on and label issues.
var permissions = map[string]string{"contents": "write", "pull_requests": "write", "issues": "write", "metadata": "read"}

const (
	// A token lasts one hour. Half of that leaves room for failed attempts.
	refreshEvery = 30 * time.Minute
	retryEvery   = 30 * time.Second
	apiURL       = "https://api.github.com"
)

// Refresher writes a token to a file and replaces it before it expires.
type Refresher struct {
	config Config
	key    *rsa.PrivateKey
	logger *slog.Logger
	http   *http.Client

	stop context.CancelFunc
	done sync.WaitGroup
}

// New reads the private key. It does not contact GitHub until Start.
func New(config Config, logger *slog.Logger) (*Refresher, error) {
	if config.AppID == "" || config.InstallationID == "" {
		return nil, errors.New("githubapp: an app ID and an installation ID are required")
	}
	if len(config.Repositories) == 0 {
		return nil, errors.New("githubapp: at least one repository is required, so that a token is not valid for every repository of the installation")
	}
	if config.TokenFile == "" {
		return nil, errors.New("githubapp: a token file is required")
	}
	key, err := readKey(config.PrivateKeyFile)
	if err != nil {
		return nil, err
	}
	return &Refresher{config: config, key: key, logger: logger, http: &http.Client{Timeout: 30 * time.Second}}, nil
}

func readKey(path string) (*rsa.PrivateKey, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("githubapp: read the private key: %w", err)
	}
	block, _ := pem.Decode(content)
	if block == nil {
		return nil, fmt.Errorf("githubapp: %s holds no PEM block", path)
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("githubapp: parse the private key in %s: %w", path, err)
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("githubapp: the private key in %s is not an RSA key", path)
	}
	return key, nil
}

// Start begins refreshing in the background. A failed request is logged and
// tried again, because GitHub being unreachable should not end the process.
func (r *Refresher) Start() {
	ctx, stop := context.WithCancel(context.Background())
	r.stop = stop
	r.done.Add(1)
	go func() {
		defer r.done.Done()
		for {
			wait := refreshEvery
			expires, err := r.refresh(ctx)
			if err != nil {
				r.logger.Error("no installation token", "error", err)
				wait = retryEvery
			} else {
				r.logger.Info("installation token written", "file", r.config.TokenFile, "expires", expires.Format(time.RFC3339), "repositories", r.config.Repositories)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
		}
	}()
}

// Stop ends the refreshing. The last token stays in the file until it expires.
func (r *Refresher) Stop() {
	if r.stop == nil {
		return
	}
	r.stop()
	r.done.Wait()
}

func (r *Refresher) refresh(ctx context.Context) (time.Time, error) {
	assertion, err := r.assertion(time.Now())
	if err != nil {
		return time.Time{}, err
	}
	scope := permissions
	if len(r.config.Permissions) > 0 {
		scope = r.config.Permissions
	}
	body, err := json.Marshal(map[string]any{"repositories": r.config.Repositories, "permissions": scope})
	if err != nil {
		return time.Time{}, err
	}
	url := apiURL + "/app/installations/" + r.config.InstallationID + "/access_tokens"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return time.Time{}, err
	}
	request.Header.Set("Authorization", "Bearer "+assertion)
	request.Header.Set("Accept", "application/vnd.github+json")
	response, err := r.http.Do(request)
	if err != nil {
		return time.Time{}, fmt.Errorf("request an installation token: %w", err)
	}
	defer response.Body.Close()
	answer, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return time.Time{}, fmt.Errorf("read GitHub's answer: %w", err)
	}
	if response.StatusCode != http.StatusCreated {
		// GitHub's error body names the cause and holds no credential.
		return time.Time{}, fmt.Errorf("GitHub answered %s: %s", response.Status, bytes.TrimSpace(answer))
	}
	var granted struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.Unmarshal(answer, &granted); err != nil || granted.Token == "" {
		return time.Time{}, errors.New("GitHub's answer holds no token")
	}
	return granted.ExpiresAt, r.write(granted.Token)
}

// assertion is the signed statement that identifies the app to GitHub.
func (r *Refresher) assertion(now time.Time) (string, error) {
	encode := base64.RawURLEncoding.EncodeToString
	claims, err := json.Marshal(map[string]any{
		// Issued a minute ago, to allow for a clock that runs ahead of GitHub's.
		"iat": now.Add(-time.Minute).Unix(),
		"exp": now.Add(8 * time.Minute).Unix(),
		"iss": r.config.AppID,
	})
	if err != nil {
		return "", err
	}
	signed := encode([]byte(`{"alg":"RS256","typ":"JWT"}`)) + "." + encode(claims)
	digest := sha256.Sum256([]byte(signed))
	signature, err := rsa.SignPKCS1v15(rand.Reader, r.key, crypto.SHA256, digest[:])
	if err != nil {
		return "", fmt.Errorf("sign the app assertion: %w", err)
	}
	return signed + "." + encode(signature), nil
}

// write replaces the token file in one step, so a reader never sees half of
// a token. The group may read it: the agent's container shares the volume's
// group and not this container's user.
func (r *Refresher) write(token string) error {
	temporary, err := os.CreateTemp(filepath.Dir(r.config.TokenFile), ".token-*")
	if err != nil {
		return fmt.Errorf("write the token: %w", err)
	}
	defer os.Remove(temporary.Name())
	if _, err := temporary.WriteString(token); err != nil {
		temporary.Close()
		return fmt.Errorf("write the token: %w", err)
	}
	if err := temporary.Chmod(0o640); err != nil {
		temporary.Close()
		return fmt.Errorf("write the token: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("write the token: %w", err)
	}
	if err := os.Rename(temporary.Name(), r.config.TokenFile); err != nil {
		return fmt.Errorf("write the token: %w", err)
	}
	return nil
}
