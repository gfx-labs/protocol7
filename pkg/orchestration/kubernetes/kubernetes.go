// Package kubernetes is the kubernetes backend: one pod per session. Its
// worker polls the queue p7-kubernetes and only creates and deletes pods. The
// session's activities then run in the session's pod, where `p7-worker
// session` runs the host session manager for that one session on its own
// queue.
//
// It calls the Kubernetes API with plain HTTP and the pod's service account
// token, because it needs three calls: create, read and delete a pod.
package kubernetes

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/snapshot"
)

// Labels and annotations every session pod carries.
const (
	// LabelSession holds the session ID. A ClusterSPIFFEID can render the
	// pod's identity from it, and only this worker sets it.
	LabelSession = "p7.gfx-labs.io/session"
	// LabelManaged marks a pod this backend created.
	LabelManaged = "p7.gfx-labs.io/managed"
	// AnnotationOwner holds the ID of the workflow that opened the session.
	AnnotationOwner = "p7.gfx-labs.io/owner"
	// EnvSession and EnvTaskQueue tell `p7-worker session` which session it
	// serves and which queue it polls.
	EnvSession   = "P7_SESSION"
	EnvTaskQueue = "P7_TASK_QUEUE"
)

// Config configures the backend.
type Config struct {
	// Namespace is where session pods are created.
	Namespace string
	// TemplateFile holds a pod manifest in JSON, the deployment's template
	// for every session pod. The worker sets its name, its labels, the owner
	// annotation and the two environment variables of the container named
	// SessionContainer. It is read once per pod, so a new template applies
	// to the next session.
	TemplateFile string
	// SessionContainer names the container that runs `p7-worker session`.
	SessionContainer string
	// APIServer is the Kubernetes API. Empty is the in-cluster address.
	APIServer string
	// TokenFile and CAFile are the service account's. Empty are the
	// in-cluster paths.
	TokenFile string
	CAFile    string
	// Snapshots is where the sessions' snapshots are kept, if they are.
	// Deleting a session's pod then also deletes its snapshot, which the
	// pod's worker only writes while the session is open.
	Snapshots snapshot.Config
}

const (
	serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"
)

// Pods creates and deletes session pods.
type Pods struct {
	config    Config
	http      *http.Client
	logger    *slog.Logger
	snapshots *snapshot.Store
}

// New returns the backend. It reads the CA file now and the token on each
// call, since the kubelet replaces it.
func New(config Config, logger *slog.Logger) (*Pods, error) {
	if config.Namespace == "" || config.TemplateFile == "" || config.SessionContainer == "" {
		return nil, errors.New("kubernetes backend: Namespace, TemplateFile and SessionContainer are required")
	}
	if config.APIServer == "" {
		host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
		if host == "" || port == "" {
			return nil, errors.New("kubernetes backend: not in a cluster, name the API server")
		}
		config.APIServer = "https://" + host + ":" + port
	}
	if config.TokenFile == "" {
		config.TokenFile = serviceAccountDir + "/token"
	}
	if config.CAFile == "" {
		config.CAFile = serviceAccountDir + "/ca.crt"
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// The API server is reached directly, never through the pod's egress
	// proxy.
	transport.Proxy = nil
	if strings.HasPrefix(config.APIServer, "https://") {
		ca, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("kubernetes backend: read the cluster CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("kubernetes backend: %s holds no certificate", config.CAFile)
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	pods := &Pods{config: config, http: &http.Client{Transport: transport, Timeout: 30 * time.Second}, logger: logger}
	if err := config.Snapshots.Validate(); err != nil {
		return nil, fmt.Errorf("kubernetes backend: %w", err)
	}
	if config.Snapshots.Enabled() {
		var err error
		if pods.snapshots, err = snapshot.New(config.Snapshots, logger); err != nil {
			return nil, fmt.Errorf("kubernetes backend: %w", err)
		}
	}
	return pods, nil
}

// PodName is the name of a session's pod.
func PodName(session string) string { return "p7-session-" + session }

// Register adds the backend's activities to a worker.
func (p *Pods) Register(w worker.Worker) {
	w.RegisterActivityWithOptions(p.Start, activity.RegisterOptions{Name: domain.ActivityStartSessionPod})
	w.RegisterActivityWithOptions(p.Delete, activity.RegisterOptions{Name: domain.ActivityDeleteSessionPod})
}

// NewWorker returns a Temporal worker on the backend's queue.
func NewWorker(temporalClient client.Client, pods *Pods) worker.Worker {
	w := worker.New(temporalClient, domain.KubernetesQueue, worker.Options{})
	pods.Register(w)
	return w
}

// Start creates a session's pod, unless it exists. It returns once the
// API server accepted it. The session's worker polls the returned queue once
// the pod runs.
func (p *Pods) Start(ctx context.Context, request domain.PodRequest) (domain.PodHandle, error) {
	handle := domain.PodHandle{Name: PodName(request.Session), TaskQueue: domain.SessionQueue(request.Session)}
	pod, err := p.render(request, handle)
	if err != nil {
		return handle, temporal.NewNonRetryableApplicationError(err.Error(), "PodTemplate", err)
	}
	err = p.do(ctx, http.MethodPost, p.podsPath(), pod, nil)
	switch {
	case err == nil:
		p.logger.Info("session pod created", "session", request.Session, "pod", handle.Name, "owner", request.Owner)
	case isStatus(err, http.StatusConflict):
		// An earlier attempt created it.
	default:
		return handle, err
	}
	return handle, nil
}

// Delete deletes a session's pod. A pod that is gone is not an error.
//
// With snapshots it waits until the pod is gone, which is after its worker
// has stopped, and then deletes the session's snapshot. A worker that is
// stopping writes snapshots of its open sessions, so the snapshot of a
// session that was not closed first would otherwise come back.
func (p *Pods) Delete(ctx context.Context, request domain.PodRequest) error {
	path := p.podsPath() + "/" + PodName(request.Session)
	err := p.do(ctx, http.MethodDelete, path, nil, nil)
	if err == nil {
		p.logger.Info("session pod deleted", "session", request.Session, "pod", PodName(request.Session))
	}
	if err != nil && !isStatus(err, http.StatusNotFound) {
		return err
	}
	if p.snapshots == nil {
		return nil
	}
	for err == nil {
		// The pod is still there until a read finds nothing.
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for the pod %s to end: %w", PodName(request.Session), ctx.Err())
		case <-time.After(podPoll):
		}
		err = p.do(ctx, http.MethodGet, path, nil, nil)
	}
	if !isStatus(err, http.StatusNotFound) {
		return err
	}
	return p.snapshots.Delete(ctx, request.Session)
}

// podPoll is how often Delete looks for a pod that is ending.
const podPoll = time.Second

func (p *Pods) podsPath() string { return "/api/v1/namespaces/" + p.config.Namespace + "/pods" }

// render makes a session's pod from the template.
func (p *Pods) render(request domain.PodRequest, handle domain.PodHandle) (map[string]any, error) {
	raw, err := os.ReadFile(p.config.TemplateFile)
	if err != nil {
		return nil, fmt.Errorf("read the session pod template: %w", err)
	}
	var pod map[string]any
	if err := json.Unmarshal(raw, &pod); err != nil {
		return nil, fmt.Errorf("the session pod template %s is not JSON: %w", p.config.TemplateFile, err)
	}
	pod["apiVersion"], pod["kind"] = "v1", "Pod"
	metadata := object(pod, "metadata")
	// Fields a template copied from a live pod could carry.
	for _, field := range []string{"generateName", "resourceVersion", "uid", "creationTimestamp", "ownerReferences"} {
		delete(metadata, field)
	}
	metadata["name"] = handle.Name
	metadata["namespace"] = p.config.Namespace
	labels := object(metadata, "labels")
	labels[LabelSession] = request.Session
	labels[LabelManaged] = "true"
	if request.Owner != "" {
		object(metadata, "annotations")[AnnotationOwner] = request.Owner
	}
	spec := object(pod, "spec")
	delete(pod, "status")
	containers, _ := spec["containers"].([]any)
	for _, entry := range containers {
		container, ok := entry.(map[string]any)
		if !ok || container["name"] != p.config.SessionContainer {
			continue
		}
		env, _ := container["env"].([]any)
		var kept []any
		for _, variable := range env {
			if named, ok := variable.(map[string]any); ok && (named["name"] == EnvSession || named["name"] == EnvTaskQueue) {
				continue
			}
			kept = append(kept, variable)
		}
		container["env"] = append(kept,
			map[string]any{"name": EnvSession, "value": request.Session},
			map[string]any{"name": EnvTaskQueue, "value": handle.TaskQueue},
		)
		return pod, nil
	}
	return nil, fmt.Errorf("the session pod template has no container named %q", p.config.SessionContainer)
}

func object(parent map[string]any, key string) map[string]any {
	if child, ok := parent[key].(map[string]any); ok {
		return child
	}
	child := map[string]any{}
	parent[key] = child
	return child
}

// statusError is a response that was not a success.
type statusError struct {
	code    int
	message string
}

func (e *statusError) Error() string {
	return fmt.Sprintf("the Kubernetes API answered %d: %s", e.code, e.message)
}

func isStatus(err error, code int) bool {
	var status *statusError
	return errors.As(err, &status) && status.code == code
}

func (p *Pods) do(ctx context.Context, method, path string, body, result any) error {
	token, err := os.ReadFile(p.config.TokenFile)
	if err != nil {
		return fmt.Errorf("read the service account token: %w", err)
	}
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, p.config.APIServer+path, reader)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := p.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	content, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return err
	}
	if response.StatusCode >= 300 {
		var failure struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(content, &failure)
		status := &statusError{code: response.StatusCode, message: failure.Message}
		// A request the API server refuses is refused on every retry.
		if response.StatusCode == http.StatusBadRequest || response.StatusCode == http.StatusForbidden ||
			response.StatusCode == http.StatusUnprocessableEntity || response.StatusCode == http.StatusUnauthorized {
			return temporal.NewNonRetryableApplicationError(status.Error(), "KubernetesRefused", status)
		}
		return status
	}
	if result != nil {
		return json.Unmarshal(content, result)
	}
	return nil
}
