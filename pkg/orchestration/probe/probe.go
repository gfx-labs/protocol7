// Package probe brokers bounded, labelled, credential-free Kubernetes probes.
package probe

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

const (
	Namespace      = "protocol7-selfdev-probes"
	Label          = "selfdev.gfx-labs/probe"
	ServiceAccount = "selfdev-probe"
	ActivityCreate = "selfdev.probe.Create"
	ActivityRead   = "selfdev.probe.Read"
	ActivityDelete = "selfdev.probe.Delete"
	TTL            = 10 * time.Minute
	tokenDir       = "/var/run/secrets/kubernetes.io/serviceaccount"
)

var chatID = regexp.MustCompile(`^[0-9a-hjkmnp-tv-z]{26}$`)
var objectName = regexp.MustCompile(`^probe-[a-z0-9-]{1,56}$`)

type Request struct {
	Chat    string            `json:"chat"`
	Name    string            `json:"name"`
	Kind    string            `json:"kind"`
	Image   string            `json:"image,omitempty"`
	Command []string          `json:"command,omitempty"`
	Data    map[string]string `json:"data,omitempty"`
	Port    int               `json:"port,omitempty"`
}

type metadata struct {
	Name        string            `json:"name"`
	Namespace   string            `json:"namespace"`
	UID         string            `json:"uid,omitempty"`
	Created     time.Time         `json:"creationTimestamp,omitempty"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type object struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Metadata   metadata          `json:"metadata"`
	Spec       map[string]any    `json:"spec,omitempty"`
	Data       map[string]string `json:"data,omitempty"`
	Status     json.RawMessage   `json:"status,omitempty"`
}

// Activities holds only the broker's own projected credential, never the agent's.
type Activities struct {
	namespace string
	endpoint  string
	token     string
	http      *http.Client
	temporal  client.Client
}

// New returns nil without a core broker token, including tenant deployments.
func New(c client.Client) (*Activities, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	if host == "" {
		return nil, nil
	}
	namespace, err := os.ReadFile(tokenDir + "/namespace")
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(namespace)) != "protocol7-selfdev" {
		return nil, nil
	}
	ca, err := os.ReadFile(tokenDir + "/ca.crt")
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("probe: empty API CA")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	return &Activities{namespace: Namespace, endpoint: "https://" + net.JoinHostPort(host, os.Getenv("KUBERNETES_SERVICE_PORT")), token: tokenDir + "/token", http: &http.Client{Transport: transport, Timeout: 20 * time.Second}, temporal: c}, nil
}

func resource(kind string) (string, error) {
	switch kind {
	case "Pod":
		return "/api/v1/namespaces/", nil
	case "Job":
		return "/apis/batch/v1/namespaces/", nil
	case "ConfigMap", "Service":
		return "/api/v1/namespaces/", nil
	default:
		return "", fmt.Errorf("kind must be Pod, Job, ConfigMap or Service")
	}
}
func plural(kind string) string {
	switch kind {
	case "Pod":
		return "pods"
	case "Job":
		return "jobs"
	case "ConfigMap":
		return "configmaps"
	case "Service":
		return "services"
	}
	return ""
}
func (a *Activities) path(kind string) string {
	prefix, _ := resource(kind)
	return prefix + a.namespace + "/" + plural(kind)
}

func (a *Activities) do(ctx context.Context, method, path string, body any) ([]byte, int, error) {
	if a == nil {
		return nil, 0, temporal.NewNonRetryableApplicationError("probes require the cluster broker", "ProbeUnavailable", nil)
	}
	var input io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
		input = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.endpoint+path, input)
	if err != nil {
		return nil, 0, err
	}
	token, err := os.ReadFile(a.token)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Content-Type", "application/json")
	res, err := a.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 2<<20))
	if err != nil {
		return nil, res.StatusCode, err
	}
	if res.StatusCode/100 != 2 {
		return raw, res.StatusCode, fmt.Errorf("probe API: %s: %.2048s", res.Status, raw)
	}
	return raw, res.StatusCode, nil
}

func valid(r Request) error {
	if !chatID.MatchString(r.Chat) || !objectName.MatchString(r.Name) || !strings.HasPrefix(r.Name, "probe-"+r.Chat+"-") {
		return errors.New("invalid chat-scoped probe name")
	}
	_, err := resource(r.Kind)
	return err
}

func build(r Request, namespace string) (object, error) {
	if err := valid(r); err != nil {
		return object{}, err
	}
	obj := object{APIVersion: "v1", Kind: r.Kind, Metadata: metadata{Name: r.Name, Namespace: namespace, Labels: map[string]string{Label: r.Chat}}}
	if r.Kind == "ConfigMap" {
		raw, _ := json.Marshal(r.Data)
		if len(raw) > 64<<10 {
			return obj, errors.New("ConfigMap data exceeds 64 KiB")
		}
		obj.Data = r.Data
	} else if r.Kind == "Service" {
		if r.Port < 1024 || r.Port > 65535 {
			return obj, errors.New("service port must be 1024 to 65535")
		}
		obj.Spec = map[string]any{"type": "ClusterIP", "selector": map[string]string{Label: r.Chat}, "ports": []any{map[string]any{"port": r.Port, "targetPort": r.Port}}}
	} else {
		if r.Image == "" || len(r.Image) > 512 || len(r.Command) > 64 {
			return obj, errors.New("image is required, command has at most 64 arguments")
		}
		pod := map[string]any{
			"serviceAccountName": ServiceAccount, "automountServiceAccountToken": false, "restartPolicy": "Never", "activeDeadlineSeconds": int(TTL.Seconds()), "terminationGracePeriodSeconds": 5,
			"securityContext": map[string]any{"runAsNonRoot": true, "runAsUser": 1000, "runAsGroup": 1000, "seccompProfile": map[string]any{"type": "RuntimeDefault"}},
			"containers": []any{map[string]any{"name": "probe", "image": r.Image, "command": r.Command,
				"securityContext": map[string]any{"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "capabilities": map[string]any{"drop": []string{"ALL"}}},
				"resources":       map[string]any{"requests": map[string]string{"cpu": "25m", "memory": "32Mi"}, "limits": map[string]string{"cpu": "500m", "memory": "128Mi"}},
			}},
		}
		if len(r.Command) == 0 {
			delete(pod["containers"].([]any)[0].(map[string]any), "command")
		}
		if r.Kind == "Pod" {
			obj.Spec = pod
		} else {
			obj.APIVersion = "batch/v1"
			obj.Spec = map[string]any{"backoffLimit": 0, "activeDeadlineSeconds": int(TTL.Seconds()), "ttlSecondsAfterFinished": 60, "template": map[string]any{"metadata": map[string]any{"labels": map[string]string{Label: r.Chat}}, "spec": pod}}
		}
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		return obj, err
	}
	obj.Metadata.Annotations = map[string]string{"selfdev.gfx-labs/probe-spec": fmt.Sprintf("%x", sha256.Sum256(raw))}
	return obj, nil
}

// Create is idempotent for the workflow-chosen name and exact specification.
func (a *Activities) Create(ctx context.Context, r Request) (string, error) {
	if a == nil {
		return "", temporal.NewNonRetryableApplicationError("probes require the cluster broker", "ProbeUnavailable", nil)
	}
	obj, err := build(r, a.namespace)
	if err != nil {
		return "", temporal.NewNonRetryableApplicationError(err.Error(), "ProbeInvalid", nil)
	}
	_, status, err := a.do(ctx, http.MethodPost, a.path(r.Kind), obj)
	if status == http.StatusConflict {
		raw, _, readErr := a.do(ctx, http.MethodGet, a.path(r.Kind)+"/"+r.Name, nil)
		if readErr != nil {
			return "", readErr
		}
		var existing object
		if err := json.Unmarshal(raw, &existing); err != nil {
			return "", err
		}
		if existing.Metadata.Labels[Label] == r.Chat && existing.Metadata.Annotations["selfdev.gfx-labs/probe-spec"] == obj.Metadata.Annotations["selfdev.gfx-labs/probe-spec"] {
			return r.Name, nil
		}
		return "", temporal.NewNonRetryableApplicationError("probe name collision", "ProbeConflict", nil)
	}
	return r.Name, err
}

func (a *Activities) owned(ctx context.Context, r Request) (object, error) {
	if err := valid(r); err != nil {
		return object{}, err
	}
	if a == nil {
		return object{}, errors.New("probes require the cluster broker")
	}
	raw, _, err := a.do(ctx, http.MethodGet, a.path(r.Kind)+"/"+r.Name, nil)
	if err != nil {
		return object{}, err
	}
	var obj object
	if err := json.Unmarshal(raw, &obj); err != nil {
		return obj, err
	}
	if obj.Metadata.Labels[Label] != r.Chat {
		return obj, errors.New("probe belongs to another chat")
	}
	return obj, nil
}

// Read returns only this chat's object's status and bounded Pod logs.
func (a *Activities) Read(ctx context.Context, r Request) (string, error) {
	obj, err := a.owned(ctx, r)
	if err != nil {
		return "", err
	}
	raw, _ := json.Marshal(obj)
	if r.Kind == "Pod" {
		logs, _, err := a.do(ctx, http.MethodGet, a.path(r.Kind)+"/"+r.Name+"/log?tailLines=100&limitBytes=16384", nil)
		if err == nil {
			return string(raw) + "\nLogs:\n" + string(logs), nil
		}
	}
	return string(raw), nil
}

func (a *Activities) remove(ctx context.Context, kind string, obj object) error {
	_, status, err := a.do(ctx, http.MethodDelete, a.path(kind)+"/"+obj.Metadata.Name, map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Background", "preconditions": map[string]string{"uid": obj.Metadata.UID}})
	if status == http.StatusNotFound {
		return nil
	}
	return err
}
func (a *Activities) Delete(ctx context.Context, r Request) error {
	obj, err := a.owned(ctx, r)
	if err != nil {
		return err
	}
	return a.remove(ctx, r.Kind, obj)
}

// Reap deletes expired objects or objects whose owning chat no longer runs.
// A Temporal lookup failure retains fresh objects, never their TTL exemption.
func (a *Activities) Reap(ctx context.Context) error {
	if a == nil {
		return nil
	}
	closed := map[string]bool{}
	var failures []error
	for _, kind := range []string{"Job", "Pod", "Service", "ConfigMap"} {
		next := ""
		for {
			path := a.path(kind) + "?limit=100&labelSelector=" + url.QueryEscape(Label) + "&continue=" + url.QueryEscape(next)
			raw, _, err := a.do(ctx, http.MethodGet, path, nil)
			if err != nil {
				failures = append(failures, err)
				break
			}
			var page struct {
				Items    []object `json:"items"`
				Metadata struct {
					Continue string `json:"continue"`
				} `json:"metadata"`
			}
			if err := json.Unmarshal(raw, &page); err != nil {
				failures = append(failures, err)
				break
			}
			for _, obj := range page.Items {
				id := obj.Metadata.Labels[Label]
				if !chatID.MatchString(id) {
					continue
				}
				expired := !obj.Metadata.Created.IsZero() && !obj.Metadata.Created.Add(TTL).After(time.Now())
				ended, known := closed[id]
				if !expired && !known && a.temporal != nil {
					info, err := a.temporal.DescribeWorkflowExecution(ctx, "chat-"+id, "")
					var missing *serviceerror.NotFound
					if errors.As(err, &missing) {
						ended = true
					} else if err == nil {
						ended = info.GetWorkflowExecutionInfo().GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
					} else {
						failures = append(failures, err)
					}
					if err == nil || ended {
						closed[id] = ended
					}
				}
				if expired || ended {
					if err := a.remove(ctx, kind, obj); err != nil {
						failures = append(failures, err)
					}
				}
			}
			next = page.Metadata.Continue
			if next == "" {
				break
			}
		}
	}
	return errors.Join(failures...)
}

// RunReaper is attached to the broker process lifecycle. Restarts do not reset TTL.
func (a *Activities) RunReaper(ctx context.Context, logger *slog.Logger) {
	if a == nil {
		return
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		sweep, cancel := context.WithTimeout(ctx, 25*time.Second)
		err := a.Reap(sweep)
		cancel()
		if err != nil && ctx.Err() == nil {
			logger.Warn("probe reaper", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
