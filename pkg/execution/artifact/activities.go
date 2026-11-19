package artifact

import (
	"context"
	"errors"
	"strings"

	"go.temporal.io/sdk/temporal"
)

// Activity names. They are served by the process beside the core worker
// (selfdev files), which holds the bucket's key and the session's workspace.
// The bytes go between the two and never through Temporal or the model.
const (
	ActivitySave = "artifact.Save"
	ActivityGet  = "artifact.Get"
	ActivityList = "artifact.List"
)

// Queue is the task queue of the process beside the core worker of a host.
func Queue(host string) string { return "selfdev-artifacts-" + host }

// SaveRequest asks for a file or directory of a session's workspace to be
// saved.
type SaveRequest struct {
	Session string `json:"session"`
	Project string `json:"project"`
	Path    string `json:"path"`
	Name    string `json:"name"`
}

// GetRequest asks for an artifact to be placed in a session's workspace. Ref
// or Name says which.
type GetRequest struct {
	Session string `json:"session"`
	Project string `json:"project"`
	Ref     string `json:"ref,omitempty"`
	Name    string `json:"name,omitempty"`
	Path    string `json:"path"`
}

// ListRequest asks for the names of a project that start with Prefix.
type ListRequest struct {
	Project string `json:"project"`
	Prefix  string `json:"prefix,omitempty"`
}

// ListResult is the answer to a ListRequest.
type ListResult struct {
	Entries []Entry `json:"entries"`
	// Truncated is set when there are more names than MaxList.
	Truncated bool `json:"truncated,omitempty"`
}

// Activities are the artifact activities.
type Activities struct {
	Store *Store
	// StateDir is the core worker's state directory, which holds the
	// sessions' workspaces.
	StateDir string
}

// Save uploads a file, or a directory as a zip, and points the name at it.
func (a Activities) Save(ctx context.Context, request SaveRequest) (SaveResult, error) {
	if err := a.ready(); err != nil {
		return SaveResult{}, err
	}
	root, dir, err := OpenWorkspace(a.StateDir, request.Session)
	if err != nil {
		return SaveResult{}, refuse(err)
	}
	defer root.Close()
	path, err := WorkspacePath(dir, request.Path)
	if err != nil {
		return SaveResult{}, refuse(err)
	}
	result, err := a.Store.SaveFrom(ctx, root, path, request.Project, request.Name, "session "+request.Session)
	return result, classify(err)
}

// Get places an artifact in the workspace.
func (a Activities) Get(ctx context.Context, request GetRequest) (GetResult, error) {
	if err := a.ready(); err != nil {
		return GetResult{}, err
	}
	root, dir, err := OpenWorkspace(a.StateDir, request.Session)
	if err != nil {
		return GetResult{}, refuse(err)
	}
	defer root.Close()
	path, err := WorkspacePath(dir, request.Path)
	if err != nil {
		return GetResult{}, refuse(err)
	}
	ref, err := a.resolve(ctx, request.Project, request.Ref, request.Name)
	if err != nil {
		return GetResult{}, classify(err)
	}
	result, err := a.Store.GetTo(ctx, root, path, ref)
	return result, classify(err)
}

// List names the artifacts of a project.
func (a Activities) List(ctx context.Context, request ListRequest) (ListResult, error) {
	if err := a.ready(); err != nil {
		return ListResult{}, err
	}
	entries, err := a.Store.List(ctx, request.Project, request.Prefix)
	if err != nil {
		return ListResult{}, classify(err)
	}
	return ListResult{Entries: entries, Truncated: len(entries) >= MaxList}, nil
}

func (a Activities) ready() error {
	if a.Store == nil || a.StateDir == "" {
		return temporal.NewNonRetryableApplicationError("artifacts are not set up on this host", "NotConfigured", nil)
	}
	return nil
}

// Resolve is the reference a ref or a name stands for. A ref that names no
// digest of its own, such as a bare name, is looked up.
func (a Activities) resolve(ctx context.Context, project, ref, name string) (Ref, error) {
	if strings.TrimSpace(ref) != "" {
		parsed, err := ParseRef(ref)
		if err != nil {
			return Ref{}, err
		}
		if _, err := a.Store.Stat(ctx, parsed.Digest); err != nil {
			return Ref{}, err
		}
		return parsed, nil
	}
	if strings.TrimSpace(name) == "" {
		return Ref{}, invalidf("give a ref or a name")
	}
	version, err := a.Store.Lookup(ctx, project, name)
	if err != nil {
		return Ref{}, err
	}
	return version.Ref(name), nil
}

// refuse is an error that no retry changes.
func refuse(err error) error {
	return temporal.NewNonRetryableApplicationError(err.Error(), "Refused", err)
}

// classify makes the errors that a retry cannot change final: a missing name,
// bytes beyond the limit, and an argument that is not valid. What is left,
// such as the bucket not answering, is retried.
func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrTooLarge) || errors.Is(err, ErrInvalid) {
		return refuse(err)
	}
	return err
}
