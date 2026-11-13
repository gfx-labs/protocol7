package session

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/gfx-labs/protocol7/pkg/domain"
)

// materialize places one input in the workspace and returns what it resolved
// to. An input whose destination exists was placed by an earlier attempt.
func materialize(ctx context.Context, workspace string, input domain.Input) (domain.ResolvedInput, error) {
	resolved := domain.ResolvedInput{Destination: input.Destination, Repository: input.Repository, Ref: input.Ref}
	if input.Kind != domain.InputGitClone {
		return resolved, fmt.Errorf("input %q: kind %q is not implemented", input.Destination, input.Kind)
	}
	if input.Repository == "" {
		return resolved, fmt.Errorf("input %q: a git clone needs a repository", input.Destination)
	}
	destination, err := insideWorkspace(workspace, input.Destination)
	if err != nil {
		return resolved, err
	}

	if _, err := os.Stat(destination); err == nil {
		resolved.Commit, err = git(ctx, destination, "rev-parse", "HEAD")
		return resolved, err
	} else if !errors.Is(err, os.ErrNotExist) {
		return resolved, fmt.Errorf("input %q: %w", input.Destination, err)
	}

	// Clone beside the destination and rename, so a clone that was cut
	// short is never taken for a finished one.
	partial := destination + ".partial"
	if err := os.RemoveAll(partial); err != nil {
		return resolved, fmt.Errorf("input %q: remove unfinished clone: %w", input.Destination, err)
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return resolved, fmt.Errorf("input %q: %w", input.Destination, err)
	}
	if _, err := git(ctx, "", "clone", "--quiet", input.Repository, partial); err != nil {
		return resolved, fmt.Errorf("input %q: %w", input.Destination, err)
	}
	if input.Ref != "" {
		if _, err := git(ctx, partial, "fetch", "--quiet", "origin", input.Ref); err != nil {
			return resolved, fmt.Errorf("input %q: %w", input.Destination, err)
		}
		if _, err := git(ctx, partial, "checkout", "--quiet", "--detach", "FETCH_HEAD"); err != nil {
			return resolved, fmt.Errorf("input %q: %w", input.Destination, err)
		}
	}
	if input.Branch != "" {
		if _, err := git(ctx, partial, "checkout", "--quiet", "-B", input.Branch); err != nil {
			return resolved, fmt.Errorf("input %q: %w", input.Destination, err)
		}
	}
	if resolved.Commit, err = git(ctx, partial, "rev-parse", "HEAD"); err != nil {
		return resolved, fmt.Errorf("input %q: %w", input.Destination, err)
	}
	if err := os.Rename(partial, destination); err != nil {
		return resolved, fmt.Errorf("input %q: %w", input.Destination, err)
	}
	return resolved, nil
}

// insideWorkspace resolves a destination, which must be a relative path that
// stays inside the workspace.
func insideWorkspace(workspace, destination string) (string, error) {
	if destination == "" || filepath.IsAbs(destination) || !filepath.IsLocal(destination) {
		return "", fmt.Errorf("input destination %q: must be a relative path inside the workspace", destination)
	}
	return filepath.Join(workspace, destination), nil
}

// git runs one git command and returns its trimmed standard output.
func git(ctx context.Context, dir string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, "git", args...)
	command.Dir = dir
	// A clone that needs a password must fail, not wait for one.
	command.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	var stderr strings.Builder
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(string(output)), nil
}
