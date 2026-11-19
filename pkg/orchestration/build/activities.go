package build

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// Source reads a repository at a commit through PDP. The builder's
// implementation uses the tenant's GitHub binding, read only.
type Source interface {
	// Tree lists every path of the commit with its git object ID.
	Tree(ctx context.Context, repository, commit string) (map[string]string, error)
	// Checkout writes the commit's files into directory.
	Checkout(ctx context.Context, repository, commit, directory string) error
}

// Registry is the part of zot the activities use.
type Registry interface {
	// Digest returns the digest of package:tag, or "" when it does not exist.
	Digest(ctx context.Context, pkg, tag string) (string, error)
	// Tag points package:tag at an existing digest.
	Tag(ctx context.Context, pkg, digest, tag string) error
}

// Solver builds one image from a checked out context and pushes it to zot
// under every reference in refs. It returns the manifest digest. A failed
// Dockerfile step is a *StepError.
type Solver interface {
	Solve(ctx context.Context, job SolveJob, progress func(line string)) (string, error)
}

// SolveJob is one image build on buildkitd.
type SolveJob struct {
	// Context is the checked out repository.
	Context    string
	Dockerfile string
	Target     string
	// Refs are full references in zot, host:port/package:tag.
	Refs []string
}

// StepError is a build that failed in a Dockerfile step, as opposed to an
// outage. Log holds its last lines.
type StepError struct {
	Log string
	Err error
}

func (e *StepError) Error() string { return fmt.Sprintf("build step failed: %v\n%s", e.Err, e.Log) }
func (e *StepError) Unwrap() error { return e.Err }

// Activities are the builder's activities. Register them with RegisterOn.
type Activities struct {
	Repository string
	// Inputs are the paths whose contents decide the images.
	Inputs []string
	// RegistryHost is host:port of zot, used in references given to Solver.
	RegistryHost string
	// Packages are the paths this builder may push to.
	Packages []string
	// WorkDir holds checkouts. Each solve uses and removes a directory in it.
	WorkDir  string
	Source   Source
	Registry Registry
	Solver   Solver
}

// Register is what a worker offers to register activities by name.
type Register interface {
	RegisterActivity(name string, function any)
}

// RegisterOn registers the activities under the contract's names.
func (a *Activities) RegisterOn(worker Register) {
	worker.RegisterActivity(ActivityResolveInputs, a.ResolveInputs)
	worker.RegisterActivity(ActivityFindByInputs, a.FindByInputs)
	worker.RegisterActivity(ActivityTag, a.Tag)
	worker.RegisterActivity(ActivitySolve, a.Solve)
}

func (a *Activities) allowed(repository, pkg string) error {
	if !strings.EqualFold(repository, a.Repository) {
		err := fmt.Errorf("build: this builder builds %s, not %s", a.Repository, repository)
		return temporal.NewNonRetryableApplicationError(err.Error(), ErrorNotAllowed, err)
	}
	if pkg == "" {
		return nil
	}
	for _, allowed := range a.Packages {
		if pkg == allowed || strings.HasPrefix(pkg, allowed+"/") {
			return nil
		}
	}
	err := fmt.Errorf("build: package %s is not this builder's", pkg)
	return temporal.NewNonRetryableApplicationError(err.Error(), ErrorNotAllowed, err)
}

// InputsKey is the SHA-256 of the sorted (path, object ID) pairs of the tree
// entries under the input paths.
func InputsKey(tree map[string]string, inputs []string) string {
	var lines []string
	for file, object := range tree {
		for _, input := range inputs {
			input = strings.Trim(path.Clean(input), "/")
			if file == input || strings.HasPrefix(file, input+"/") {
				lines = append(lines, file+"\x00"+object)
				break
			}
		}
	}
	sort.Strings(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return hex.EncodeToString(sum[:])
}

// ResolveInputs returns the commit's inputs key.
func (a *Activities) ResolveInputs(ctx context.Context, request InputsRequest) (string, error) {
	if err := a.allowed(request.Repository, ""); err != nil {
		return "", err
	}
	tree, err := a.Source.Tree(ctx, request.Repository, request.Commit)
	if err != nil {
		return "", err
	}
	return InputsKey(tree, a.Inputs), nil
}

// FindByInputs returns every image when all of them carry the inputs tag.
func (a *Activities) FindByInputs(ctx context.Context, request FindRequest) (FindResult, error) {
	var images []Image
	for _, spec := range request.Images {
		if err := a.allowed(a.Repository, spec.Package); err != nil {
			return FindResult{}, err
		}
		digest, err := a.Registry.Digest(ctx, spec.Package, InputsTag(request.Inputs, spec.Name))
		if err != nil {
			return FindResult{}, err
		}
		if digest == "" {
			return FindResult{}, nil
		}
		images = append(images, Image{Name: spec.Name, Package: spec.Package, Digest: digest, Tag: InputsTag(request.Inputs, spec.Name)})
	}
	return FindResult{Images: images}, nil
}

// Tag adds each image's Tag to images found by FindByInputs.
func (a *Activities) Tag(ctx context.Context, request TagRequest) error {
	for _, image := range request.Images {
		if err := a.allowed(a.Repository, image.Package); err != nil {
			return err
		}
		if err := a.Registry.Tag(ctx, image.Package, image.Digest, image.Tag); err != nil {
			return err
		}
	}
	return nil
}

// Solve checks out the commit, builds one image on buildkitd and pushes it.
func (a *Activities) Solve(ctx context.Context, request SolveRequest) (Image, error) {
	if err := a.allowed(request.Repository, request.Image.Package); err != nil {
		return Image{}, err
	}
	if !commitPattern.MatchString(request.Commit) || len(request.Tags) == 0 {
		err := errors.New("build: a solve needs a full commit and a tag")
		return Image{}, temporal.NewNonRetryableApplicationError(err.Error(), ErrorNotAllowed, err)
	}
	if err := os.MkdirAll(a.WorkDir, 0o700); err != nil {
		return Image{}, err
	}
	directory, err := os.MkdirTemp(a.WorkDir, "checkout-")
	if err != nil {
		return Image{}, err
	}
	defer os.RemoveAll(directory)
	if err := a.Source.Checkout(ctx, request.Repository, request.Commit, directory); err != nil {
		return Image{}, err
	}
	job := SolveJob{Context: directory, Dockerfile: request.Image.Dockerfile, Target: request.Image.Target}
	for _, tag := range request.Tags {
		job.Refs = append(job.Refs, a.RegistryHost+"/"+request.Image.Package+":"+tag)
	}
	digest, err := a.Solver.Solve(ctx, job, func(line string) { activity.RecordHeartbeat(ctx, line) })
	var step *StepError
	if errors.As(err, &step) {
		return Image{}, temporal.NewNonRetryableApplicationError(step.Error(), ErrorBuildFailed, err)
	}
	if err != nil {
		return Image{}, err
	}
	return Image{Name: request.Image.Name, Package: request.Image.Package, Digest: digest, Tag: request.Tags[0]}, nil
}

// GitSource reads GitHub through PDP: the trees API for Tree and git for
// Checkout. Placeholder is the tenant binding's placeholder, which PDP swaps
// for a read token by the builder's identity. Git reaches PDP through the
// proxy in its environment.
type GitSource struct {
	// Tree lists the commit's tree, recursively.
	ListTree func(ctx context.Context, repository, commit string) (map[string]string, error)
	// Placeholder is sent as the Basic password to github.com.
	Placeholder string
	// Env is the environment git runs with: the proxy and the CA bundle.
	Env []string
}

// Tree calls ListTree.
func (s GitSource) Tree(ctx context.Context, repository, commit string) (map[string]string, error) {
	return s.ListTree(ctx, repository, commit)
}

// Checkout fetches exactly the commit, without history, and checks it out.
func (s GitSource) Checkout(ctx context.Context, repository, commit, directory string) error {
	remote := (&url.URL{Scheme: "https", Host: "github.com", Path: "/" + repository + ".git", User: url.UserPassword("x-access-token", s.Placeholder)}).String()
	for _, args := range [][]string{
		{"init", "--quiet", directory},
		{"-C", directory, "fetch", "--quiet", "--depth=1", remote, commit},
		{"-C", directory, "checkout", "--quiet", "--detach", "FETCH_HEAD"},
	} {
		command := exec.CommandContext(ctx, "git", args...)
		command.Env = append(append([]string{}, s.Env...), "GIT_TERMINAL_PROMPT=0", "HOME="+filepath.Dir(directory))
		output, err := command.CombinedOutput()
		if err != nil {
			// The remote URL carries only the placeholder, never a token.
			return fmt.Errorf("git %s: %w: %s", args[len(args)-2], err, strings.TrimSpace(string(output)))
		}
	}
	return os.RemoveAll(filepath.Join(directory, ".git"))
}
