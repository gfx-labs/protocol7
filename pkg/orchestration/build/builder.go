package build

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ImageSpec is one image a builder builds: name:package:dockerfile[:target].
// It implements encoding.TextUnmarshaler so that a YAML list of strings in the
// builder's settings fills a list of them.
type ImageSpec struct {
	// Name is the image's name in the manifests' "# p7-image:" markers.
	Name string
	// Package is the repository path in zot, such as "oku-trade/oku-review".
	Package string
	// Dockerfile is the path of the Dockerfile from the repository root.
	Dockerfile string
	// Target is the Dockerfile stage to build, or empty for the last.
	Target string
}

// UnmarshalText parses name:package:dockerfile[:target].
func (spec *ImageSpec) UnmarshalText(text []byte) error {
	parsed, err := ParseImageSpec(string(text))
	if err != nil {
		return err
	}
	*spec = parsed
	return nil
}

// MarshalText writes name:package:dockerfile[:target], so that an ImageSpec
// round-trips through Temporal's JSON payloads.
func (spec ImageSpec) MarshalText() ([]byte, error) {
	text := spec.Name + ":" + spec.Package + ":" + spec.Dockerfile
	if spec.Target != "" {
		text += ":" + spec.Target
	}
	return []byte(text), nil
}

// ParseImageSpec parses name:package:dockerfile[:target].
func ParseImageSpec(text string) (ImageSpec, error) {
	parts := strings.Split(text, ":")
	if len(parts) < 3 || len(parts) > 4 || parts[0] == "" || parts[2] == "" || !packagePattern.MatchString(parts[1]) {
		return ImageSpec{}, fmt.Errorf("build: image %q is not name:package:dockerfile[:target]", text)
	}
	spec := ImageSpec{Name: parts[0], Package: parts[1], Dockerfile: parts[2]}
	if len(parts) == 4 {
		spec.Target = parts[3]
	}
	return spec, nil
}

// packagePattern is a lowercase repository path with at least one slash.
var packagePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*(?:/[a-z0-9][a-z0-9._-]*)+$`)

// Builder is the build.Build workflow for one repository. Its fields come
// from the builder's settings, never from a request.
type Builder struct {
	// Repository is owner/name. Requests for any other repository are refused.
	Repository string
	// Images are built in this order.
	Images []ImageSpec
	// BuildTimeout bounds one image's build.
	BuildTimeout time.Duration
}

// InputsRequest asks ActivityResolveInputs for a commit's inputs key.
type InputsRequest struct {
	Repository string
	Commit     string
}

// FindRequest asks ActivityFindByInputs whether every image already exists
// with the inputs tag.
type FindRequest struct {
	Inputs string
	Images []ImageSpec
}

// FindResult is the answer of ActivityFindByInputs. Images is empty unless
// every image was found.
type FindResult struct {
	Images []Image
}

// TagRequest asks ActivityTag to add each image's Tag to it. The images
// exist already.
type TagRequest struct {
	Images []Image
}

// SolveRequest asks ActivitySolve to build one image at a commit and push it
// to zot with every tag in Tags.
type SolveRequest struct {
	Repository string
	Commit     string
	Image      ImageSpec
	Tags       []string
}

// InputsTag is the zot tag that marks an image built from an inputs key:
// inputs-<key16>-<name>. The name keeps apart images that share a package.
func InputsTag(inputs, name string) string {
	if len(inputs) > 16 {
		inputs = inputs[:16]
	}
	return "inputs-" + inputs + "-" + name
}

// RequestTag is the tag a request's image gets: main-<commit12>-<name> or
// pr-<number>-<commit12>-<name>. The prefix comes first for zot's retention
// patterns, and the name keeps apart images that share a package.
func RequestTag(request Request, name string) string {
	short := request.Commit[:12]
	if request.Trigger == TriggerMain {
		return "main-" + short + "-" + name
	}
	return fmt.Sprintf("pr-%d-%s-%s", request.PullRequest, short, name)
}

// Workflow is the workflow registered as WorkflowName.
func (b *Builder) Workflow(ctx workflow.Context, request Request) (Result, error) {
	started := workflow.Now(ctx)
	if err := request.Validate(); err != nil {
		return Result{}, temporal.NewNonRetryableApplicationError(err.Error(), ErrorNotAllowed, err)
	}
	if !strings.EqualFold(request.Repository, b.Repository) {
		err := fmt.Errorf("build: this builder builds %s, not %s", b.Repository, request.Repository)
		return Result{}, temporal.NewNonRetryableApplicationError(err.Error(), ErrorNotAllowed, err)
	}
	if len(b.Images) == 0 {
		return Result{}, errors.New("build: the builder has no images configured")
	}
	quick := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
	})
	var inputs string
	if err := workflow.ExecuteActivity(quick, ActivityResolveInputs, InputsRequest{Repository: request.Repository, Commit: request.Commit}).Get(ctx, &inputs); err != nil {
		return Result{}, err
	}
	result := Result{Commit: request.Commit, Inputs: inputs}

	var found FindResult
	if err := workflow.ExecuteActivity(quick, ActivityFindByInputs, FindRequest{Inputs: inputs, Images: b.Images}).Get(ctx, &found); err != nil {
		return Result{}, err
	}
	if len(found.Images) == len(b.Images) {
		for _, image := range found.Images {
			image.Tag = RequestTag(request, image.Name)
			result.Images = append(result.Images, image)
		}
		if err := workflow.ExecuteActivity(quick, ActivityTag, TagRequest{Images: result.Images}).Get(ctx, nil); err != nil {
			return Result{}, err
		}
		result.Reused = true
		result.Duration = workflow.Now(ctx).Sub(started)
		return result, nil
	}

	timeout := b.BuildTimeout
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	solve := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		// Builds queue behind each other in the activity, so the wait counts.
		ScheduleToCloseTimeout: 4 * timeout,
		StartToCloseTimeout:    timeout,
		HeartbeatTimeout:       2 * time.Minute,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 3, NonRetryableErrorTypes: []string{ErrorBuildFailed, ErrorNotAllowed}},
	})
	for _, spec := range b.Images {
		var image Image
		request := SolveRequest{Repository: request.Repository, Commit: request.Commit, Image: spec, Tags: []string{RequestTag(request, spec.Name), InputsTag(inputs, spec.Name)}}
		if err := workflow.ExecuteActivity(solve, ActivitySolve, request).Get(ctx, &image); err != nil {
			return Result{}, err
		}
		result.Images = append(result.Images, image)
	}
	result.Duration = workflow.Now(ctx).Sub(started)
	return result, nil
}
