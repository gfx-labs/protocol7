package chat

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/execution/artifact"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// artifactActivity runs the artifact activities on the queue of the process
// beside the core worker, which holds the workspace and the bucket's key. The
// bytes go between the two. A host where that process does not run has no one
// polling the queue, and the call fails after half a minute and says so.
func artifactActivity(host string) workflow.ActivityOptions {
	return workflow.ActivityOptions{
		TaskQueue:              artifact.Queue(host),
		ScheduleToStartTimeout: 30 * time.Second,
		StartToCloseTimeout:    5 * time.Minute,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 3},
	}
}

// CoreProject is the navi name of the project that an empty Request.Project
// means: the platform's own, protocol7-selfdev.
const CoreProject = "selfdev"

// artifactProject is the project whose names a chat's artifacts live under.
func artifactProject(request Request) string {
	if request.Project != "" {
		return request.Project
	}
	return CoreProject
}

// artifactTools let the agent keep files out of git and share them across
// chats and jobs. Only the core project's chats on the host backend have them:
// the process that serves them runs beside the core worker.
func artifactTools(state *State) []p7.Tool {
	request := state.Request
	if request.Project != "" || request.Backend == p7.BackendKubernetes || request.Host == "" {
		return nil
	}
	project := artifactProject(request)
	run := func(ctx workflow.Context, activity string, input, result any) error {
		activityCtx := workflow.WithActivityOptions(ctx, artifactActivity(request.Host))
		if err := workflow.ExecuteActivity(activityCtx, activity, input).Get(ctx, result); err != nil {
			if timeout, ok := errors.AsType[*temporal.TimeoutError](err); ok && timeout.TimeoutType() == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START {
				return fmt.Errorf("artifacts are not available: nothing on this host answered within 30 seconds")
			}
			return fmt.Errorf("%s", cause(err))
		}
		return nil
	}
	save := p7.Tool{
		Name: "artifact_save",
		Description: `Save a file, or a directory as a zip, outside git so that other chats and jobs can read it: mockups, reference code, screenshots, reports. Returns a reference, p7://sha256:<hex>#<name>. Put the reference in messages, issues and pull request bodies, never the bytes.
The name is for finding it later, with parts separated by '/', such as ui-redesign/chats.png. Saving under a name again moves the name to the new bytes and keeps the old reference readable. The newest version of a name is kept; others are removed after 90 days. At most 256 MiB.`,
		InputSchema: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string","description":"The file or directory in your workspace, such as protocol7-selfdev/docs/ui-redesign/chats.png."},"name":{"type":"string","description":"The name to save it under, such as ui-redesign/chats.png."}},"required":["path","name"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct {
				Path string `json:"path"`
				Name string `json:"name"`
			}
			if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
				return p7.ToolResult{}, fmt.Errorf("want {\"path\": ..., \"name\": ...}: %w", err)
			}
			var result artifact.SaveResult
			if err := run(ctx, artifact.ActivitySave, artifact.SaveRequest{Session: call.Session, Project: project, Path: arguments.Path, Name: arguments.Name}, &result); err != nil {
				return p7.ToolResult{}, err
			}
			kind := "file"
			if result.Dir {
				kind = "directory, as a zip"
			}
			return p7.TextResult(fmt.Sprintf("Saved %s (%s, %s) as %s", arguments.Path, kind, byteSize(result.Size), result.Ref)), nil
		},
	}
	get := p7.Tool{
		Name:        "artifact_get",
		Description: "Place an artifact in your workspace. Give its reference (p7://sha256:<hex>, as someone saved it) or its name, and the path to put it at. A directory that was saved as a zip is unpacked under the path. A name gives its newest version. A file goes to the path, or into it when that is a directory.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"ref":{"type":"string","description":"A reference, p7://sha256:<hex>."},"name":{"type":"string","description":"A name, for its newest version. Used when there is no ref."},"path":{"type":"string","description":"Where to put it in your workspace."}},"required":["path"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct {
				Ref  string `json:"ref"`
				Name string `json:"name"`
				Path string `json:"path"`
			}
			if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
				return p7.ToolResult{}, fmt.Errorf("want {\"ref\": ..., \"path\": ...}: %w", err)
			}
			var result artifact.GetResult
			if err := run(ctx, artifact.ActivityGet, artifact.GetRequest{Session: call.Session, Project: project, Ref: arguments.Ref, Name: arguments.Name, Path: arguments.Path}, &result); err != nil {
				return p7.ToolResult{}, err
			}
			if result.Dir {
				return p7.TextResult(fmt.Sprintf("Unpacked %s into %s: %d files, %s", result.Ref, result.Path, result.Files, byteSize(result.Size))), nil
			}
			return p7.TextResult(fmt.Sprintf("Wrote %s to %s (%s)", result.Ref, result.Path, byteSize(result.Size))), nil
		},
	}
	list := p7.Tool{
		Name:        "artifact_list",
		Description: "List the saved artifacts of this project: each name with its newest reference, size, age and whether it is pinned. Give a prefix to list the names under it, such as ui-redesign/.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"prefix":{"type":"string","description":"Only names that start with this."}},"additionalProperties":false}`),
		Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct {
				Prefix string `json:"prefix"`
			}
			if len(call.Arguments) > 0 {
				if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
					return p7.ToolResult{}, fmt.Errorf("want {\"prefix\": ...}: %w", err)
				}
			}
			var result artifact.ListResult
			if err := run(ctx, artifact.ActivityList, artifact.ListRequest{Project: project, Prefix: arguments.Prefix}, &result); err != nil {
				return p7.ToolResult{}, err
			}
			return p7.TextResult(describeArtifacts(result, workflow.Now(ctx))), nil
		},
	}
	return []p7.Tool{save, get, list}
}

// describeArtifacts is a list as the agent is told about it.
func describeArtifacts(result artifact.ListResult, now time.Time) string {
	if len(result.Entries) == 0 {
		return "No artifacts."
	}
	var text strings.Builder
	for _, entry := range result.Entries {
		kept := entry.Kept
		fmt.Fprintf(&text, "%s  %s  %s  %s old  %s", entry.Name, artifact.Ref{Digest: entry.Digest, Name: entry.Name}, byteSize(entry.Size), age(now.Sub(entry.Saved)), kept)
		if entry.Versions > 1 {
			fmt.Fprintf(&text, "  (%d versions)", entry.Versions)
		}
		text.WriteByte('\n')
	}
	if result.Truncated {
		text.WriteString("The list is cut: give a longer prefix.\n")
	}
	return strings.TrimRight(text.String(), "\n")
}

func byteSize(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d bytes", n)
}

func age(d time.Duration) string {
	switch {
	case d >= 48*time.Hour:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	case d >= time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dm", max(int(d.Minutes()), 0))
}
