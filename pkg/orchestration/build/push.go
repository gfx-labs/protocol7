package build

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// PushEvent is the part of a GitHub push delivery the builder reads.
type PushEvent struct {
	Ref        string `json:"ref"`
	After      string `json:"after"`
	Deleted    bool   `json:"deleted"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Commits []struct {
		Added    []string `json:"added"`
		Removed  []string `json:"removed"`
		Modified []string `json:"modified"`
	} `json:"commits"`
}

// Starter starts a workflow. client.Client satisfies it.
type Starter interface {
	ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow any, args ...any) (client.WorkflowRun, error)
}

// PushHandler turns pushes to the configured branch into main builds. Main
// builds only warm the cache and make digests available in zot. Nothing is
// committed.
type PushHandler struct {
	Repository string
	Branch     string
	Inputs     []string
	Temporal   Starter
}

// MainWorkflowID is the ID of the main build of a commit, so that a
// redelivered push joins the running build.
func MainWorkflowID(repository, commit string) string {
	return "main/" + repository + "/" + commit
}

// Handle starts a main build for a push that changed an input path. It
// returns nil for deliveries it ignores, so they are acknowledged.
func (h PushHandler) Handle(ctx context.Context, event string, body []byte) error {
	if event != "push" {
		return nil
	}
	var push PushEvent
	if err := json.Unmarshal(body, &push); err != nil {
		return nil
	}
	if push.Deleted || !strings.EqualFold(push.Repository.FullName, h.Repository) || push.Ref != "refs/heads/"+h.Branch || !commitPattern.MatchString(push.After) {
		return nil
	}
	if !h.touchesInputs(push) {
		return nil
	}
	_, err := h.Temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       MainWorkflowID(h.Repository, push.After),
		TaskQueue:                TaskQueue,
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}, WorkflowName, Request{Repository: h.Repository, Commit: push.After, Trigger: TriggerMain})
	if err != nil {
		return fmt.Errorf("start the main build of %s: %w", push.After, err)
	}
	return nil
}

// touchesInputs is true when a changed file is under an input path. A push
// whose commit list is empty (GitHub caps it at 2048) is built.
func (h PushHandler) touchesInputs(push PushEvent) bool {
	if len(push.Commits) == 0 {
		return true
	}
	for _, commit := range push.Commits {
		for _, files := range [][]string{commit.Added, commit.Removed, commit.Modified} {
			for _, file := range files {
				for _, input := range h.Inputs {
					input = strings.Trim(input, "/")
					if file == input || strings.HasPrefix(file, input+"/") {
						return true
					}
				}
			}
		}
	}
	return false
}
