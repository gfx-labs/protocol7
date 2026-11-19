package replay_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/orchestration/issue"
	"github.com/gfx-labs/protocol7/pkg/orchestration/learning"
	"github.com/gfx-labs/protocol7/pkg/orchestration/review"
)

// HistoriesEnv names a directory of workflow histories exported with
// `temporal workflow show --output json`. The histories hold a tenant's chats,
// so they live in the tenant's private repository, which sets this in its CI.
// Without it the test skips.
const HistoriesEnv = "P7_REPLAY_HISTORIES"

// Every recorded history replays on the workflow code of this module, so a
// change that would break the workflows that run in a namespace fails here.
func TestRecordedHistoriesReplay(t *testing.T) {
	dir := os.Getenv(HistoriesEnv)
	if dir == "" {
		t.Skipf("%s is not set", HistoriesEnv)
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatalf("no histories in %s", dir)
	}
	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			workflowType, err := historyType(file)
			if err != nil {
				t.Fatal(err)
			}
			replayer := worker.NewWorkflowReplayer()
			register(replayer)
			if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, file); err != nil {
				t.Fatalf("%s: %v", workflowType, err)
			}
		})
	}
}

// register registers every workflow an app's worker runs, under its name.
func register(replayer worker.WorkflowReplayer) {
	workflows := map[string]any{
		chat.WorkflowName:           chat.NewWorkflow(chat.Integrations{}),
		chat.EscalationWorkflowName: chat.EscalationWorkflow,
		chat.HandlerWorkflowName:    chat.HandlerWorkflow,
		learning.WorkflowName:       learning.Workflow,
		review.WorkflowName:         review.Workflow,
		issue.WorkflowName:          issue.Workflow,
		issue.ScanWorkflowName:      issue.ScanWorkflow,
		issue.ScannerWorkflowName:   issue.ScannerWorkflow,
		issue.IntakeWorkflowName:    issue.IntakeWorkflow,
	}
	for name, function := range workflows {
		replayer.RegisterWorkflowWithOptions(function, workflow.RegisterOptions{Name: name})
	}
}

// historyType reads the workflow type from a history's first event.
func historyType(file string) (string, error) {
	content, err := os.ReadFile(file)
	if err != nil {
		return "", err
	}
	var history struct {
		Events []struct {
			Attributes struct {
				WorkflowType struct {
					Name string `json:"name"`
				} `json:"workflowType"`
			} `json:"workflowExecutionStartedEventAttributes"`
		} `json:"events"`
	}
	if err := json.Unmarshal(content, &history); err != nil {
		return "", err
	}
	if len(history.Events) == 0 {
		return "", os.ErrNotExist
	}
	return strings.TrimSpace(history.Events[0].Attributes.WorkflowType.Name), nil
}
