package learning_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/orchestration/learning"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// A learning entry that ran in a deployed namespace, recorded with
// `temporal workflow show --output json` and stripped of its text, replays on
// the SDK's workflow under its registered name. Every app's running entries
// replay on it after the move from the app's own copy, so a change that breaks
// this breaks them.
func TestLiveLearningsReplay(t *testing.T) {
	files, err := filepath.Glob("testdata/*.json")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{DataConverter: p7.DataConverter()})
			require.NoError(t, err)
			replayer.RegisterWorkflowWithOptions(learning.Workflow, workflow.RegisterOptions{Name: learning.WorkflowName})
			require.NoError(t, replayer.ReplayWorkflowHistoryFromJSONFile(nil, file))
		})
	}
}
