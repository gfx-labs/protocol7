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

// Learning entries that ran, recorded as Temporal's history JSON, replay on the
// SDK's workflow under its registered name. Every app's running entries replay
// on it, so a change that breaks this breaks them.
//
//   - live-learning-sanitized.json ran in a deployed namespace, stripped of its
//     text. It holds a start and timers.
//   - decided-before-the-curator.json was recorded on a Temporal dev server from
//     the workflow as it was before the curator (protocol7 ab4776d): a proposed
//     entry that a person accepted and then retired, two updates. The settle
//     handler replays it without the memo command, since it has no marker.
//   - settled-by-the-curator.json was recorded from the workflow as it is: an
//     open entry that the curator applied and a person retired. It holds the
//     version marker and the memo upsert.
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
