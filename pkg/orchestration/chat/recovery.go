package chat

import (
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/p7"
)

// A harness error is a completed activity, not a successful model turn.
// Keep the provider's diagnostics visible to the person and the chat's owner.
func failedTurnError(turn p7.Turn) string {
	if turn.Failure == nil {
		return "the agent's turn failed"
	}
	failure := turn.Failure
	message := failure.Message
	if message == "" {
		message = "the agent's turn failed"
	}
	var details []string
	if failure.Kind != "" {
		details = append(details, failure.Kind)
	}
	if failure.Account != "" {
		details = append(details, "account "+failure.Account)
	}
	if failure.Model != "" {
		details = append(details, "model "+failure.Model)
	}
	if !failure.ResetAt.IsZero() {
		details = append(details, "reset at "+failure.ResetAt.UTC().Format(time.RFC3339))
	}
	if len(details) > 0 {
		message += " (" + strings.Join(details, ", ") + ")"
	}
	return message
}

// versionTurnResume gates the resume of an interrupted turn. A history from
// before it ended such a turn and went on, with no timer in between, and
// replays that way.
const versionTurnResume = "turn-resume"

// resumeDelays are the waits before each new turn that continues an
// interrupted one, so their count is how many times a message is resumed.
// p7 has already retried the turn's activity by the time a turn counts as
// interrupted, so what is left is a session whose processes keep dying.
var resumeDelays = []time.Duration{15 * time.Second, time.Minute, 4 * time.Minute}

// turnOutcome is the end of one message's turns.
type turnOutcome struct {
	Turn p7.Turn
	// GaveUp is set when the last resume was cut off as well.
	GaveUp bool
}

// runTurn runs the turn that answers a message. p7 reports a turn whose
// session processes went away as a finished turn with the stop reason
// interrupted, which Temporal's activity retry does not see. The chat then
// waits on a workflow timer and starts a new turn that says what happened,
// up to len(resumeDelays) times. A turn that ctx's cancellation ended, which
// is what a person's stop and a close do, is never resumed.
func runTurn(ctx workflow.Context, state *State, message *Message, prompt p7.TextPrompt) (turnOutcome, error) {
	resume := workflow.GetVersion(ctx, versionTurnResume, workflow.DefaultVersion, 1) >= 1
	state.Resumes = 0
	original := prompt
	for {
		turn, err := modelTurn(ctx, state, prompt)
		if err != nil || turn.StopReason != p7.StopInterrupted || !resume || ctx.Err() != nil {
			return turnOutcome{Turn: turn}, err
		}
		if int(state.Resumes) >= len(resumeDelays) {
			return turnOutcome{Turn: turn, GaveUp: true}, nil
		}
		if err := workflow.Sleep(ctx, resumeDelays[state.Resumes]); err != nil {
			// A stop or a close came during the wait.
			turn.StopReason = p7.StopCancelled
			return turnOutcome{Turn: turn}, nil
		}
		state.Resumes++
		prompt = resumePrompt(original, message, int(state.Resumes))
	}
}

// resumePrompt is the turn that continues an interrupted one. The note and
// the request go in the hidden context, so the page shows a short message
// and not the request again. The request is repeated because the
// interrupted turn may not have reached the agent.
func resumePrompt(original p7.TextPrompt, message *Message, resumes int) p7.TextPrompt {
	prompt := original
	prompt.TurnID = fmt.Sprintf("%s-resume%d", message.Id, resumes)
	note := "Your previous turn was cut off before it ended, because the session's processes stopped (a restart of the machine or pod, or the harness died). No person stopped it. " +
		"What you did until then is kept, but a command that was running was killed, and work that is not committed may be half done. " +
		"Before you go on, check the state of each clone with `git status` and `git log --oneline -5`, and whether your branch is on the remote. " +
		"Commit and push what is worth keeping, then continue the request from where it stopped.\n\nThe request was:\n" + message.Text
	if original.Context != "" {
		note = original.Context + "\n\n" + note
	}
	prompt.Context = note
	prompt.Text = "Continue from where your last turn was cut off."
	return prompt
}

// giveUpError is what the chat says when the last resume was cut off too.
func giveUpError(turn p7.Turn) string {
	reason := "the session's processes stopped during the turn"
	if turn.Failure != nil && turn.Failure.Message != "" {
		reason = turn.Failure.Message
	}
	return fmt.Sprintf("the turn was cut off %d times, so the chat stopped resuming it (%s). Send a message to continue.", len(resumeDelays)+1, reason)
}
