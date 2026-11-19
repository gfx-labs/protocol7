package chat

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/gfx-labs/protocol7/pkg/execution/design"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// designActivity runs the design board's activities on this worker. The
// agent waits on them inside a tool call.
var designActivity = workflow.ActivityOptions{
	TaskQueue:           TaskQueue,
	StartToCloseTimeout: time.Minute,
	RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
}

// boardInstructions tell the agent how the design board works. They are the
// description of design_show, which the agent reads when it lists tools.
const boardInstructions = `Show a design board beside the chat, where the person sees your pages live and can point at elements.

A board is a directory inside one of your clones, such as protocol7-selfdev/design/<name>/. It holds plain HTML, CSS and JS pages (no build step) and a board.json:
{"title": "Checkout", "artboards": [{"id": "desktop", "title": "Desktop", "page": "index.html", "width": 1440, "height": 900}, {"id": "mobile", "page": "index.html", "width": 390, "height": 844}]}
Each artboard shows one page at a viewport size, from 120 to 4096 pixels, up to 24 artboards. Give x and y to place an artboard, or leave them out to put it right of the one before.

Write the files first, then call this once with the directory. After that the page reloads by itself whenever you change a file, so iterate by editing files and commit each iteration on the chat's branch. Call it again only to show another directory. A message from the person may carry the elements they picked on the board, with each element's artboard, CSS selector, text and computed styles.`

// designTools are the tools of a chat's session. They read the chat's state
// and set its board, so they are made in the workflow, around its state.
func designTools(state *State) []p7.Tool {
	show := p7.Tool{
		Name:        "design_show",
		Description: boardInstructions,
		InputSchema: json.RawMessage(`{"type":"object","properties":{"dir":{"type":"string","description":"The board's directory, relative to your working directory, such as protocol7-selfdev/design/checkout."}},"required":["dir"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct {
				Dir string `json:"dir"`
			}
			if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
				return p7.ToolResult{}, fmt.Errorf("want {\"dir\": ...}: %w", err)
			}
			view, err := checkBoard(ctx, state, call.Session, arguments.Dir)
			if err != nil {
				return p7.ToolResult{}, err
			}
			if len(view.Artboards) == 0 {
				return p7.ToolResult{}, fmt.Errorf("the board cannot be shown: %s", view.Problem)
			}
			state.Board = &Board{Dir: view.Dir}
			return p7.TextResult("The person now sees the board beside the chat. " + describe(view)), nil
		},
	}
	check := p7.Tool{
		Name:        "design_check",
		Description: "Check the board shown beside the chat: its board.json and that every page it names exists. Returns the artboards and any problem.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
		Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
			if state.Board == nil {
				return p7.ToolResult{}, fmt.Errorf("no board is shown yet: call design_show")
			}
			view, err := checkBoard(ctx, state, call.Session, state.Board.Dir)
			if err != nil {
				return p7.ToolResult{}, err
			}
			return p7.TextResult(describe(view)), nil
		},
	}
	shot := p7.Tool{
		Name:        "design_shot",
		Description: "Render one artboard of the board shown beside the chat at its size, in headless Chromium, and see it as an image. Use it to check your work as the person sees it.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"artboard":{"type":"string","description":"The artboard's id in board.json."}},"required":["artboard"],"additionalProperties":false}`),
		Run: func(ctx workflow.Context, call p7.ToolCall) (p7.ToolResult, error) {
			var arguments struct {
				Artboard string `json:"artboard"`
			}
			if err := json.Unmarshal(call.Arguments, &arguments); err != nil {
				return p7.ToolResult{}, fmt.Errorf("want {\"artboard\": ...}: %w", err)
			}
			if state.Board == nil {
				return p7.ToolResult{}, fmt.Errorf("no board is shown yet: call design_show")
			}
			var shot design.Shot
			request := design.ShotRequest{Session: call.Session, Dir: state.Board.Dir, Artboard: arguments.Artboard, Pod: podOf(state)}
			activityCtx := workflow.WithActivityOptions(ctx, designActivity)
			if err := workflow.ExecuteActivity(activityCtx, design.ActivityShot, request).Get(ctx, &shot); err != nil {
				return p7.ToolResult{}, fmt.Errorf("%s", cause(err))
			}
			return p7.ToolResult{Content: []p7.ToolContent{
				{Text: fmt.Sprintf("Artboard %s at %dx%d:", arguments.Artboard, shot.Width, shot.Height)},
				{Data: shot.JPEG, MIMEType: "image/jpeg"},
			}}, nil
		},
	}
	return []p7.Tool{show, check, shot}
}

func checkBoard(ctx workflow.Context, state *State, session, dir string) (design.View, error) {
	var view design.View
	activityCtx := workflow.WithActivityOptions(ctx, designActivity)
	if err := workflow.ExecuteActivity(activityCtx, design.ActivityCheck, session, dir, podOf(state)).Get(ctx, &view); err != nil {
		return view, fmt.Errorf("%s", cause(err))
	}
	return view, nil
}

// describe is a board as the agent is told about it.
func describe(view design.View) string {
	var text strings.Builder
	fmt.Fprintf(&text, "Board %q in %s:", view.Title, view.Dir)
	for _, artboard := range view.Artboards {
		fmt.Fprintf(&text, "\n- %s: %s at %dx%d, placed at %d,%d", artboard.ID, artboard.Page, artboard.Width, artboard.Height, artboard.Left, artboard.Top)
	}
	if view.Problem != "" {
		text.WriteString("\nProblems:\n" + view.Problem)
	}
	return text.String()
}

// podOf is the session's pod when it shares it with others, which the file
// server of the pod is found by. Empty for the pod named for the session.
func podOf(state *State) string {
	if state.Session == nil || state.Session.Pool == nil {
		return ""
	}
	return state.Session.Pod
}
