package design

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
)

// Activity names of the design board. They run on the consumer's worker,
// which reaches the file server beside the core worker.
const (
	ActivityCheck = "design.Check"
	ActivityShot  = "design.Shot"
)

// MaxShotSize bounds a screenshot. It reaches the agent through Temporal,
// in the activity's result and the tool call's, and each counts toward the
// chat's history. Temporal warns about payloads above 512 KiB.
const MaxShotSize = 512 << 10

// ShotRequest names one artboard of a board to render.
type ShotRequest struct {
	Session  string `json:"session"`
	Dir      string `json:"dir"`
	Artboard string `json:"artboard"`
	// Pod is the session's pod, if it is not the one named for the session.
	Pod string `json:"pod,omitempty"`
}

// Shot is a rendered artboard.
type Shot struct {
	JPEG  []byte `json:"jpeg"`
	Width int    `json:"width"`
	// Height is the artboard's height.
	Height int `json:"height"`
}

// Activities are the design board's activities.
type Activities struct {
	Files FilesClient
}

// Check reads and checks a board. See FilesClient.Check.
//
// pod is the name of the session's pod, empty for the pod named for the
// session.
func (a Activities) Check(ctx context.Context, session, dir, pod string) (View, error) {
	if !a.Files.Configured() {
		return View{}, temporal.NewNonRetryableApplicationError("design boards are not set up on this worker", "NotConfigured", nil)
	}
	return a.Files.ForPod(pod).Check(ctx, session, dir)
}

// Shot renders one artboard at its size.
func (a Activities) Shot(ctx context.Context, request ShotRequest) (Shot, error) {
	view, err := a.Check(ctx, request.Session, request.Dir, request.Pod)
	if err != nil {
		return Shot{}, err
	}
	for _, artboard := range view.Artboards {
		if artboard.ID != request.Artboard {
			continue
		}
		page, _ := PagePath(artboard.Page)
		query := "?width=" + strconv.Itoa(artboard.Width) + "&height=" + strconv.Itoa(artboard.Height)
		shotCtx, cancel := context.WithTimeout(ctx, shotTimeout+10*time.Second)
		defer cancel()
		response, err := a.Files.Get(shotCtx, "shot", request.Session, path.Join(view.Dir, page), query)
		if err != nil {
			return Shot{}, fmt.Errorf("render artboard %s: %w", artboard.ID, err)
		}
		defer response.Body.Close()
		content, err := io.ReadAll(io.LimitReader(response.Body, MaxShotSize+1))
		if err != nil {
			return Shot{}, fmt.Errorf("render artboard %s: %w", artboard.ID, err)
		}
		if response.StatusCode != http.StatusOK {
			return Shot{}, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("render artboard %s: %s", artboard.ID, strings.TrimSpace(string(content))), "ShotFailed", nil)
		}
		if len(content) > MaxShotSize {
			return Shot{}, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("the screenshot of artboard %s is larger than %d KiB", artboard.ID, MaxShotSize>>10), "ShotTooLarge", nil)
		}
		return Shot{JPEG: content, Width: artboard.Width, Height: artboard.Height}, nil
	}
	if view.Problem != "" && len(view.Artboards) == 0 {
		return Shot{}, temporal.NewNonRetryableApplicationError(view.Problem, "BoardProblem", nil)
	}
	return Shot{}, temporal.NewNonRetryableApplicationError(fmt.Sprintf("the board has no artboard %q", request.Artboard), "NoArtboard", errors.New("no artboard"))
}
