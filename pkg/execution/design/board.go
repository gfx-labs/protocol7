// Package design is the design board of a chat: a directory in one of the
// chat's clones that holds web pages and a board.json, which lays the pages
// out as artboards. The agent writes the files. The page shows each
// artboard live, in a sandboxed frame, and reloads it when the files change.
package design

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"
)

// ManifestName is the file in a board's directory that describes the board.
const ManifestName = "board.json"

// Limits of a board. An artboard is a page at one viewport size.
const (
	MaxArtboards = 24
	MinSize      = 120
	MaxSize      = 4096
	// gap is the space between artboards laid out with no position.
	gap = 80
)

// Board is the content of board.json.
type Board struct {
	Title     string     `json:"title"`
	Artboards []Artboard `json:"artboards"`
}

// Artboard is one page of the board, shown at Width by Height CSS pixels.
type Artboard struct {
	ID    string `json:"id"`
	Title string `json:"title,omitempty"`
	// Page is the page's path in the board's directory, such as home.html.
	Page   string `json:"page"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	// X and Y place the artboard on the board. An artboard without them is
	// put to the right of the one before it.
	X *int `json:"x,omitempty"`
	Y *int `json:"y,omitempty"`
}

var artboardID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,47}$`)

// Parse reads and checks a board.json. It does not look at the pages.
func Parse(content []byte) (Board, error) {
	var board Board
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&board); err != nil {
		return board, fmt.Errorf("%s is not the expected JSON: %w", ManifestName, err)
	}
	if len(board.Artboards) == 0 || len(board.Artboards) > MaxArtboards {
		return board, fmt.Errorf("%s: a board has 1 to %d artboards, this one has %d", ManifestName, MaxArtboards, len(board.Artboards))
	}
	seen := map[string]bool{}
	var problems []error
	for index, artboard := range board.Artboards {
		name := fmt.Sprintf("artboard %d", index+1)
		if artboard.ID != "" {
			name = fmt.Sprintf("artboard %q", artboard.ID)
		}
		switch {
		case !artboardID.MatchString(artboard.ID):
			problems = append(problems, fmt.Errorf("%s: the id must be up to 48 lower-case letters, digits and '-'", name))
		case seen[artboard.ID]:
			problems = append(problems, fmt.Errorf("%s: the id is used twice", name))
		}
		seen[artboard.ID] = true
		if _, err := PagePath(artboard.Page); err != nil {
			problems = append(problems, fmt.Errorf("%s: %w", name, err))
		}
		if artboard.Width < MinSize || artboard.Width > MaxSize || artboard.Height < MinSize || artboard.Height > MaxSize {
			problems = append(problems, fmt.Errorf("%s: width and height must be %d to %d", name, MinSize, MaxSize))
		}
		if (artboard.X == nil) != (artboard.Y == nil) {
			problems = append(problems, fmt.Errorf("%s: give both x and y, or neither", name))
		}
	}
	return board, errors.Join(problems...)
}

// PagePath checks a path relative to a board's directory and returns it
// cleaned. It may not leave the directory or name a hidden file.
func PagePath(page string) (string, error) {
	cleaned := path.Clean(page)
	if page == "" || path.IsAbs(page) || strings.Contains(page, `\`) || cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("the page %q must be a path inside the board's directory", page)
	}
	for part := range strings.SplitSeq(cleaned, "/") {
		if strings.HasPrefix(part, ".") {
			return "", fmt.Errorf("the page %q names a hidden file or directory", page)
		}
	}
	return cleaned, nil
}

// Placed is an artboard with its position on the board.
type Placed struct {
	Artboard
	Left int `json:"left"`
	Top  int `json:"top"`
}

// Layout places every artboard. One with no position goes to the right of
// the artboard before it, at the same top.
func Layout(board Board) []Placed {
	placed := make([]Placed, 0, len(board.Artboards))
	left, top := 0, 0
	for _, artboard := range board.Artboards {
		if artboard.X != nil {
			left, top = *artboard.X, *artboard.Y
		}
		placed = append(placed, Placed{Artboard: artboard, Left: left, Top: top})
		left += artboard.Width + gap
	}
	return placed
}

// BoardPath checks the path of a board's directory, given relative to the
// session's workspace, such as protocol7-selfdev/design/checkout. The first
// part is a clone.
func BoardPath(dir string) (string, error) {
	cleaned, err := PagePath(dir)
	if err != nil {
		return "", fmt.Errorf("board directory: %w", err)
	}
	if !strings.Contains(cleaned, "/") {
		return "", fmt.Errorf("board directory %q: want a directory inside a clone, such as <repository>/design/<name>", dir)
	}
	return cleaned, nil
}
