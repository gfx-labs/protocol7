// Package review is the selfdev pull request review: one workflow per pull
// request, filed as a ticket by whoever opened it. An agent session reads the
// pull request at its head and gives a verdict, the workflow publishes the
// verdict as a GitHub review, and a pull request that is approved and passes
// the merge criteria is merged. The criteria are code (Decide), not the
// agent's say.
package review

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"
)

const (
	// WorkflowName is the registered name of the review workflow.
	WorkflowName = navi.ReviewWorkflow
	// TaskQueue is the queue of the selfdev worker, which runs the workflow
	// and its GitHub activities.
	TaskQueue = navi.TaskQueue

	// SignalRereview asks for a look now, for example after the author
	// pushed a fix or a person asked again.
	SignalRereview = navi.SignalRereview
	// QueryState returns the review's State.
	QueryState = navi.QueryState

	// NoMergeLabel keeps a pull request from being merged by the workflow.
	NoMergeLabel = "no-auto-merge"
	// MergeBranchPrefix is what the head branch of a merged pull request
	// starts with: the branches that chats make. A person merges the rest.
	MergeBranchPrefix = "chat/"
	// ReviewMarker starts the comment in a review's body that says which
	// head and verdict it was for. Publishing looks for it, so that a retried
	// activity does not post the review twice.
	ReviewMarker = "<!-- selfdev-review "
)

// WorkflowID is the workflow ID of the review of one pull request. Filing a
// review for a pull request that already has a running one reaches that one.
func WorkflowID(repo string, number int) string {
	return "review-" + strings.ReplaceAll(repo, "/", "-") + "-" + strconv.Itoa(number)
}

// Request describes a review ticket.
type Request = navi.ReviewRequest

// CanMerge says whether the request's scope allows the merge of a pull request
// of repo: the repository is in the scope, which holds write on pull requests
// and on contents. A request with no scope is not limited here.
func CanMerge(r Request) bool { return navi.ReviewCanMerge(r) }

// ParseURL reads a pull request's address, https://github.com/owner/name/pull/N.
func ParseURL(address string) (repo string, number int, err error) {
	return navi.ParseReviewURL(address)
}

// Status says what a review is doing.
type Status = navi.ReviewStatus

// Statuses of a review.
const (
	StatusReading   = navi.ReviewReading   // reading the pull request from GitHub
	StatusReviewing = navi.ReviewReviewing // an agent reads the head
	StatusPublish   = navi.ReviewPublish
	StatusWaiting   = navi.ReviewWaiting // approved, waiting for checks or for GitHub
	StatusDraft     = navi.ReviewDraft   // waiting for the pull request to be ready
	StatusChanges   = navi.ReviewChanges
	StatusBlocked   = navi.ReviewBlocked // a person has to act: the reason is in Blocked
	StatusApproved  = navi.ReviewApproved
	StatusMerged    = navi.ReviewMerged
	StatusClosed    = navi.ReviewClosed // closed without a merge
	StatusFailed    = navi.ReviewFailed
)

// State is everything the review workflow keeps. It is the workflow's input,
// so it is also what one run hands to the next on Continue-As-New. A new
// review starts from a State that holds only its Request.
type State = navi.ReviewState

// Limits.
const (
	MaxRounds        = 6
	MaxMergeFailures = 3
	// MinCheckWait is how long a head has been seen before it counts as having
	// no checks, because checks register a little after a push.
	MinCheckWait = 2 * time.Minute
	// PollEvery is how often a waiting review reads the pull request again.
	PollEvery = time.Minute
)

// Verdict is what the reviewer decided about one head.
type Verdict = navi.Verdict

// Verdicts the reviewer may give.
const (
	Approve        = "approve"
	RequestChanges = "request_changes"
	Comment        = "comment"
)

var verdictBlock = regexp.MustCompile("(?s)```review-verdict\\s*(\\{.*?\\})\\s*```")

// ParseVerdict reads the last review-verdict block of an agent's message.
func ParseVerdict(text string) (Verdict, error) {
	blocks := verdictBlock.FindAllStringSubmatch(text, -1)
	if len(blocks) == 0 {
		return Verdict{}, errors.New("no review-verdict block in the answer")
	}
	var verdict Verdict
	if err := json.Unmarshal([]byte(blocks[len(blocks)-1][1]), &verdict); err != nil {
		return Verdict{}, fmt.Errorf("the review-verdict block is not valid JSON: %w", err)
	}
	switch verdict.Decision {
	case Approve, RequestChanges, Comment:
	default:
		return Verdict{}, fmt.Errorf("the verdict %q is none of approve, request_changes and comment", verdict.Decision)
	}
	verdict.Summary = strings.TrimSpace(verdict.Summary)
	if verdict.Summary == "" {
		return Verdict{}, errors.New("the verdict has no summary")
	}
	return verdict, nil
}

// Pull is what the workflow reads of a pull request.
type Pull struct {
	Repo     string   `json:"repo"`
	Number   int      `json:"number"`
	Title    string   `json:"title"`
	Author   string   `json:"author"`
	Base     string   `json:"base"`
	HeadRef  string   `json:"head_ref"`
	HeadRepo string   `json:"head_repo"`
	Head     string   `json:"head"`
	Open     bool     `json:"open"`
	Merged   bool     `json:"merged"`
	Draft    bool     `json:"draft"`
	Labels   []string `json:"labels,omitempty"`
	// Mergeable is nil while GitHub has not worked it out. MergeState is
	// GitHub's mergeable_state: clean, dirty, blocked, behind, unstable...
	Mergeable  *bool  `json:"mergeable,omitempty"`
	MergeState string `json:"merge_state,omitempty"`
	Checks     Checks `json:"checks"`
}

// Checks counts the check runs of a head.
type Checks struct {
	Total   int `json:"total"`
	Pending int `json:"pending"`
	Failed  int `json:"failed"`
}

// Action is what Decide says to do about an approved pull request.
type Action string

// Actions.
const (
	ActionMerge Action = "merge"
	ActionWait  Action = "wait"
	ActionBlock Action = "block"
)

// Decide applies the merge criteria to an approved pull request. It has no
// side effects. headAge is how long the workflow has seen the head. The
// reason says why a wait or a block is one.
//
// The criteria: a chat's branch, no opt-out label, no conflict, no branch
// protection that is still unmet, and every check run finished and green. A
// head with no check runs counts as green only after MinCheckWait, because
// checks register a little after a push.
func Decide(pull Pull, headAge time.Duration) (Action, string) {
	switch {
	case !strings.HasPrefix(pull.HeadRef, MergeBranchPrefix):
		return ActionBlock, fmt.Sprintf("the branch %s is not one of a chat's (%s*), so a person merges it", pull.HeadRef, MergeBranchPrefix)
	case pull.HeadRepo != pull.Repo:
		return ActionBlock, "the branch is in a fork"
	case contains(pull.Labels, NoMergeLabel):
		return ActionBlock, "the pull request has the label " + NoMergeLabel
	case pull.Draft:
		return ActionBlock, "the pull request is a draft"
	case pull.Mergeable != nil && !*pull.Mergeable:
		return ActionBlock, "the pull request does not merge cleanly: " + pull.MergeState
	case pull.Checks.Failed > 0:
		return ActionBlock, fmt.Sprintf("%d check run(s) failed", pull.Checks.Failed)
	case pull.Mergeable == nil || pull.MergeState == "unknown" || pull.MergeState == "":
		return ActionWait, "GitHub has not worked out whether it merges"
	case pull.Checks.Pending > 0:
		return ActionWait, fmt.Sprintf("%d check run(s) still run", pull.Checks.Pending)
	case pull.Checks.Total == 0 && headAge < MinCheckWait:
		return ActionWait, "no check has registered for this head yet"
	}
	switch pull.MergeState {
	case "clean", "has_hooks", "unstable":
		// unstable is a failing check that is not required. Failed counts
		// the check runs already, and it is zero here.
		return ActionMerge, ""
	case "behind", "blocked", "dirty", "draft":
		return ActionBlock, "GitHub says the pull request is " + pull.MergeState + ", which a review cannot change"
	}
	return ActionWait, "GitHub says " + pull.MergeState
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
