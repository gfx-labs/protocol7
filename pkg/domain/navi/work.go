package navi

import (
	"fmt"
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain/scope"
)

// IssueRef names a GitHub issue or pull request.
type IssueRef struct {
	Repo   string `json:"repo"`
	Number int    `json:"number"`
}

func (r IssueRef) String() string { return fmt.Sprintf("%s#%d", r.Repo, r.Number) }

// IntakeInput is the input of IssueIntakeWorkflow.
type IntakeInput struct {
	Issue IssueRef `json:"issue"`
	Label string   `json:"label"`
}

// Template is how the scanner starts each issue's workflow. It is the
// payload of SignalScannerTemplate.
type Template struct {
	Repos        []string     `json:"repos"`
	Label        string       `json:"label"`
	Limit        int          `json:"limit"`
	Host         string       `json:"host"`
	Backend      string       `json:"backend,omitempty"`
	PoolScope    string       `json:"pool_scope,omitempty"`
	Models       []string     `json:"models,omitempty"`
	ModelName    string       `json:"model_name,omitempty"`
	Repositories []Repository `json:"repositories"`
	PullRepos    []string     `json:"pull_repos"`
	PageURL      string       `json:"page_url,omitempty"`
	Scope        *scope.Scope `json:"scope,omitempty"`
}

// IssueRequest is the input of IssueWorkflow.
type IssueRequest struct {
	Ref      IssueRef `json:"ref"`
	Template Template `json:"template"`
}

// IssueStatus says where an issue's workflow is.
type IssueStatus string

const (
	IssueStarting IssueStatus = "starting"
	IssueWorking  IssueStatus = "working"
	IssueWaiting  IssueStatus = "waiting"
	IssueDone     IssueStatus = "done"
	IssueClosed   IssueStatus = "closed"
	IssueStopped  IssueStatus = "stopped"
	IssueGaveUp   IssueStatus = "gave_up"
)

// IssuePull is a pull request of an issue's chat.
type IssuePull struct {
	Repo            string `json:"repo"`
	Number          int    `json:"number"`
	URL             string `json:"url"`
	Head            string `json:"head"`
	Base            string `json:"base"`
	State           string `json:"state"`
	FiledHead       string `json:"filed_head,omitempty"`
	LastReview      int64  `json:"last_review,omitempty"`
	ChecksRelayed   string `json:"checks_relayed,omitempty"`
	ConflictRelayed string `json:"conflict_relayed,omitempty"`
	ClosedRelayed   bool   `json:"closed_relayed,omitempty"`
}

// IssueState is what QueryState returns from an issue's workflow.
type IssueState struct {
	Request     IssueRequest `json:"request"`
	Status      IssueStatus  `json:"status"`
	Started     time.Time    `json:"started,omitempty"`
	Chat        string       `json:"chat,omitempty"`
	Sent        int          `json:"sent"`
	LastSent    string       `json:"last_sent,omitempty"`
	Answered    bool         `json:"answered,omitempty"`
	ChatEnded   bool         `json:"chat_ended,omitempty"`
	Reports     []Report     `json:"reports,omitempty"`
	LastComment int64        `json:"last_comment,omitempty"`
	Pulls       []IssuePull  `json:"pulls,omitempty"`
	Error       string       `json:"error,omitempty"`
}

// ScannerState is what QueryState returns from the scanner.
type ScannerState struct {
	Template    Template   `json:"template"`
	Paused      bool       `json:"paused,omitempty"`
	Passes      int        `json:"passes"`
	LastPass    time.Time  `json:"last_pass,omitempty"`
	LastStarted []IssueRef `json:"last_started,omitempty"`
	LastError   string     `json:"last_error,omitempty"`
	Woken       bool       `json:"woken,omitempty"`
	NextWake    time.Time  `json:"next_wake,omitempty"`
}

// ReviewRequest is the input of ReviewWorkflow.
type ReviewRequest struct {
	Repo    string       `json:"repo"`
	Number  int          `json:"number"`
	Merge   bool         `json:"merge,omitempty"`
	Host    string       `json:"host"`
	Backend string       `json:"backend,omitempty"`
	Models  []string     `json:"models,omitempty"`
	Scope   *scope.Scope `json:"scope,omitempty"`
}

// ReviewStatus says where a review is.
type ReviewStatus string

const (
	ReviewReading   ReviewStatus = "reading"
	ReviewReviewing ReviewStatus = "reviewing"
	ReviewPublish   ReviewStatus = "publishing"
	ReviewWaiting   ReviewStatus = "waiting"
	ReviewDraft     ReviewStatus = "draft"
	ReviewChanges   ReviewStatus = "changes_requested"
	ReviewBlocked   ReviewStatus = "blocked"
	ReviewApproved  ReviewStatus = "approved"
	ReviewMerged    ReviewStatus = "merged"
	ReviewClosed    ReviewStatus = "closed"
	ReviewFailed    ReviewStatus = "failed"
)

// Verdict is a reviewer's decision on a head.
type Verdict struct {
	Decision string   `json:"verdict"`
	Summary  string   `json:"summary"`
	Findings []string `json:"findings,omitempty"`
}

// ReviewState is what QueryState returns from a review.
type ReviewState struct {
	Request       ReviewRequest `json:"request"`
	Status        ReviewStatus  `json:"status"`
	Started       time.Time     `json:"started,omitempty"`
	Head          string        `json:"head,omitempty"`
	HeadSeen      time.Time     `json:"head_seen,omitempty"`
	Reviewed      string        `json:"reviewed,omitempty"`
	Verdict       *Verdict      `json:"verdict,omitempty"`
	Published     string        `json:"published,omitempty"`
	Rounds        int           `json:"rounds"`
	MergeFailures int           `json:"merge_failures,omitempty"`
	Blocked       string        `json:"blocked,omitempty"`
	Error         string        `json:"error,omitempty"`
	Rereview      bool          `json:"rereview,omitempty"`
}

// LearningState is the state of a learning. AttributeLearningState holds it.
type LearningState string

const (
	LearningProposed LearningState = "proposed"
	LearningAccepted LearningState = "accepted"
	LearningRejected LearningState = "rejected"
	LearningRetired  LearningState = "retired"
)

// LearningScopeKind says what a learning applies to.
type LearningScopeKind string

const (
	LearningScopeProject    LearningScopeKind = "project"
	LearningScopeRepository LearningScopeKind = "repository"
	LearningScopeArea       LearningScopeKind = "area"
)

// LearningScope is what a learning applies to.
type LearningScope struct {
	Kind  LearningScopeKind `json:"kind"`
	Repos []string          `json:"repos,omitempty"`
	Area  string            `json:"area,omitempty"`
}

// LearningSource is where a learning came from.
type LearningSource struct {
	Escalation string `json:"escalation,omitempty"`
	Chat       string `json:"chat,omitempty"`
}

// LearningEntry is what QueryLearning returns and MemoLearningEntry holds.
type LearningEntry struct {
	ID       string         `json:"id"`
	Title    string         `json:"title"`
	Problem  string         `json:"problem"`
	Cause    string         `json:"cause"`
	Fix      string         `json:"fix"`
	Scope    LearningScope  `json:"scope"`
	Source   LearningSource `json:"source"`
	Project  string         `json:"project,omitempty"`
	Recorded time.Time      `json:"recorded"`
	State    LearningState  `json:"state"`
}

// LearningDecision is the payload of UpdateLearningDecide.
type LearningDecision struct {
	Action string `json:"action"`
	By     string `json:"by,omitempty"`
}
