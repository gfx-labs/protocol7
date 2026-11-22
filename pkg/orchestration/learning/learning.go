// Package learning is the learnings log: what went wrong, why, and what
// worked, recorded once so that no chat finds it out again.
//
// # Where the log lives
//
// An entry is a workflow of the project's Temporal namespace (Workflow). A
// proposed or accepted entry is a running workflow, which Temporal's
// retention never removes, and the entry's text is its memo. The state is a
// search attribute. The choice, against the two the design named:
//
//   - Files in the selfdev repository would be reviewed and versioned, but
//     navi holds no GitHub token and its network policy does not admit GitHub,
//     so it could not read them, and a chat of another project could not read
//     the core project's repository at all.
//   - The artifacts bucket (issue 4) is not built, and reading it from navi
//     needs a new PDP grant and key.
//   - Temporal is what navi and the selfdev worker already read and write,
//     for each project in its own namespace, with the credentials they have.
//     The record survives restarts of every pod. What it lacks, review in a
//     pull request, an entry has in its state: an agent's entry is only
//     proposed, and the handler or a person accepts it. Only accepted
//     entries reach a chat.
//
// A rejected or retired entry ends its workflow and ages out with Temporal's
// retention. Nothing else is ever deleted.
package learning

import (
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"
	"github.com/gfx-labs/protocol7/pkg/support/embedding"
)

const (
	// WorkflowName is the registered name of the learning workflow.
	WorkflowName = navi.LearningWorkflow
	// WorkflowPrefix starts the workflow ID of an entry.
	WorkflowPrefix = navi.LearningWorkflowPrefix
	// KindLearning is the SelfdevKind of an entry's workflow.
	KindLearning = "learning"

	// UpdateDecide accepts, rejects or retires an entry. It carries a
	// Decision.
	UpdateDecide = navi.UpdateLearningDecide
	// QueryEntry returns the entry's current Entry.
	QueryEntry = navi.QueryLearning
	// MemoEntry is the memo field that holds the entry as it was recorded.
	MemoEntry = navi.MemoLearningEntry

	// Bounds of an entry, so that an entry is a note and never a transcript.
	TitleLimit = 120
	FieldLimit = 1000
	LineLimit  = 12
	AreaLimit  = 60
	MaxRepos   = 8
)

// State is where an entry is in its life.
type State string

const (
	// Proposed is an entry that an ordinary chat recorded and nobody
	// accepted. Chats do not read it.
	Proposed State = "proposed"
	// Accepted is an entry that the handler or a person accepted, or the
	// handler recorded. Chats read it.
	Accepted State = "accepted"
	// Rejected and Retired are the ends: the workflow completes.
	Rejected State = "rejected"
	Retired  State = "retired"
)

// Search attributes of an entry. SelfdevKind and SelfdevProject are the
// chat's own.
var (
	AttributeKind    = navi.AttributeKind
	AttributeProject = navi.AttributeProject
	// AttributeState is the entry's State.
	AttributeState = navi.AttributeLearningState
)

// ScopeKind says how far an entry reaches.
type ScopeKind string

const (
	// ScopeProject holds for every repository of the project.
	ScopeProject ScopeKind = "project"
	// ScopeRepository holds for the repositories it names.
	ScopeRepository ScopeKind = "repository"
	// ScopeArea holds for an area of the work, such as "kubernetes" or
	// "ci", in the repositories it names, or in any.
	ScopeArea ScopeKind = "area"
)

// Scope says where an entry applies.
type Scope struct {
	Kind ScopeKind `json:"kind"`
	// Repos are owner/name, lower case. Required for a repository scope.
	Repos []string `json:"repos,omitempty"`
	// Area is required for an area scope.
	Area string `json:"area,omitempty"`
}

// Source says where an entry came from.
type Source struct {
	// Escalation is the ID of the escalation it was learned from, and Chat
	// the chat that recorded it. Either may be empty.
	Escalation string `json:"escalation,omitempty"`
	Chat       string `json:"chat,omitempty"`
}

// Entry is one learning.
type Entry struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	// Problem is what went wrong, Cause why, and Fix what worked.
	Problem string `json:"problem"`
	Cause   string `json:"cause"`
	Fix     string `json:"fix"`
	Scope   Scope  `json:"scope"`
	Source  Source `json:"source"`
	// Project is the navi project, empty for the core one.
	Project  string    `json:"project,omitempty"`
	Recorded time.Time `json:"recorded"`
	// State is as the search attribute says when an entry is listed, and
	// what it started as in the memo.
	State State `json:"state"`
	// Vector is the entry's embedding and VectorModel the model that made
	// it. They are set when the entry is recorded, in the memo with the
	// rest. An entry recorded before embeddings, or while the embeddings
	// server was down, has none and is embedded when it is first needed.
	Vector      embedding.Vector `json:"vector,omitempty"`
	VectorModel string           `json:"vector_model,omitempty"`
}

// Decision is the payload of UpdateDecide.
type Decision = navi.LearningDecision

// WorkflowID is the workflow ID of an entry.
func WorkflowID(id string) string { return WorkflowPrefix + id }

var (
	idPattern   = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	repoPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*/[a-z0-9][a-z0-9._-]*$`)
	refPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`)
)

// secrets are what an entry must not hold. This is a screen for the usual
// mistakes, and not a guarantee: the tools tell the agent to write no
// secrets either.
var secrets = []*regexp.Regexp{
	regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY`),
	regexp.MustCompile(`\b(ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}`),
	regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{20,}`),
	regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`),
	regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{20,}`),
	regexp.MustCompile(`\bxox[abprs]-[A-Za-z0-9-]{10,}`),
	regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}`),
	regexp.MustCompile(`(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{20,}`),
	regexp.MustCompile(`(?i)\b(password|passwd|secret|token|api[_-]?key|access[_-]?key)\s*[:=]\s*["']?[^\s"']{8,}`),
	regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://[^\s/:@]+:[^\s/@]+@`),
}

// checkText refuses a field that is too long, holds a secret, or reads as a
// pasted transcript or log.
func checkText(name, text string, limit int) error {
	if utf8.RuneCountInString(text) > limit {
		return fmt.Errorf("%s is longer than %d characters: a learning is a short note, not a log", name, limit)
	}
	if strings.Count(text, "\n") >= LineLimit {
		return fmt.Errorf("%s has %d lines or more: write a short note, not a transcript or a log", name, LineLimit)
	}
	for _, pattern := range secrets {
		if pattern.MatchString(text) {
			return fmt.Errorf("%s looks like it holds a secret: a learning never does. Say what kind of credential and where it lives, not its value", name)
		}
	}
	return nil
}

// Normalize trims the entry's text and puts the repositories in the form the
// log keeps them in.
func (e *Entry) Normalize() {
	e.Title, e.Problem, e.Cause, e.Fix = strings.TrimSpace(e.Title), strings.TrimSpace(e.Problem), strings.TrimSpace(e.Cause), strings.TrimSpace(e.Fix)
	e.Scope.Area = strings.ToLower(strings.TrimSpace(e.Scope.Area))
	repos := make([]string, 0, len(e.Scope.Repos))
	for _, repo := range e.Scope.Repos {
		repo = strings.ToLower(strings.TrimSpace(repo))
		if repo != "" && !contains(repos, repo) {
			repos = append(repos, repo)
		}
	}
	e.Scope.Repos = repos
}

// Validate says whether the entry may be recorded. Call Normalize first.
func (e Entry) Validate() error {
	if !idPattern.MatchString(e.ID) {
		return fmt.Errorf("the entry's ID %q is not valid", e.ID)
	}
	if e.Title == "" || e.Problem == "" || e.Cause == "" || e.Fix == "" {
		return fmt.Errorf("a learning says what went wrong (problem), why (cause) and what worked (fix), and has a title")
	}
	for name, field := range map[string]string{"problem": e.Problem, "cause": e.Cause, "fix": e.Fix} {
		if err := checkText(name, field, FieldLimit); err != nil {
			return err
		}
	}
	if err := checkText("title", e.Title, TitleLimit); err != nil {
		return err
	}
	if strings.Contains(e.Title, "\n") {
		return fmt.Errorf("the title is one line")
	}
	if len(e.Scope.Repos) > MaxRepos {
		return fmt.Errorf("a scope names at most %d repositories", MaxRepos)
	}
	for _, repo := range e.Scope.Repos {
		if !repoPattern.MatchString(repo) {
			return fmt.Errorf("repository %q is not owner/name", repo)
		}
	}
	switch e.Scope.Kind {
	case ScopeProject:
		if len(e.Scope.Repos) > 0 || e.Scope.Area != "" {
			return fmt.Errorf("a project scope names no repository and no area")
		}
	case ScopeRepository:
		if len(e.Scope.Repos) == 0 {
			return fmt.Errorf("a repository scope names at least one repository, as owner/name")
		}
	case ScopeArea:
		if e.Scope.Area == "" {
			return fmt.Errorf("an area scope names the area")
		}
		if err := checkText("area", e.Scope.Area, AreaLimit); err != nil {
			return err
		}
	default:
		return fmt.Errorf("the scope is project, repository or area, not %q", e.Scope.Kind)
	}
	for name, ref := range map[string]string{"escalation": e.Source.Escalation, "chat": e.Source.Chat} {
		if ref != "" && !refPattern.MatchString(ref) {
			return fmt.Errorf("the source %s %q is not an ID", name, ref)
		}
	}
	if e.State != Proposed && e.State != Accepted {
		return fmt.Errorf("an entry starts as proposed or accepted")
	}
	return nil
}

func contains(list []string, item string) bool {
	for _, have := range list {
		if have == item {
			return true
		}
	}
	return false
}

// ValidID says whether id can be an entry's ID.
func ValidID(id string) bool { return idPattern.MatchString(id) }

// Decide says the state an entry moves to when the action is taken on it, or
// why it may not be: only a proposed entry is accepted or rejected, and only
// an accepted one is retired.
func (e Entry) Decide(action string) (State, error) {
	switch {
	case action == "accept" && e.State == Proposed:
		return Accepted, nil
	case action == "reject" && e.State == Proposed:
		return Rejected, nil
	case action == "retire" && e.State == Accepted:
		return Retired, nil
	case action == "accept", action == "reject", action == "retire":
		return e.State, fmt.Errorf("a %s learning cannot be %sed: only a proposed one is accepted or rejected, and only an accepted one retired", e.State, action)
	default:
		return e.State, fmt.Errorf("the decision is accept, reject or retire, not %q", action)
	}
}
