// Package learning is the learnings log: what went wrong, why, and what
// worked, recorded once so that no chat finds it out again.
//
// The SDK provides the log and its storage: the Entry, Scope, Source and
// State types and their validation (with the secret screen), the workflow
// that holds an entry, the Store that records, lists, gets and decides, and
// the Record and Decide activities, which only write the log. It holds no
// curation and no prompt text. How a chat finds, ranks, briefs and words the
// entries (search, a brief for a first prompt, duplicate holding, areas,
// embeddings, the chat tools and their instructions) is for an app to build on
// the log.
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
//     pull request, a change that comes from an entry has in the curator's
//     pull request, which the entry links.
//
// # Open until settled
//
// Nobody accepts an entry. A recorded entry is open, and an app's chats read
// it from then on. A curator, which an app builds on the mechanics of this
// package (curate.go), settles it: applied, when a change in a pull request
// carries it (the entry links the PR), covered, when a line of the code or
// the docs says it already, or rejected, with the reason. An entry that is
// none of these stays open, to join a later recurrence. A person may still
// override: reject an open entry, or retire an applied one.
//
// A covered, rejected or retired entry ends its workflow and ages out with
// Temporal's retention. An open, accepted or applied entry runs, and nothing
// else is ever deleted.
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
	// Open is an entry that is recorded and not settled. An app's chats
	// read it, and the curator settles it.
	Open State = "open"
	// Proposed is what an open entry was called when a person or the handler
	// had to accept it. The entries that were recorded then are still in the
	// log under it, and it means Open.
	Proposed State = "proposed"
	// Applied is an entry that the curator turned into a change. Its PR is
	// the link. An app's chats read it, and the workflow runs on.
	Applied State = "applied"
	// Covered is an entry that a line of the code or the docs says already.
	// Its Note says which. It ends.
	Covered State = "covered"
	// Accepted is an entry that the handler or a person accepted, before the
	// curator. An app's chats read it.
	Accepted State = "accepted"
	// Rejected and Retired are the ends: the workflow completes. A
	// rejection carries its reason in Note.
	Rejected State = "rejected"
	Retired  State = "retired"
)

// IsOpen says whether the state is an unsettled one: Open, or Proposed from
// before the curator.
func (s State) IsOpen() bool { return s == Open || s == Proposed }

// Readable says whether an app's chats read an entry in this state: the open
// ones, the applied ones and the accepted ones from before the curator.
func (s State) Readable() bool { return s.IsOpen() || s == Applied || s == Accepted }

// Ended says whether the entry's workflow completes in this state.
func (s State) Ended() bool { return s == Covered || s == Rejected || s == Retired }

// Actions of a Decision.
const (
	// ActionAccept keeps an open entry as accepted. A person's override from
	// before the curator, which is still allowed.
	ActionAccept = "accept"
	// ActionReject ends an open entry, with a reason in the note.
	ActionReject = "reject"
	// ActionRetire ends an accepted or applied entry.
	ActionRetire = "retire"
	// ActionApply settles an open entry as applied, with the PR.
	ActionApply = "apply"
	// ActionCover settles an open entry as covered, with a note that names
	// the line that says it already.
	ActionCover = "cover"
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
	// PR is the pull request that applied the entry, Note the reason an
	// entry was covered or rejected, Pass the curator pass that settled it,
	// SettledBy who decided it and Settled when. A decision sets them, and
	// the workflow keeps them in the memo with the entry.
	PR        string    `json:"pr,omitempty"`
	Note      string    `json:"note,omitempty"`
	Pass      string    `json:"pass,omitempty"`
	SettledBy string    `json:"settled_by,omitempty"`
	Settled   time.Time `json:"settled,omitzero"`
	// Vector is an embedding of the entry and VectorModel the model that
	// made it. They are opaque to the SDK, which does not compute them: an
	// app that curates the log sets them before it records the entry, and
	// they are kept in the memo with the rest. An entry has none when its
	// app did not set them.
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
// mistakes, and not a guarantee: an app's tools should tell the agent to
// write no secrets either.
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
	if e.State != Open && e.State != Proposed && e.State != Accepted {
		return fmt.Errorf("an entry starts as open")
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
// why it may not be: only an open entry is accepted, rejected, applied or
// covered, and only an accepted or applied one is retired.
func (e Entry) Decide(action string) (State, error) {
	switch {
	case action == ActionAccept && e.State.IsOpen():
		return Accepted, nil
	case action == ActionReject && e.State.IsOpen():
		return Rejected, nil
	case action == ActionApply && e.State.IsOpen():
		return Applied, nil
	case action == ActionCover && e.State.IsOpen():
		return Covered, nil
	case action == ActionRetire && (e.State == Accepted || e.State == Applied):
		return Retired, nil
	case action == ActionAccept, action == ActionReject, action == ActionApply, action == ActionCover, action == ActionRetire:
		return e.State, fmt.Errorf("a %s learning cannot be %s: only an open one is accepted, rejected, applied or covered, and only an accepted or applied one retired", e.State, past(action))
	default:
		return e.State, fmt.Errorf("the decision is accept, reject, retire, apply or cover, not %q", action)
	}
}

func past(action string) string {
	if strings.HasSuffix(action, "e") {
		return action + "d"
	}
	return action + "ed"
}

// PRLimit bounds the pull request link of an entry.
const PRLimit = 300

var prPattern = regexp.MustCompile(`^https://[A-Za-z0-9.-]+/[^\s]+$`)

// CheckDecision says whether the decision carries what its action needs: an apply
// links its pull request, and a cover says which line covers the entry. Its
// note is a short text like an entry's, with no secret.
func CheckDecision(d Decision) error {
	switch d.Action {
	case ActionApply:
		if d.PR == "" {
			return fmt.Errorf("an applied learning links the pull request that carries the change")
		}
	case ActionCover:
		if strings.TrimSpace(d.Note) == "" {
			return fmt.Errorf("a covered learning says which line of the code or the docs covers it, in the note")
		}
	}
	if d.PR != "" {
		if d.Action != ActionApply {
			return fmt.Errorf("only an apply links a pull request")
		}
		if len(d.PR) > PRLimit || !prPattern.MatchString(d.PR) {
			return fmt.Errorf("the pull request %q is not an https link", d.PR)
		}
	}
	if err := checkText("note", d.Note, FieldLimit); err != nil {
		return err
	}
	for name, ref := range map[string]string{"by": d.By, "pass": d.Pass} {
		if utf8.RuneCountInString(ref) > 120 || strings.ContainsAny(ref, "\n\r") {
			return fmt.Errorf("the decision's %s is one short line", name)
		}
	}
	return nil
}

// Settle applies the decision to the entry at the time now, or says why it
// may not be: the action must fit the state (Decide) and carry what it needs
// (CheckDecision). The returned entry has the new state, and for a decision
// that settles it, the PR, note, pass, who and when.
func (e Entry) Settle(d Decision, now time.Time) (Entry, error) {
	next, err := e.Decide(d.Action)
	if err != nil {
		return e, err
	}
	if err := CheckDecision(d); err != nil {
		return e, err
	}
	e.State = next
	switch d.Action {
	case ActionApply, ActionCover, ActionReject:
		e.PR, e.Note, e.Pass, e.SettledBy, e.Settled = d.PR, strings.TrimSpace(d.Note), d.Pass, d.By, now.UTC()
	}
	return e, nil
}
