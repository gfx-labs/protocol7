package kubeop

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// ActivityIssue is the registered name of the broker's Issue, which the app
// that hands out the operator credential registers.
const ActivityIssue = "selfdev.kubeop.Issue"

// Lifetime is how long a token lives. Asking again renews it.
const Lifetime = time.Hour

// CoreRepositories are the only repositories a chat that gets the credential
// may work on.
var CoreRepositories = []string{"gfx-labs/protocol7", "gfx-labs/protocol7-selfdev", "gfx-labs/pdp", "gfx-labs/towa", "gfx-labs/go-acp", "gfx-labs/acphost"}

// Issue is the issue an issue chat works on.
type Issue struct {
	Repo   string `json:"repo"`
	Number int    `json:"number"`
}

// Subject is what the decision is made from: the chat's request, as the
// workflow holds it, and the message that is being answered.
type Subject struct {
	Chat string `json:"chat"`
	// StartedBy is the login of the person who started the chat. Empty for a
	// chat that no person started.
	StartedBy string `json:"started_by,omitempty"`
	// Project is the navi project. Empty is the core project.
	Project string `json:"project,omitempty"`
	// Restricted is set when the chat's person is not an admin.
	Restricted bool `json:"restricted,omitempty"`
	// MessageRestricted is set when the message of the running turn came from
	// someone who is not an admin.
	MessageRestricted bool   `json:"message_restricted,omitempty"`
	Parent            string `json:"parent,omitempty"`
	Escalation        bool   `json:"escalation,omitempty"`
	Handler           bool   `json:"handler,omitempty"`
	// Kubernetes is set for a chat on the kubernetes backend.
	Kubernetes bool `json:"kubernetes,omitempty"`
	// Repositories are the clones' repositories as owner/name, or the clone
	// address of one that is not on GitHub.
	Repositories []string `json:"repositories"`
	Issue        *Issue   `json:"issue,omitempty"`
}

// Check says whether the chat may have the credential, and on what basis. The
// answer is an admin's chat in the core project on the host backend, working
// only on protocol7 and protocol7-selfdev, and no one else's. An issue chat is
// eligible by its basis here and the broker then reads who labelled the issue.
func Check(s Subject) (Basis, error) {
	refuse := func(reason string) (Basis, error) {
		return "", fmt.Errorf("the operator credential is refused: %s", reason)
	}
	switch {
	case s.Chat == "":
		return refuse("no chat")
	case s.Project != "":
		return refuse("only chats of the core project get it")
	case s.Restricted:
		return refuse("the chat was not started by an admin")
	case s.MessageRestricted:
		return refuse("the message being answered is not from an admin")
	case s.Parent != "":
		return refuse("a child chat does not get it")
	case s.Escalation || s.Handler:
		return refuse("an escalation does not get it")
	case s.Kubernetes:
		return refuse("only a chat on the host backend gets it: the credential is written into the host's workspace")
	case len(s.Repositories) == 0:
		return refuse("the chat has no repository")
	}
	for _, repo := range s.Repositories {
		if !slices.Contains(CoreRepositories, repo) {
			return refuse(fmt.Sprintf("the chat clones %q, and only the core repositories (%s) are allowed", repo, strings.Join(CoreRepositories, ", ")))
		}
	}
	if s.Issue != nil {
		if !slices.Contains(CoreRepositories, s.Issue.Repo) || s.Issue.Number <= 0 {
			return refuse("the issue is not in a core repository")
		}
		return BasisIssue, nil
	}
	if strings.TrimSpace(s.StartedBy) == "" {
		return refuse("no admin started the chat")
	}
	return BasisStarted, nil
}

// Issued is what the chat keeps of a grant, for navi. It holds no token.
type Issued struct {
	At      time.Time `json:"at"`
	Expires time.Time `json:"expires"`
	By      string    `json:"by"`
	Basis   Basis     `json:"basis"`
}

// Access is the record of the chat's grants that navi shows.
type Access struct {
	// Count is how many tokens were issued.
	Count int `json:"count"`
	// Last are the most recent grants, newest last.
	Last []Issued `json:"last,omitempty"`
}

// MaxRecorded bounds Access.Last.
const MaxRecorded = 8

// Record adds a grant.
func (a *Access) Record(grant Issued) {
	a.Count++
	a.Last = append(a.Last, grant)
	if len(a.Last) > MaxRecorded {
		a.Last = a.Last[len(a.Last)-MaxRecorded:]
	}
}
