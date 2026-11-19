package navi

import (
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain/scope"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// Kind says what a chat is to the page. AttributeKind holds it.
type Kind string

const (
	// KindChat is a person's chat, started from the page.
	KindChat Kind = "chat"
	// KindSession is an agent session that another workflow started and talks
	// to. The page shows it read-only.
	KindSession Kind = "session"
	// KindEscalation is an escalation workflow, or an older chat the system
	// opened to ask a person something.
	KindEscalation Kind = "escalation"
	// KindLearning is what AttributeKind says of a learning.
	KindLearning Kind = "learning"
)

// Person is someone who acted through the page, as the tailnet named them.
type Person struct {
	// Login is the Tailscale login name, such as alice@example.com.
	Login string `json:"login"`
	// Name is the display name.
	Name string `json:"name,omitempty"`
}

// Repository is one repository cloned into a chat's session.
type Repository struct {
	// Name is the directory the clone is placed in.
	Name string `json:"name"`
	URL  string `json:"url"`
}

// Action is the payload of SignalStop and SignalClose.
type Action struct {
	By *Person `json:"by,omitempty"`
	// Discard makes a stop drop the waiting messages as well.
	Discard bool `json:"discard,omitempty"`
}

// Dequeue is the payload of SignalDequeue.
type Dequeue struct {
	ID string  `json:"id"`
	By *Person `json:"by,omitempty"`
}

// Archive is the payload of UpdateArchive.
type Archive struct {
	Archived bool    `json:"archived"`
	By       *Person `json:"by,omitempty"`
}

// Board is a design board shown beside a chat.
type Board struct {
	// Dir is the board's directory, relative to the session's workspace.
	Dir string `json:"dir"`
}

// Escalation is set on an older chat the system opened to ask a person
// something.
type Escalation struct {
	Reason string `json:"reason"`
}

// Serving names what an escalation handler's chat serves.
type Serving struct {
	Escalation string `json:"escalation"`
	Chat       string `json:"chat"`
}

// Issue names a GitHub issue.
type Issue struct {
	// Repo is owner/name.
	Repo   string `json:"repo"`
	Number int    `json:"number"`
}

// ChatRequest describes a new chat. It is what navi starts a chat with.
type ChatRequest struct {
	ID        string  `json:"id"`
	Title     string  `json:"title"`
	StartedBy *Person `json:"started_by,omitempty"`
	// Host names the machine whose core worker runs the session.
	Host string `json:"host"`
	// Backend is "kubernetes" for a pod of its own, anything else the host
	// backend on Host.
	Backend      string       `json:"backend,omitempty"`
	Model        string       `json:"model,omitempty"`
	Models       []string     `json:"models,omitempty"`
	Repositories []Repository `json:"repositories"`
	Issue        *Issue       `json:"issue,omitempty"`
	// Owner is the ID of a workflow that gets a Report after each turn.
	Owner      string      `json:"owner,omitempty"`
	Escalation *Escalation `json:"escalation,omitempty"`
	// Project is the navi project. Empty is the core project.
	Project    string `json:"project,omitempty"`
	Restricted bool   `json:"restricted,omitempty"`
	// Scope is the chat's effective GitHub authority, resolved once by navi.
	Scope      *scope.Scope `json:"scope,omitempty"`
	Parent     string       `json:"parent,omitempty"`
	Depth      int          `json:"depth,omitempty"`
	Handler    bool         `json:"handler,omitempty"`
	PoolScope  string       `json:"pool_scope,omitempty"`
	Pool       string       `json:"pool,omitempty"`
	Serves     *Serving     `json:"serves,omitempty"`
	ReviewHint string       `json:"review_hint,omitempty"`
}

// Kind is what the chat is to the page.
func (r ChatRequest) Kind() Kind {
	switch {
	case r.Escalation != nil:
		return KindEscalation
	case r.Owner != "":
		return KindSession
	default:
		return KindChat
	}
}

// Pick is one element the person picked on a design board.
type Pick struct {
	Artboard string `json:"artboard"`
	Page     string `json:"page"`
	Selector string `json:"selector"`
	Tag      string `json:"tag"`
	Text     string `json:"text,omitempty"`
	HTML     string `json:"html,omitempty"`
	Box      struct {
		X      int `json:"x"`
		Y      int `json:"y"`
		Width  int `json:"width"`
		Height int `json:"height"`
	} `json:"box"`
	Styles map[string]string `json:"styles,omitempty"`
}

// Message is the payload of SignalMessage. Its ID is that of the turn that
// answers it.
type Message struct {
	ID         string  `json:"id"`
	Text       string  `json:"text"`
	From       *Person `json:"from,omitempty"`
	Restricted bool    `json:"restricted,omitempty"`
	Picks      []Pick  `json:"picks,omitempty"`
}

// Report is the payload of SignalReport, and a chat's last result.
type Report struct {
	ChatID     string         `json:"chat_id,omitempty"`
	Attention  bool           `json:"attention,omitempty"`
	Message    string         `json:"message,omitempty"`
	StopReason string         `json:"stop_reason,omitempty"`
	Text       string         `json:"text,omitempty"`
	Error      string         `json:"error,omitempty"`
	Stopped    bool           `json:"stopped,omitempty"`
	StoppedBy  string         `json:"stopped_by,omitempty"`
	Resumes    int            `json:"resumes,omitempty"`
	GaveUp     bool           `json:"gave_up,omitempty"`
	Idle       bool           `json:"idle"`
	Ended      bool           `json:"ended,omitempty"`
	Escalation *EscalationRef `json:"escalation,omitempty"`
}

// ChatStatus says what a chat is doing.
type ChatStatus string

// Statuses of a chat.
const (
	ChatOpening   ChatStatus = "opening"
	ChatIdle      ChatStatus = "idle"
	ChatRunning   ChatStatus = "running"
	ChatEscalated ChatStatus = "escalated"
	ChatClosing   ChatStatus = "closing"
	ChatClosed    ChatStatus = "closed"
	ChatFailed    ChatStatus = "failed"
)

// Probe is a probe a chat deployed.
type Probe struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

// Child is a child chat owned by a chat.
type Child struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Message   string `json:"message,omitempty"`
	Done      bool   `json:"done"`
	Attention bool   `json:"attention,omitempty"`
	Result    Report `json:"result"`
}

// OperatorBasis is why an operator token was issued.
type OperatorBasis string

const (
	// OperatorBasisStarted is a chat that an admin started in the page.
	OperatorBasisStarted OperatorBasis = "started"
	// OperatorBasisIssue is the chat of an issue that an admin labelled.
	OperatorBasisIssue OperatorBasis = "issue"
)

// OperatorIssued is one operator token grant. It holds no token.
type OperatorIssued struct {
	At      time.Time     `json:"at"`
	Expires time.Time     `json:"expires"`
	By      string        `json:"by"`
	Basis   OperatorBasis `json:"basis"`
}

// OperatorAccess is the record of a chat's operator grants that navi shows.
type OperatorAccess struct {
	Count int              `json:"count"`
	Last  []OperatorIssued `json:"last,omitempty"`
}

// ChatState is what QueryState returns from a chat.
type ChatState struct {
	Probes       []Probe           `json:"probes,omitempty"`
	Operator     *OperatorAccess   `json:"operator,omitempty"`
	Children     []Child           `json:"children,omitempty"`
	Escalations  []EscalationRef   `json:"escalations,omitempty"`
	Attention    bool              `json:"attention,omitempty"`
	Result       Report            `json:"result"`
	ModelLeases  []*p7.Lease       `json:"model_leases,omitempty"`
	Request      ChatRequest       `json:"request"`
	Status       ChatStatus        `json:"status"`
	Session      *p7.Session       `json:"session,omitempty"`
	Pending      []Message         `json:"pending,omitempty"`
	Current      *Message          `json:"current,omitempty"`
	Turns        int               `json:"turns"`
	Closing      bool              `json:"closing,omitempty"`
	Resumes      int               `json:"resumes,omitempty"`
	Senders      map[string]string `json:"senders,omitempty"`
	StoppedBy    *Person           `json:"stopped_by,omitempty"`
	ClosedBy     *Person           `json:"closed_by,omitempty"`
	Briefed      bool              `json:"briefed,omitempty"`
	Learnings    int               `json:"learnings,omitempty"`
	LastLearning string            `json:"last_learning,omitempty"`
	Archived     bool              `json:"archived,omitempty"`
	Board        *Board            `json:"board,omitempty"`
	LastStop     string            `json:"last_stop,omitempty"`
	Error        string            `json:"error,omitempty"`
}
