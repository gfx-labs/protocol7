// Package kubeop places a short-lived Kubernetes credential in a session's
// workspace. It runs beside the core worker, which holds the workspaces. The
// platform that issues the credential (the broker) is not here: an app that
// hands its chats an operator credential issues it itself and asks this
// package's Writer, on the host's artifact queue, to place it.
package kubeop

import "time"

const (
	// Dir is the directory of the session's workspace that holds the token and
	// the kubeconfig. It is not in any clone, so nothing commits it.
	Dir = ".selfdev-operator"
	// Context is the name of the kubeconfig's only context.
	Context = "selfdev-operator"

	// ActivityWrite is the registered name of Writer.Write.
	ActivityWrite = "selfdev.kubeop.Write"
)

// ErrorRefused is the type of an error that asking again cannot change.
const ErrorRefused = "Refused"

// Basis says why a chat is eligible.
type Basis string

const (
	// BasisStarted is a chat that an admin started in the page.
	BasisStarted Basis = "started"
	// BasisIssue is the chat of an issue that an admin labelled.
	BasisIssue Basis = "issue"
)

// Credentials are what the broker returns for the writer to place. The token
// is in the workflow's history until the history is deleted, so it lives an
// hour and no longer.
type Credentials struct {
	Token   string    `json:"token"`
	Expires time.Time `json:"expires"`
	// Server is the API server's address and CA its certificate authority, in
	// PEM.
	Server string `json:"server"`
	CA     string `json:"ca"`
	// By is the admin: the login who started the chat, or the GitHub login
	// that labelled the issue. Basis says which.
	By    string `json:"by"`
	Basis Basis  `json:"basis"`
}

// WriteRequest asks the writer to place credentials in a session's workspace.
type WriteRequest struct {
	Session     string      `json:"session"`
	Credentials Credentials `json:"credentials"`
}

// WriteResult says where the files are, as the agent sees them.
type WriteResult struct {
	Kubeconfig string `json:"kubeconfig"`
	TokenFile  string `json:"token_file"`
}
