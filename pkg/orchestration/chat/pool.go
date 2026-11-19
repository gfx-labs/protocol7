package chat

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/gfx-labs/protocol7/pkg/domain/scope"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// Session pools.
//
// On the kubernetes backend a chat's session runs on a worker of a pool
// (protocol7's SessionSpec.Pool): up to SessionsPerWorker sessions on each of
// up to MaxWorkers pods, started on demand. The sessions of a worker see each
// other's files, processes and queue, and a pool has one identity, so a pool
// may hold only sessions with identical permissions. PoolKey derives the name
// from everything that decides what a session may do. protocol7 never reads
// meaning into it.

// Pool sizes. The workflow that opens the session chooses them in Go. A chat
// has no use for more than a few agents at once on one worker's memory, and
// the pods are started on demand.
const (
	PoolMaxWorkers        = 3
	PoolSessionsPerWorker = 3
	PoolIdleTimeout       = 10 * time.Minute
)

// poolNameLength is the number of hex digits of the pool's name. It becomes
// part of pod names, queue names, a SPIFFE ID and an object path.
const poolNameLength = 24

// PoolKey is the name of the pool of a chat's session. Two chats share a pool
// only if all of this matches:
//
//   - the project, which decides who may work on it;
//   - the repositories that are cloned (name and URL), which decide the
//     token's reach;
//   - whether the person is an admin (Restricted);
//   - the chat's kind: whether it works alone on an issue (and which issue,
//     because its tools act on that issue), is an escalation, or is the
//     escalation handler and the escalation it serves. The tools of a chat
//     are answered by its workflow, and a session that can read another's ID from the files of their shared
//     worker can call them, so sessions with other tools must not share one;
//   - the model accounts the chat may use, which decide the credentials PDP
//     uses for it. Entries may list fallbacks, so every account of every entry
//     counts, in sorted order;
//   - the chat's effective scope (Request.Scope: project, repositories and
//     permissions, the lower of the project's and the person's), so that chats
//     with different scopes never share a pod and the token it holds;
//   - scope: what the deployment gives every session pod, the GitHub token's
//     repositories and permissions. A change of it makes new names, so a
//     session never joins a pool whose pods hold another token.
//
// What is left out on purpose: the chat's ID, title, message, depth and parent.
// Depth and Parent only change how many children a chat may spawn and where an
// escalation's forwarding goes, in the chat's workflow. None of them widens a
// token, an account or the egress of the session, and a parent and its children
// must share a pool, which they could not if the depth were in the key.
//
// Handlers of escalations (handlerChat) do not copy the scope, so today they
// get a pod of their own. The key has them (Handler, Serves) so that copying it
// later does not put handlers of different escalations in one pool.
//
// Operator access to the cluster (#80), when it exists, is one more: a chat
// with it must never share a pool with one without.
//
// An empty scope turns pools off: the chat gets a pod of its own.
func PoolKey(request *Request, poolScope string) string {
	if poolScope == "" {
		return ""
	}
	repositories := make([][2]string, 0, len(request.Repositories))
	for _, repository := range request.Repositories {
		repositories = append(repositories, [2]string{repository.Name, repository.Url})
	}
	slices.SortFunc(repositories, func(a, b [2]string) int { return strings.Compare(a[0]+"\x00"+a[1], b[0]+"\x00"+b[1]) })
	issue := ""
	if request.Issue != nil {
		issue = fmt.Sprintf("%s#%d", request.Issue.Repo, request.Issue.Number)
	}
	serves := ""
	if request.Serves != nil {
		serves = request.Serves.Escalation + "\x00" + request.Serves.Chat
	}
	effective := ""
	if request.Scope != nil {
		effective = scope.Key(request.Scope)
	}
	// JSON keeps every value apart from its neighbours, so no two sets of
	// permissions encode alike.
	encoded, err := json.Marshal(struct {
		Version      int
		Project      string
		Repositories [][2]string
		Restricted   bool
		Issue        string
		Escalation   bool
		Handler      bool
		Serves       string
		Accounts     []string
		Scope        string
		// Effective is the chat's own scope, which decides what its token may
		// reach. Left out when none was recorded, so the name of a chat that
		// started before scopes does not change.
		Effective string `json:",omitempty"`
	}{1, request.Project, repositories, request.Restricted, issue, request.Escalation != nil, request.Handler, serves, modelAccounts(request.Models), poolScope, effective})
	if err != nil {
		// A struct of strings and booleans always encodes.
		panic(err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])[:poolNameLength]
}

// modelAccounts are the PDP accounts of a chat's model list, sorted and
// without repeats, as the limit of a turn names them: the part of each entry
// before its first "/". An empty list is the default account.
func modelAccounts(entries []string) []string {
	var accounts []string
	for _, entry := range entries {
		for _, model := range strings.Split(entry, ",") {
			account, _, _ := strings.Cut(strings.TrimSpace(model), "/")
			if account != "" && !slices.Contains(accounts, account) {
				accounts = append(accounts, account)
			}
		}
	}
	if len(accounts) == 0 {
		accounts = append(accounts, "default")
	}
	slices.Sort(accounts)
	return accounts
}

// withPool returns the request with its Pool named from what it is now: the
// key of its permissions under the scope it has, on the kubernetes backend, and
// otherwise none. A name that an earlier opening gave is not kept: a chat that
// opens its session again after the deployment turned pools off, or changed the
// token's scope, gets the pool its permissions name today, or a pod of its own.
func withPool(request *Request) *Request {
	request.Pool = ""
	if request.PoolScope != "" && request.Backend == p7.BackendKubernetes {
		request.Pool = PoolKey(request, request.PoolScope)
	}
	return request
}

// poolSpec is the pool of a request, with the sizes this package chooses. Nil
// is a pod of the session's own.
func poolSpec(request *Request) *p7.SessionPool {
	if request.Pool == "" || request.Backend != p7.BackendKubernetes {
		return nil
	}
	return &p7.SessionPool{
		Name: request.Pool, MaxWorkers: PoolMaxWorkers, SessionsPerWorker: PoolSessionsPerWorker, IdleTimeout: PoolIdleTimeout,
	}
}
