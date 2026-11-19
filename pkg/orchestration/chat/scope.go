package chat

import (
	"fmt"
	"slices"
	"strings"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"

	"github.com/gfx-labs/protocol7/pkg/domain/scope"
)

// Effective scope.
//
// A chat's authority on GitHub is the lower of its project's and the person's
// who started it. navi resolves it once, when it starts the chat
// (acl.Subject.Scope), and stores it in Request.Scope. Everything below only
// narrows it: the repositories that are cloned, the pool a session joins, and
// the scope of what the chat starts.

// inheritScope is the scope of a chat, handler or escalation that a chat with
// the scope parent starts. requested may ask for less, never for more: the
// result is within parent. A parent with no recorded scope (a chat that
// started before scopes) has none to hand on.
func inheritScope(parent, requested *scope.Scope) *scope.Scope {
	if parent == nil {
		return nil
	}
	narrowed := parent.Narrow(requested)
	return &narrowed
}

// ScopedRepositories are the repositories of a chat that its scope reaches. A
// repository that is not on github.com, or is not in the scope, is not cloned.
// Without a scope, every repository is kept.
func ScopedRepositories(repositories []Repository, scope *scope.Scope) []Repository {
	return navi.ScopedRepositories(repositories, scope)
}

// scopeInstructions tell the agent what its scope is. The scope itself is
// enforced where the agent cannot change it: by what is cloned, by the
// credential and by the pool of the session.
func scopeInstructions(text *strings.Builder, scope *scope.Scope) {
	if scope == nil {
		return
	}
	permissions := make([]string, 0, len(scope.Permissions))
	for name, level := range scope.Permissions {
		permissions = append(permissions, name+": "+level)
	}
	slices.Sort(permissions)
	fmt.Fprintf(text, "\nYour GitHub access is the lower of the project's and your person's. Repositories: %s. Permissions: %s (%s). It cannot be widened from here: do not try to reach any other repository, and say that an admin has to change it if you need more.\n",
		strings.Join(scope.Repositories, ", "), strings.Join(permissions, ", "), scope.Level())
}
