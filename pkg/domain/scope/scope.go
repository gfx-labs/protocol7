// Package scope is what a chat may do on GitHub: a project, its repositories
// and GitHub App permissions. A scope never grows: the operations here only
// keep or narrow it. Policy (who gets which scope) is the platform's, not here.
package scope

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	naviv1 "github.com/gfx-labs/protocol7/pkg/transport/naviapi/navi/v1"
)

// Scope is what a chat may do on GitHub: which repositories, and with which
// permissions. A chat's authority is the lower of two things, what its project
// allows (the project's ceiling) and what the person who started it may do
// (the user's ceiling). Scope is their intersection, resolved once when navi
// starts the chat and stored on its request, so that a later policy change
// does not widen a chat that runs.
//
// It is the navi.v1 message: Project is the navi project, Repositories are
// owner/name in lower case and sorted, and Permissions are GitHub App
// permissions by name (contents, pull_requests, issues, checks, actions,
// statuses, metadata), each "read" or "write". A permission that is not
// listed is not held. A nil scope holds nothing.
//
// A scope never grows: Intersect is how a child chat, a handler or an
// escalation takes its parent's, and it cannot yield more than either.
type Scope = naviv1.Scope

// Level is the highest permission a scope holds.
type Level string

const (
	// LevelNone is a scope with no permission.
	LevelNone Level = "none"
	// LevelRead holds read permissions only.
	LevelRead Level = "read"
	// LevelWrite holds at least one write permission.
	LevelWrite Level = "write"
)

// Rank orders GitHub permission values: write is above read, which is above
// none. An unknown value ranks as none, so a typo never grants anything.
func rank(permission string) int {
	switch permission {
	case "read":
		return 1
	case "write":
		return 2
	case "admin":
		return 3
	}
	return 0
}

// LevelOf is the highest permission the scope holds.
func LevelOf(s *Scope) Level {
	level := LevelNone
	for _, permission := range s.GetPermissions() {
		switch rank(permission) {
		case 0:
		case 1:
			if level == LevelNone {
				level = LevelRead
			}
		default:
			return LevelWrite
		}
	}
	return level
}

// New returns a scope with its repositories in lower case, sorted and without
// repeats, and the permissions that are read, write or admin.
func New(project string, repositories []string, permissions map[string]string) *Scope {
	scope := &Scope{Project: project, Repositories: []string{}}
	for _, repository := range repositories {
		// GitHub names are not case sensitive.
		repository = strings.ToLower(repository)
		if !slices.Contains(scope.Repositories, repository) {
			scope.Repositories = append(scope.Repositories, repository)
		}
	}
	slices.Sort(scope.Repositories)
	for name, permission := range permissions {
		if rank(permission) > 0 {
			if scope.Permissions == nil {
				scope.Permissions = map[string]string{}
			}
			scope.Permissions[name] = permission
		}
	}
	return scope
}

// Intersect is what both scopes allow: the repositories that are in both, and
// for each permission the lower of the two (write above read above none). Two
// scopes of different projects have nothing in common.
func Intersect(s, other *Scope) *Scope {
	if s.GetProject() != other.GetProject() {
		return New(s.GetProject(), nil, nil)
	}
	var repositories []string
	for _, repository := range s.GetRepositories() {
		if HasRepository(other, repository) {
			repositories = append(repositories, repository)
		}
	}
	permissions := map[string]string{}
	for name, permission := range s.GetPermissions() {
		if theirs, ok := other.GetPermissions()[name]; ok {
			if rank(theirs) < rank(permission) {
				permission = theirs
			}
			permissions[name] = permission
		}
	}
	return New(s.GetProject(), repositories, permissions)
}

// Narrow is the scope of something that a chat with scope s starts, a child
// chat, a handler or an escalation: requested, cut to what the chat itself
// holds. Nil requests nothing narrower.
func Narrow(s, requested *Scope) *Scope {
	if requested == nil {
		return Clone(s)
	}
	return Intersect(s, requested)
}

// ReadOnly is the scope with every permission cut to read.
func ReadOnly(s *Scope) *Scope {
	permissions := map[string]string{}
	for name, permission := range s.GetPermissions() {
		if rank(permission) > 0 {
			permissions[name] = "read"
		}
	}
	return New(s.GetProject(), s.GetRepositories(), permissions)
}

// Clone is a copy of s that shares nothing with it.
func Clone(s *Scope) *Scope {
	return New(s.GetProject(), s.GetRepositories(), maps.Clone(s.GetPermissions()))
}

// Within says whether s allows nothing that other does not.
func Within(s, other *Scope) bool {
	if s.GetProject() != other.GetProject() {
		return false
	}
	for _, repository := range s.GetRepositories() {
		if !HasRepository(other, repository) {
			return false
		}
	}
	for name, permission := range s.GetPermissions() {
		if rank(permission) > rank(other.GetPermissions()[name]) {
			return false
		}
	}
	return true
}

// Equal says whether two scopes allow the same.
func Equal(s, other *Scope) bool { return Within(s, other) && Within(other, s) }

// HasRepository says whether repo (owner/name) is one of the scope's.
func HasRepository(s *Scope, repo string) bool {
	return slices.ContainsFunc(s.GetRepositories(), func(r string) bool { return strings.EqualFold(r, repo) })
}

// Allows says whether the scope holds permission at level or above.
func Allows(s *Scope, permission, level string) bool {
	return rank(s.GetPermissions()[permission]) >= rank(level) && rank(level) > 0
}

// Empty says whether the scope reaches no repository.
func Empty(s *Scope) bool { return len(s.GetRepositories()) == 0 }

// ParsePermissions reads "name=level;name=level".
func ParsePermissions(text string) (map[string]string, error) {
	permissions := map[string]string{}
	for _, entry := range strings.Split(text, ";") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		name, level, ok := strings.Cut(entry, "=")
		if !ok || name == "" || rank(level) == 0 {
			return nil, fmt.Errorf("the permission %q must be name=read or name=write", entry)
		}
		permissions[name] = level
	}
	return permissions, nil
}

// Key names the scope in a way that two scopes that allow the same share, and
// no two that differ do. Pool names and audit lines use it.
func Key(s *Scope) string {
	names := slices.Sorted(maps.Keys(s.GetPermissions()))
	permissions := make([]string, 0, len(names))
	for _, name := range names {
		permissions = append(permissions, name+"="+s.GetPermissions()[name])
	}
	return fmt.Sprintf("project=%s;repositories=%s;%s", s.GetProject(), strings.Join(s.GetRepositories(), ","), strings.Join(permissions, ";"))
}
