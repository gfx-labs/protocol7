// Package scope is what a chat may do on GitHub: a project, its repositories
// and GitHub App permissions. A scope never grows: the operations here only
// keep or narrow it. Policy (who gets which scope) is the platform's, not here.
package scope

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Scope is what a chat may do on GitHub: which repositories, and with which
// permissions. A chat's authority is the lower of two things, what its project
// allows (the project's ceiling) and what the person who started it may do
// (the user's ceiling). Scope is their intersection, resolved once when navi
// starts the chat and stored on its request, so that a later policy change
// does not widen a chat that runs.
//
// A scope never grows: Intersect is how a child chat, a handler or an
// escalation takes its parent's, and it cannot yield more than either.
type Scope struct {
	// Project is the navi project's name. Empty or a name, as the platform defines it.
	Project string `json:"project"`
	// Repositories are owner/name in lower case, sorted.
	Repositories []string `json:"repositories"`
	// Permissions are GitHub App permissions by name: contents, pull_requests,
	// issues, checks, actions, statuses and metadata. The value is "read" or
	// "write". A permission that is not listed is not held.
	Permissions map[string]string `json:"permissions,omitempty"`
}

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

// Level is the highest permission the scope holds.
func (s Scope) Level() Level {
	level := LevelNone
	for _, permission := range s.Permissions {
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

// NewScope returns a scope with its repositories in lower case, sorted and
// without repeats,
// and the permissions that are read, write or admin.
func NewScope(project string, repositories []string, permissions map[string]string) Scope {
	scope := Scope{Project: project, Repositories: []string{}}
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
func (s Scope) Intersect(other Scope) Scope {
	if s.Project != other.Project {
		return NewScope(s.Project, nil, nil)
	}
	var repositories []string
	for _, repository := range s.Repositories {
		if slices.ContainsFunc(other.Repositories, func(r string) bool { return strings.EqualFold(r, repository) }) {
			repositories = append(repositories, repository)
		}
	}
	permissions := map[string]string{}
	for name, permission := range s.Permissions {
		if theirs, ok := other.Permissions[name]; ok {
			if rank(theirs) < rank(permission) {
				permission = theirs
			}
			permissions[name] = permission
		}
	}
	return NewScope(s.Project, repositories, permissions)
}

// Narrow is the scope of something that a chat with this scope starts, a
// child chat, a handler or an escalation: requested, cut to what the chat
// itself holds. Nil requests nothing narrower.
func (s Scope) Narrow(requested *Scope) Scope {
	if requested == nil {
		return s.clone()
	}
	return s.Intersect(*requested)
}

// ReadOnly is the scope with every permission cut to read.
func (s Scope) ReadOnly() Scope {
	permissions := map[string]string{}
	for name, permission := range s.Permissions {
		if rank(permission) > 0 {
			permissions[name] = "read"
		}
	}
	return NewScope(s.Project, s.Repositories, permissions)
}

func (s Scope) clone() Scope {
	return NewScope(s.Project, s.Repositories, maps.Clone(s.Permissions))
}

// Within says whether the scope allows nothing that other does not.
func (s Scope) Within(other Scope) bool {
	if s.Project != other.Project {
		return false
	}
	for _, repository := range s.Repositories {
		if !other.HasRepository(repository) {
			return false
		}
	}
	for name, permission := range s.Permissions {
		if rank(permission) > rank(other.Permissions[name]) {
			return false
		}
	}
	return true
}

// Equal says whether two scopes allow the same.
func (s Scope) Equal(other Scope) bool { return s.Within(other) && other.Within(s) }

// HasRepository says whether repo (owner/name) is one of the scope's.
func (s Scope) HasRepository(repo string) bool {
	return slices.ContainsFunc(s.Repositories, func(r string) bool { return strings.EqualFold(r, repo) })
}

// Allows says whether the scope holds permission at level or above.
func (s Scope) Allows(permission, level string) bool {
	return rank(s.Permissions[permission]) >= rank(level) && rank(level) > 0
}

// Empty says whether the scope reaches no repository.
func (s Scope) Empty() bool { return len(s.Repositories) == 0 }

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
func (s Scope) Key() string {
	names := slices.Sorted(maps.Keys(s.Permissions))
	permissions := make([]string, 0, len(names))
	for _, name := range names {
		permissions = append(permissions, name+"="+s.Permissions[name])
	}
	return fmt.Sprintf("project=%s;repositories=%s;%s", s.Project, strings.Join(s.Repositories, ","), strings.Join(permissions, ";"))
}
