package learning

import (
	"fmt"
	"path"
	"strings"
)

// Lane says who may merge a change that a curator makes.
type Lane string

const (
	// LaneFast is a change to files that an app lists as safe for the
	// curator to merge itself, such as skills, docs and notes.
	LaneFast Lane = "fast"
	// LaneReviewed is every other change: it goes through the app's normal
	// review, which merges it.
	LaneReviewed Lane = "reviewed"
	// LanePerson is a change to a protected path, which a person merges.
	LanePerson Lane = "person"
)

// Lanes are an app's globs for the curator's two special lanes. A path that
// matches neither is reviewed. The patterns use "/" and "**", which matches
// any number of directories, and path.Match's "*", "?" and classes within
// one name. A pattern with no "/" matches the name at any depth.
type Lanes struct {
	// Fast are the files whose change the curator merges itself.
	Fast []string `json:"fast,omitempty"`
	// Person are the protected paths: a change to one needs a person.
	Person []string `json:"person,omitempty"`
}

// Protected are the paths that are never fast, whatever an app lists: the
// workflows that run CI and the files that say who must review. A curator
// that could change them could weaken the gates that check its own work. They
// need a person.
var Protected = []string{".github/**", "**/CODEOWNERS", "**/.github/**"}

// Validate says whether the patterns can be matched.
func (l Lanes) Validate() error {
	for _, pattern := range slices2(l.Fast, l.Person) {
		if strings.TrimSpace(pattern) == "" || strings.HasPrefix(pattern, "/") {
			return fmt.Errorf("the lane pattern %q is empty or starts with a slash", pattern)
		}
		for _, segment := range strings.Split(pattern, "/") {
			if segment == "**" {
				continue
			}
			if _, err := path.Match(segment, ""); err != nil {
				return fmt.Errorf("the lane pattern %q: %w", pattern, err)
			}
		}
	}
	return nil
}

func slices2(a, b []string) []string { return append(append([]string(nil), a...), b...) }

// Classify says the lane of a change that touches the paths, which are
// slash-separated and relative to the repository's root, and include the
// previous name of a renamed file. A change with no path, or a path that
// leaves the repository, is reviewed. One protected path makes it a person's.
// It is fast only when every path is fast.
func (l Lanes) Classify(paths []string) Lane {
	if len(paths) == 0 {
		return LaneReviewed
	}
	fast := true
	for _, p := range paths {
		p = strings.TrimPrefix(path.Clean("/"+p), "/")
		if p == "" || p == "." || strings.HasPrefix(p, "../") {
			return LaneReviewed
		}
		if matchAny(l.Person, p) || matchAny(Protected, p) {
			return LanePerson
		}
		if !matchAny(l.Fast, p) {
			fast = false
		}
	}
	if fast {
		return LaneFast
	}
	return LaneReviewed
}

func matchAny(patterns []string, name string) bool {
	for _, pattern := range patterns {
		if Match(pattern, name) {
			return true
		}
	}
	return false
}

// Match says whether the slash-separated name matches the pattern.
func Match(pattern, name string) bool {
	if !strings.Contains(pattern, "/") {
		// A bare name matches the file's name at any depth.
		_, base := path.Split(name)
		ok, _ := path.Match(pattern, base)
		return ok
	}
	return matchSegments(strings.Split(pattern, "/"), strings.Split(name, "/"))
}

func matchSegments(pattern, name []string) bool {
	for len(pattern) > 0 {
		if pattern[0] == "**" {
			// A trailing ** takes everything below, including nothing.
			if len(pattern) == 1 {
				return true
			}
			for i := 0; i <= len(name); i++ {
				if matchSegments(pattern[1:], name[i:]) {
					return true
				}
			}
			return false
		}
		if len(name) == 0 {
			return false
		}
		if ok, err := path.Match(pattern[0], name[0]); err != nil || !ok {
			return false
		}
		pattern, name = pattern[1:], name[1:]
	}
	return len(name) == 0
}
