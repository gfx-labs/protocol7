package learning_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gfx-labs/protocol7/pkg/orchestration/learning"
)

// A change is fast only when every path is, a protected path makes it a
// person's whatever the app lists, and anything else is reviewed.
func TestLanes(t *testing.T) {
	lanes := learning.Lanes{
		Fast:   []string{"docs/**/*.md", "README.md", "skills/**", "kubernetes/skills/**", "AGENTS.md", "**/.github/**"},
		Person: []string{"kubernetes/policy/**", "*.pem"},
	}
	require.NoError(t, lanes.Validate())
	for name, test := range map[string]struct {
		paths []string
		want  learning.Lane
	}{
		"a doc":                    {[]string{"docs/status.md"}, learning.LaneFast},
		"a nested doc":             {[]string{"docs/a/b/c.md"}, learning.LaneFast},
		"a readme in the root":     {[]string{"README.md"}, learning.LaneFast},
		"a readme deeper":          {[]string{"pkg/x/README.md"}, learning.LaneFast},
		"a skill and a doc":        {[]string{"skills/observe/SKILL.md", "docs/plan.md"}, learning.LaneFast},
		"a prompt is not a doc":    {[]string{"pkg/workflow/chat/prompts/intro.md"}, learning.LaneReviewed},
		"a doc and code":           {[]string{"docs/plan.md", "pkg/a.go"}, learning.LaneReviewed},
		"code":                     {[]string{"pkg/a.go"}, learning.LaneReviewed},
		"a doc that is not md":     {[]string{"docs/diagram.png"}, learning.LaneReviewed},
		"no paths":                 {nil, learning.LaneReviewed},
		"a path out of the repo":   {[]string{"../etc/passwd"}, learning.LaneReviewed},
		"a protected path":         {[]string{"kubernetes/policy/egress.yaml"}, learning.LanePerson},
		"a person's glob at depth": {[]string{"docs/key.pem"}, learning.LanePerson},
		"a workflow is a person's": {[]string{".github/workflows/ci.yml"}, learning.LanePerson},
		"even if the app lists it": {[]string{"x/.github/workflows/ci.yml"}, learning.LanePerson},
		"codeowners":               {[]string{"CODEOWNERS"}, learning.LanePerson},
		"codeowners deeper":        {[]string{"docs/CODEOWNERS"}, learning.LanePerson},
		"one protected among fast": {[]string{"docs/a.md", ".github/CODEOWNERS"}, learning.LanePerson},
		"a rename out of docs":     {[]string{"docs/a.md", "pkg/b.go"}, learning.LaneReviewed},
		"a dirty path":             {[]string{"docs/../pkg/a.go"}, learning.LaneReviewed},
	} {
		t.Run(name, func(t *testing.T) { assert.Equal(t, test.want, lanes.Classify(test.paths)) })
	}
	assert.Error(t, learning.Lanes{Fast: []string{"/abs"}}.Validate())
	assert.Error(t, learning.Lanes{Fast: []string{"docs/[a"}}.Validate())
	assert.Error(t, learning.Lanes{Person: []string{" "}}.Validate())
}

func TestMatch(t *testing.T) {
	for pattern, names := range map[string]map[string]bool{
		"a/**/b":   {"a/b": true, "a/x/b": true, "a/x/y/b": true, "a/x/c": false, "b": false},
		"**/c.md":  {"c.md": true, "a/c.md": true, "a/b/c.md": true, "a/c.mdx": false},
		"a/*":      {"a/b": true, "a/b/c": false, "a": false},
		"*.md":     {"x.md": true, "a/b/x.md": true, "x.txt": false},
		"docs/**":  {"docs": true, "docs/a": true, "docs/a/b": true, "doc/a": false},
		"a/?.md":   {"a/b.md": true, "a/bc.md": false},
		"a/[bc].x": {"a/b.x": true, "a/d.x": false},
	} {
		for name, want := range names {
			assert.Equal(t, want, learning.Match(pattern, name), "%s ~ %s", pattern, name)
		}
	}
}
