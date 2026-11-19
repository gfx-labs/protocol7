package scope_test

import (
	"slices"
	"testing"

	"github.com/gfx-labs/protocol7/pkg/domain/scope"
)

func TestScopeIntersect(t *testing.T) {
	a := scope.NewScope("oku", []string{"o/a", "o/b"}, map[string]string{"contents": "write", "issues": "write", "checks": "read"})
	b := scope.NewScope("oku", []string{"O/B", "o/c"}, map[string]string{"contents": "read", "checks": "write", "metadata": "read"})
	got := a.Intersect(b)
	if !slices.Equal(got.Repositories, []string{"o/b"}) {
		t.Errorf("repositories = %v", got.Repositories)
	}
	// The lower of each permission, and none for one that only one side has.
	want := map[string]string{"contents": "read", "checks": "read"}
	if len(got.Permissions) != len(want) || got.Permissions["contents"] != "read" || got.Permissions["checks"] != "read" {
		t.Errorf("permissions = %v, want %v", got.Permissions, want)
	}
	if !got.Within(a) || !got.Within(b) {
		t.Error("an intersection is not within both")
	}
	if got.Equal(a) || a.Within(got) {
		t.Error("the wider scope is within the narrower")
	}
	if got.Key() != b.Intersect(a).Key() {
		t.Error("the intersection depends on the order")
	}
	// Another project has nothing in common, and nil narrows nothing.
	if other := a.Intersect(scope.NewScope("website", []string{"o/a"}, a.Permissions)); !other.Empty() || other.Level() != scope.LevelNone {
		t.Errorf("another project's scope intersects: %s", other.Key())
	}
	if !a.Narrow(nil).Equal(a) {
		t.Error("Narrow(nil) changed the scope")
	}
	if wider := a.Narrow(&scope.Scope{Project: "oku", Repositories: []string{"o/a", "o/z"}, Permissions: map[string]string{"contents": "admin"}}); wider.Allows("contents", "admin") || wider.HasRepository("o/z") || wider.Allows("issues", "read") {
		t.Errorf("a wider request widened the scope: %s", wider.Key())
	}
	// Permission values that are no permission are dropped.
	if dropped := scope.NewScope("oku", nil, map[string]string{"contents": "none", "issues": "banana"}); len(dropped.Permissions) != 0 || dropped.Level() != scope.LevelNone {
		t.Errorf("unknown values kept: %v", dropped.Permissions)
	}
}

func TestScopeLevelAndKey(t *testing.T) {
	if got := scope.NewScope("p", nil, map[string]string{"contents": "write"}).Level(); got != scope.LevelWrite {
		t.Errorf("level = %s", got)
	}
	if got := scope.NewScope("p", nil, map[string]string{"contents": "read"}).Level(); got != scope.LevelRead {
		t.Errorf("level = %s", got)
	}
	x := scope.NewScope("p", []string{"o/a", "o/b"}, map[string]string{"contents": "read", "issues": "write"})
	y := scope.NewScope("p", []string{"o/b", "o/a"}, map[string]string{"issues": "write", "contents": "read"})
	if x.Key() != y.Key() {
		t.Error("scopes that allow the same have different keys")
	}
	for _, other := range []scope.Scope{
		scope.NewScope("q", []string{"o/a", "o/b"}, x.Permissions),
		scope.NewScope("p", []string{"o/a"}, x.Permissions),
		scope.NewScope("p", []string{"o/a", "o/b"}, map[string]string{"contents": "write", "issues": "write"}),
	} {
		if other.Key() == x.Key() {
			t.Errorf("%s has the key of %s", other.Key(), x.Key())
		}
	}
}
