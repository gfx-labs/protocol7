package scope_test

import (
	"slices"
	"testing"

	"github.com/gfx-labs/protocol7/pkg/domain/scope"
)

func TestScopeIntersect(t *testing.T) {
	a := scope.New("oku", []string{"o/a", "o/b"}, map[string]string{"contents": "write", "issues": "write", "checks": "read"})
	b := scope.New("oku", []string{"O/B", "o/c"}, map[string]string{"contents": "read", "checks": "write", "metadata": "read"})
	got := scope.Intersect(a, b)
	if !slices.Equal(got.Repositories, []string{"o/b"}) {
		t.Errorf("repositories = %v", got.Repositories)
	}
	// The lower of each permission, and none for one that only one side has.
	want := map[string]string{"contents": "read", "checks": "read"}
	if len(got.Permissions) != len(want) || got.Permissions["contents"] != "read" || got.Permissions["checks"] != "read" {
		t.Errorf("permissions = %v, want %v", got.Permissions, want)
	}
	if !scope.Within(got, a) || !scope.Within(got, b) {
		t.Error("an intersection is not within both")
	}
	if scope.Equal(got, a) || scope.Within(a, got) {
		t.Error("the wider scope is within the narrower")
	}
	if scope.Key(got) != scope.Key(scope.Intersect(b, a)) {
		t.Error("the intersection depends on the order")
	}
	// Another project has nothing in common, and nil narrows nothing.
	if other := scope.Intersect(a, scope.New("website", []string{"o/a"}, a.Permissions)); !scope.Empty(other) || scope.LevelOf(other) != scope.LevelNone {
		t.Errorf("another project's scope intersects: %s", scope.Key(other))
	}
	if !scope.Equal(scope.Narrow(a, nil), a) {
		t.Error("Narrow(nil) changed the scope")
	}
	if wider := scope.Narrow(a, &scope.Scope{Project: "oku", Repositories: []string{"o/a", "o/z"}, Permissions: map[string]string{"contents": "admin"}}); scope.Allows(wider, "contents", "admin") || scope.HasRepository(wider, "o/z") || scope.Allows(wider, "issues", "read") {
		t.Errorf("a wider request widened the scope: %s", scope.Key(wider))
	}
	// Permission values that are no permission are dropped.
	if dropped := scope.New("oku", nil, map[string]string{"contents": "none", "issues": "banana"}); len(dropped.Permissions) != 0 || scope.LevelOf(dropped) != scope.LevelNone {
		t.Errorf("unknown values kept: %v", dropped.Permissions)
	}
}

func TestScopeLevelAndKey(t *testing.T) {
	if got := scope.LevelOf(scope.New("p", nil, map[string]string{"contents": "write"})); got != scope.LevelWrite {
		t.Errorf("level = %s", got)
	}
	if got := scope.LevelOf(scope.New("p", nil, map[string]string{"contents": "read"})); got != scope.LevelRead {
		t.Errorf("level = %s", got)
	}
	x := scope.New("p", []string{"o/a", "o/b"}, map[string]string{"contents": "read", "issues": "write"})
	y := scope.New("p", []string{"o/b", "o/a"}, map[string]string{"issues": "write", "contents": "read"})
	if scope.Key(x) != scope.Key(y) {
		t.Error("scopes that allow the same have different keys")
	}
	for _, other := range []*scope.Scope{
		scope.New("q", []string{"o/a", "o/b"}, x.Permissions),
		scope.New("p", []string{"o/a"}, x.Permissions),
		scope.New("p", []string{"o/a", "o/b"}, map[string]string{"contents": "write", "issues": "write"}),
	} {
		if scope.Key(other) == scope.Key(x) {
			t.Errorf("%s has the key of %s", scope.Key(other), scope.Key(x))
		}
	}
}
