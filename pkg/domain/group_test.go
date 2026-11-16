package domain_test

import (
	"strings"
	"testing"

	"github.com/gfx-labs/protocol7/pkg/domain"
)

func TestValidGroup(t *testing.T) {
	for _, good := range []string{"0123456789abcdef", "abcdefgh", strings.Repeat("a", 40)} {
		if err := domain.ValidGroup(good); err != nil {
			t.Errorf("%q: %v", good, err)
		}
	}
	for _, bad := range []string{"", "short", strings.Repeat("a", 41), "ABCDEFGHIJ", "0123/56789", "0123456789ab-def", "0123456789 abcd", "../../abcdef01"} {
		if domain.ValidGroup(bad) == nil {
			t.Errorf("%q was accepted", bad)
		}
	}
}

func TestGroupNames(t *testing.T) {
	if got := domain.GroupQueue("0123456789abcdef"); got != "p7-group-0123456789abcdef" {
		t.Error(got)
	}
	if got := domain.GroupPodName("0123456789abcdef"); got != "p7-group-0123456789abcdef" {
		t.Error(got)
	}
	for prefix, want := range map[string]string{"sessions": "sessions/groups/g", "/sessions/": "sessions/groups/g", "": "groups/g", "a/b": "a/b/groups/g"} {
		if got := domain.GroupSnapshotPrefix(prefix, "g"); got != want {
			t.Errorf("prefix %q: %q, want %q", prefix, got, want)
		}
	}
}
