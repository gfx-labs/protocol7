package chat_test

import (
	"testing"

	"github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// A chat with no backend, as one that started before the kubernetes backend,
// stays on the host, and the kubernetes backend names no host.
func TestPlacement(t *testing.T) {
	for _, test := range []struct {
		backend, host string
		want          p7.Placement
	}{
		{"", "selfdev", p7.Placement{Backend: p7.BackendHost, Host: "selfdev"}},
		{"host", "selfdev", p7.Placement{Backend: p7.BackendHost, Host: "selfdev"}},
		{"kubernetes", "selfdev", p7.Placement{Backend: p7.BackendKubernetes}},
	} {
		if got := chat.Placement(test.backend, test.host); got != test.want {
			t.Errorf("Placement(%q, %q) = %+v, want %+v", test.backend, test.host, got, test.want)
		}
	}
}
