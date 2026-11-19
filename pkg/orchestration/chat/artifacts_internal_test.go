package chat

import (
	"testing"

	"github.com/gfx-labs/protocol7/pkg/p7"
)

func TestArtifactToolsNeedTheHostBackendAndTheCoreProject(t *testing.T) {
	for name, tc := range map[string]struct {
		request Request
		want    int
	}{
		"host":       {Request{Host: "selfdev"}, 3},
		"kubernetes": {Request{Host: "selfdev", Backend: p7.BackendKubernetes}, 0},
		"project":    {Request{Host: "selfdev", Project: "acme"}, 0},
		"no host":    {Request{}, 0},
	} {
		if got := len(artifactTools(&State{Request: tc.request})); got != tc.want {
			t.Errorf("%s: %d tools, want %d", name, got, tc.want)
		}
	}
}
