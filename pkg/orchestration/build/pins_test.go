package build_test

import (
	"strings"
	"testing"

	"github.com/gfx-labs/protocol7/pkg/orchestration/build"
)

func TestPinFiles(t *testing.T) {
	old := "sha256:" + strings.Repeat("a", 64)
	digest := "sha256:" + strings.Repeat("b", 64)
	image := build.Image{Name: "reviewer", Package: "oku-trade/oku-review", Digest: digest}
	for _, registry := range []string{"10.10.20.21:30500", "registry.example:5443"} {
		t.Run(registry, func(t *testing.T) {
			image := image
			original := "  image: ghcr.io/oku-trade/oku-review@" + old + " # p7-image: reviewer\r\n"
			files := map[string]string{
				"kubernetes/chat.yaml":       original,
				"kubernetes/config.yaml":     "    emulator-image: ghcr.io/oku-trade/oku-review@" + old + " # p7-image: reviewer\n",
				"kubernetes/unmarked.yaml":   "image: ghcr.io/oku-trade/oku-review@" + old + "\n",
				"kubernetes/other.yaml":      "image: ghcr.io/other/package@" + old + " # p7-image: other\n",
				"docs/example.yaml":          original,
				"kubernetes/../outside.yaml": original,
			}
			changed, err := build.PinFiles(files, registry, []build.Image{image})
			if err != nil {
				t.Fatal(err)
			}
			want := "  image: " + registry + "/oku-trade/oku-review@" + digest + " # p7-image: reviewer\r\n"
			if len(changed) != 2 || changed["kubernetes/chat.yaml"] != want || files["kubernetes/chat.yaml"] != original {
				t.Fatalf("changed files: %+v", changed)
			}
			again, err := build.PinFiles(changed, registry, []build.Image{image})
			if err != nil || len(again) != 0 {
				t.Fatalf("repeated pin changed files: %+v, %v", again, err)
			}
			image.Digest = old
			repinned, err := build.PinFiles(changed, registry, []build.Image{image})
			if err != nil || !strings.Contains(repinned["kubernetes/chat.yaml"], "@"+old) {
				t.Fatalf("direct registry re-pin: %+v, %v", repinned, err)
			}
		})
	}
	image.Digest = digest
	for _, reference := range []string{"ghcr.io/other/package@" + old, "ghcr.io/oku-trade/oku-review:latest"} {
		_, err := build.PinFiles(map[string]string{"kubernetes/pod.yaml": "image: " + reference + " # p7-image: reviewer\n"}, "registry.example:5443", []build.Image{image})
		if err == nil {
			t.Fatalf("accepted mismatched or unpinned reference %q", reference)
		}
	}
	for _, registry := range []string{"", "https://registry.example", "registry.example/path", "user@registry.example"} {
		if _, err := build.PinFiles(nil, registry, nil); err == nil {
			t.Fatalf("accepted registry %q", registry)
		}
	}
	if _, err := build.PinFiles(nil, "registry.example", []build.Image{image}); err == nil {
		t.Fatal("accepted a build without any marked pins")
	}
}
