package build_test

import (
	"maps"
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

func TestPinOnlyGate(t *testing.T) {
	image := build.Image{Name: "reviewer", Package: "oku-trade/oku-review", Digest: "sha256:" + strings.Repeat("b", 64)}
	line := func(reference string) string { return "  image: " + reference + " # p7-image: reviewer\n" }
	before := map[string]string{
		"kubernetes/chat.yaml": "replicas: 1\n" + line("ghcr.io/oku-trade/oku-review@sha256:"+strings.Repeat("a", 64)),
		"main.go":              "package main\n",
	}
	pinned := "replicas: 1\n" + line(image.Reference("registry.example:30500"))
	for _, test := range []struct {
		name   string
		change func(map[string]string)
		accept bool
	}{
		{"pin-only", func(files map[string]string) {}, true},
		{"mixed-code", func(files map[string]string) { files["main.go"] += "// feature\n" }, false},
		{"mixed-manifest", func(files map[string]string) {
			files["kubernetes/chat.yaml"] = strings.Replace(pinned, "replicas: 1", "replicas: 2", 1)
		}, false},
		{"tag", func(files map[string]string) {
			files["kubernetes/chat.yaml"] = "replicas: 1\n" + line("registry.example:30500/oku-trade/oku-review:latest")
		}, false},
		{"untrusted-digest", func(files map[string]string) {
			files["kubernetes/chat.yaml"] = strings.ReplaceAll(pinned, image.Digest, "sha256:"+strings.Repeat("c", 64))
		}, false},
		{"added-file", func(files map[string]string) { files["new.go"] = "package main\n" }, false},
		{"removed-file", func(files map[string]string) { delete(files, "main.go") }, false},
		{"changed-marker", func(files map[string]string) {
			files["kubernetes/chat.yaml"] = strings.Replace(pinned, "p7-image: reviewer", "p7-image: worker", 1)
		}, false},
		{"unchanged", func(files map[string]string) { files["kubernetes/chat.yaml"] = before["kubernetes/chat.yaml"] }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			after := maps.Clone(before)
			after["kubernetes/chat.yaml"] = pinned
			test.change(after)
			err := build.PinOnly(before, after, "registry.example:30500", []build.Image{image})
			if (err == nil) != test.accept {
				t.Fatalf("accepted=%v, error=%v", test.accept, err)
			}
		})
	}
}
