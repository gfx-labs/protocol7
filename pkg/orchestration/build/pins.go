package build

import (
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"
)

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// PinFiles returns only changed files. Keys are repository-relative paths.
// Only marked image or <name>-image settings under kubernetes/ are rewritten.
// The marker and package must agree with the build result, even when migrating
// an existing ghcr pin. Every built image must have at least one matching pin.
func PinFiles(files map[string]string, registry string, images []Image) (map[string]string, error) {
	endpoint, err := url.Parse("https://" + registry)
	if err != nil || endpoint.Host != registry || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Path != "" || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, fmt.Errorf("build: registry %q must be a host[:port]", registry)
	}
	byName := make(map[string]Image, len(images))
	for _, image := range images {
		if image.Name == "" || !packagePattern.MatchString(image.Package) || !digestPattern.MatchString(image.Digest) {
			return nil, fmt.Errorf("build: invalid image result %+v", image)
		}
		if _, exists := byName[image.Name]; exists {
			return nil, fmt.Errorf("build: duplicate image name %q", image.Name)
		}
		byName[image.Name] = image
	}
	// The opt-in marker selects the image, not its old registry hostname.
	pin := regexp.MustCompile(`(?m)^([\t ]*(?:-[\t ]+)?(?:[a-z][a-z0-9-]*-)?image:[\t ]*)([^\s]+)([\t ]+#[\t ]*p7-image:[\t ]*([^\s]+)[\t ]*\r?)$`)
	matched := make(map[string]bool, len(images))
	changed := make(map[string]string)
	for filename, contents := range files {
		if path.Clean(filename) != filename || !strings.HasPrefix(filename, "kubernetes/") {
			continue
		}
		var pinError error
		updated := pin.ReplaceAllStringFunc(contents, func(line string) string {
			parts := pin.FindStringSubmatch(line)
			image, known := byName[parts[4]]
			if !known {
				return line
			}
			_, reference, hasHost := strings.Cut(parts[2], "/")
			pkg, digest, hasDigest := strings.Cut(reference, "@")
			if !hasHost || !hasDigest || pkg != image.Package || !digestPattern.MatchString(digest) {
				pinError = fmt.Errorf("build: %s: marker %q must pin package %s by digest", filename, image.Name, image.Package)
				return line
			}
			matched[image.Name] = true
			return parts[1] + image.Reference(registry) + parts[3]
		})
		if pinError != nil {
			return nil, pinError
		}
		if updated != contents {
			changed[filename] = updated
		}
	}
	for _, image := range images {
		if !matched[image.Name] {
			return nil, fmt.Errorf("build: no kubernetes pin marked p7-image: %s for %s", image.Name, image.Package)
		}
	}
	return changed, nil
}
