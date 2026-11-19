// Package artifact keeps the files that should not be in git, such as
// mockups, reference code, screenshots and reports, in a bucket of their own,
// and shares them across chats and jobs by reference.
//
// Bytes are stored under their digest, sha256/<hex>. A name index,
// <project>/<name>, points at a digest and keeps the earlier ones, so a
// reference to an old digest still reads when a name has moved on. A
// reference is p7://sha256:<hex>, with #<name> for display. References go
// into history, chat messages, issues and pull request bodies. Bytes never
// do.
package artifact

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Scheme is the scheme of a reference.
const Scheme = "p7://"

const digestPrefix = "sha256:"

// BlobPrefix is where the bytes are in the bucket: sha256/<hex>.
const BlobPrefix = "sha256/"

var (
	digestPattern  = regexp.MustCompile(`^[0-9a-f]{64}$`)
	segmentPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+@=-]*$`)
	projectPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
)

// MaxNameLength bounds a name, and MaxNameDepth the segments in it.
const (
	MaxNameLength = 200
	MaxNameDepth  = 8
)

// ErrInvalid marks a refusal of the request itself, which no retry changes:
// a name, project, digest, reference or path that is not valid.
var ErrInvalid = errors.New("invalid request")

type invalidError struct{ message string }

func (e *invalidError) Error() string        { return e.message }
func (e *invalidError) Is(target error) bool { return target == ErrInvalid }

func invalidf(format string, args ...any) error {
	return &invalidError{message: fmt.Sprintf(format, args...)}
}

// Ref is a reference to bytes: a digest, and optionally the name to display
// it with.
type Ref struct {
	// Digest is the hex SHA-256 of the bytes.
	Digest string
	// Name is for display only. It does not select anything.
	Name string
}

// String is p7://sha256:<hex>, followed by #<name> when there is one.
func (r Ref) String() string {
	s := Scheme + digestPrefix + r.Digest
	if r.Name != "" {
		s += "#" + r.Name
	}
	return s
}

// ParseRef reads p7://sha256:<hex> with an optional #<name>. A bare
// sha256:<hex> or <hex> is accepted too.
func ParseRef(text string) (Ref, error) {
	rest := strings.TrimSpace(text)
	rest = strings.TrimPrefix(rest, Scheme)
	rest, name, _ := strings.Cut(rest, "#")
	rest = strings.TrimPrefix(rest, digestPrefix)
	if !digestPattern.MatchString(rest) {
		return Ref{}, invalidf("%q is not a reference: want p7://sha256:<64 hex digits>", text)
	}
	if name != "" && !validDisplay(name) {
		name = ""
	}
	return Ref{Digest: rest, Name: name}, nil
}

// validDisplay is a name fit to print: the fragment is advisory, so a bad one
// is dropped and not an error.
func validDisplay(name string) bool { return ValidName(name) == nil }

// ValidDigest reports whether s is a hex SHA-256.
func ValidDigest(s string) bool { return digestPattern.MatchString(s) }

// ValidName says what is wrong with a name. A name is segments separated by
// "/", each of letters, digits and . _ + @ = -, starting with a letter or
// digit, so it cannot climb out of its place.
func ValidName(name string) error {
	if name == "" {
		return invalidf("the name is empty")
	}
	if len(name) > MaxNameLength {
		return invalidf("the name is longer than %d bytes", MaxNameLength)
	}
	segments := strings.Split(name, "/")
	if len(segments) > MaxNameDepth {
		return invalidf("the name has more than %d parts", MaxNameDepth)
	}
	for _, segment := range segments {
		if !segmentPattern.MatchString(segment) {
			return invalidf("the name %q must be parts separated by '/', each of letters, digits and . _ + @ = -, starting with a letter or digit", name)
		}
	}
	return nil
}

// ValidProject says what is wrong with a project name.
func ValidProject(project string) error {
	if !projectPattern.MatchString(project) || project == strings.TrimSuffix(BlobPrefix, "/") {
		return invalidf("%q is not a project name", project)
	}
	return nil
}

func blobKey(digest string) string { return BlobPrefix + digest }

func indexKey(project, name string) string { return project + "/" + name }
