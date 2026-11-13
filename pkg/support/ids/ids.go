// Package ids makes identifiers for sessions and turns.
package ids

import (
	"crypto/rand"
	"encoding/base32"
	"strings"
)

var encoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// New returns a random identifier of 16 lower-case letters and digits. It
// is safe as a directory name and as one token of a stream subject.
func New() string {
	var raw [10]byte
	// crypto/rand.Read does not fail on the platforms Go supports.
	_, _ = rand.Read(raw[:])
	return strings.ToLower(encoding.EncodeToString(raw[:]))
}
