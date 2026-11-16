// Package ids makes identifiers for sessions, turns and other records.
//
// An identifier is a UUIDv7 written in the text form of a ULID: 26 lower-case
// characters of Crockford's base32. The first 48 bits of a UUIDv7 are a
// millisecond timestamp, so identifiers sort by creation time both as bytes
// and as strings. The text is safe as a directory name, a branch name, a
// Kubernetes object name and one token of a stream subject.
package ids

import (
	"encoding/binary"
	"fmt"
	"uuid"
)

// Length is the number of characters in an identifier.
const Length = 26

// alphabet is Crockford's base32 in lower case. Its characters are in ASCII
// order, so the text sorts the same way as the bytes.
const alphabet = "0123456789abcdefghjkmnpqrstvwxyz"

// New returns a new identifier.
func New() string { return Format(uuid.NewV7()) }

// Format writes a UUID as an identifier. 26 characters hold 130 bits, so the
// first character carries only the top 3 bits of the UUID, as in a ULID.
func Format(u uuid.UUID) string {
	hi := binary.BigEndian.Uint64(u[:8])
	lo := binary.BigEndian.Uint64(u[8:])
	var text [Length]byte
	for i := Length - 1; i >= 0; i-- {
		text[i] = alphabet[lo&31]
		lo = lo>>5 | hi<<59
		hi >>= 5
	}
	return string(text[:])
}

// Parse reads an identifier written by Format. It accepts upper case, as a
// ULID's canonical text is upper case.
func Parse(text string) (uuid.UUID, error) {
	var u uuid.UUID
	if len(text) != Length {
		return u, fmt.Errorf("parse id %q: has %d characters, want %d", text, len(text), Length)
	}
	var hi, lo uint64
	for i := 0; i < Length; i++ {
		value := digit(text[i])
		if value < 0 {
			return u, fmt.Errorf("parse id %q: character %q is not in the alphabet", text, text[i])
		}
		if i == 0 && value > 7 {
			return u, fmt.Errorf("parse id %q: overflows 128 bits", text)
		}
		hi = hi<<5 | lo>>59
		lo = lo<<5 | uint64(value)
	}
	binary.BigEndian.PutUint64(u[:8], hi)
	binary.BigEndian.PutUint64(u[8:], lo)
	return u, nil
}

func digit(c byte) int {
	if 'A' <= c && c <= 'Z' {
		c += 'a' - 'A'
	}
	for i := 0; i < len(alphabet); i++ {
		if alphabet[i] == c {
			return i
		}
	}
	return -1
}
