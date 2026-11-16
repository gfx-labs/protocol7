package ids

import (
	"slices"
	"strings"
	"testing"
	"uuid"
)

// The vectors come from python-ulid, an independent ULID implementation.
func TestFormatMatchesULID(t *testing.T) {
	for _, vector := range []struct{ uuid, text string }{
		{"00000000-0000-0000-0000-000000000000", "00000000000000000000000000"},
		{"ffffffff-ffff-ffff-ffff-ffffffffffff", "7zzzzzzzzzzzzzzzzzzzzzzzzz"},
		{"01563e3a-b5d3-d676-4c61-efb99302bd5b", "01arz3ndektsv4rrffq69g5fav"},
		{"0192e3f0-6b1d-7c3a-9f2e-5d4c3b2a1908", "01jbhz0trxfgx9ybjx9gxjm688"},
	} {
		u := uuid.MustParse(vector.uuid)
		if got := Format(u); got != vector.text {
			t.Errorf("Format(%s) = %s, want %s", vector.uuid, got, vector.text)
		}
		for _, text := range []string{vector.text, strings.ToUpper(vector.text)} {
			if got, err := Parse(text); err != nil || got != u {
				t.Errorf("Parse(%s) = %s, %v, want %s", text, got, err, vector.uuid)
			}
		}
	}
}

func TestParseRejects(t *testing.T) {
	for _, text := range []string{"", "01arz3ndektsv4rrffq69g5fa", "01arz3ndektsv4rrffq69g5fau", "80000000000000000000000000", "y2hzfq4mnfpylgpw"} {
		if _, err := Parse(text); err == nil {
			t.Errorf("Parse(%q) succeeded", text)
		}
	}
}

func TestNewSortsByCreation(t *testing.T) {
	made := make([]string, 1000)
	for i := range made {
		made[i] = New()
	}
	if !slices.IsSorted(made) {
		t.Fatal("identifiers made in order do not sort in order")
	}
	if len(slices.Compact(slices.Clone(made))) != len(made) {
		t.Fatal("New repeated an identifier")
	}
}
