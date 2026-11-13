package journal_test

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/gfx-labs/protocol7/pkg/domain"
	"github.com/gfx-labs/protocol7/pkg/execution/journal"
)

// testdata/towa-recovery.ndjson is the journal of one real session, recorded
// on 2026-11-13 from towa behind p7acpshim. Its turns, in order: two that
// end normally, one whose caller was killed and that was picked up again,
// one that was cancelled, one after the cancel, one after the shim was
// stopped and the conversation loaded again, one during which the shim was
// stopped, and one after that.
func readJournal(t *testing.T) []domain.Frame {
	t.Helper()
	file, err := os.Open("testdata/towa-recovery.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var frames []domain.Frame
	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, 4<<20)
	for scanner.Scan() {
		var frame domain.Frame
		if err := json.Unmarshal(scanner.Bytes(), &frame); err != nil {
			t.Fatalf("frame %d: %v", len(frames)+1, err)
		}
		frames = append(frames, frame)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return frames
}

func TestTurnsOfARecordedSession(t *testing.T) {
	frames := readJournal(t)
	want := []struct {
		turn, stop, text string
	}{
		{"t1", "end_turn", "notes: add heron"},
		{"t2", "end_turn", "heron"},
		{"t3", "end_turn", "awake"},
		{"t4", "cancelled", ""},
		{"t4b", "end_turn", "ready"},
		{"t5", "end_turn", "heron"},
		{"t6", domain.StopInterrupted, ""},
		{"t7", "end_turn", "back"},
	}
	for _, expected := range want {
		decoder := journal.NewDecoder("s1")
		turn := journal.NewTurn(expected.turn)
		for _, frame := range frames {
			for _, event := range decoder.Decode(frame) {
				turn.Observe(event)
			}
		}
		if !turn.Ended() {
			t.Errorf("turn %s: not ended", expected.turn)
			continue
		}
		result := turn.Result()
		if result.StopReason != expected.stop || strings.TrimSpace(result.Text) != expected.text {
			t.Errorf("turn %s: got stop %q text %q, want stop %q text %q",
				expected.turn, result.StopReason, result.Text, expected.stop, expected.text)
		}
	}
}

// A harness that loads a conversation replays it. A viewer must see each
// prompt and each turn end once, and the replayed messages not at all.
func TestALoadedConversationIsNotShownTwice(t *testing.T) {
	decoder := journal.NewDecoder("s1")
	var prompts, ends int
	var message strings.Builder
	for _, frame := range readJournal(t) {
		for _, event := range decoder.Decode(frame) {
			switch event.Kind {
			case domain.EventPrompt:
				prompts++
			case domain.EventTurnEnd:
				ends++
			case domain.EventMessage:
				message.WriteString(event.Text)
			}
		}
	}
	if prompts != 8 || ends != 8 {
		t.Errorf("got %d prompts and %d turn ends, want 8 of each", prompts, ends)
	}
	if got := strings.Count(message.String(), "notes: add heron"); got != 1 {
		t.Errorf("the first turn's answer appears %d times in the messages, want 1", got)
	}
}
