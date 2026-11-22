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

// frames builds a session's journal from JSON-RPC messages, in order.
func frames(t *testing.T, messages ...[2]string) []domain.Frame {
	t.Helper()
	var out []domain.Frame
	for index, message := range messages {
		out = append(out, domain.Frame{Seq: int64(index + 1), T: 1700000000000 + int64(index), From: message[0], Msg: json.RawMessage(message[1])})
	}
	return out
}

// A turn's usage is the sum of its model requests' usage updates, by model,
// with the session's cost as of the last one. A usage update with no model or
// no cost is counted and adds nothing to what it lacks.
func TestTurnUsageSumsTheUsageUpdates(t *testing.T) {
	journalFrames := frames(t,
		[2]string{"controller", `{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"_meta":{"p7":{"turn":"t1"}},"prompt":[{"type":"text","text":"go"}]}}`},
		[2]string{"agent", `{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"usage_update","size":200000,"used":1000,"cost":{"amount":0.5,"currency":"USD"},"_meta":{"towa":{"model":"claude-opus-5-5"}}}}}`},
		[2]string{"agent", `{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"done"}}}}`},
		[2]string{"agent", `{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"usage_update","size":200000,"used":2500,"cost":{"amount":1.25,"currency":"USD"},"_meta":{"towa":{"model":"claude-opus-5-5"}}}}}`},
		[2]string{"agent", `{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"usage_update","size":128000,"used":300}}}`},
		[2]string{"agent", `{"jsonrpc":"2.0","id":1,"result":{"stopReason":"end_turn"}}`},
	)
	decoder := journal.NewDecoder("s1")
	turn := journal.NewTurn("t1")
	for _, frame := range journalFrames {
		for _, event := range decoder.Decode(frame) {
			turn.Observe(event)
		}
	}
	result := turn.Result()
	if result.Usage == nil {
		t.Fatal("the turn has no usage")
	}
	if result.Usage.Requests != 3 || result.Usage.Tokens != 3800 {
		t.Errorf("usage = %d requests, %d tokens, want 3 requests, 3800 tokens", result.Usage.Requests, result.Usage.Tokens)
	}
	if got := result.Usage.ByModel["claude-opus-5-5"]; got.Requests != 2 || got.Tokens != 3500 {
		t.Errorf("claude-opus-5-5 usage = %+v, want 2 requests, 3500 tokens", got)
	}
	if len(result.Usage.ByModel) != 1 {
		t.Errorf("by model = %v, want only the named model", result.Usage.ByModel)
	}
	// The request with no cost does not erase the cost of the one before it.
	if result.Usage.SessionCost == nil || result.Usage.SessionCost.Amount != 1.25 || result.Usage.SessionCost.Currency != "USD" {
		t.Errorf("session cost = %+v, want 1.25 USD", result.Usage.SessionCost)
	}
}

// A turn whose harness reported no usage has none, so that a caller can tell
// "nothing was reported" from "nothing was used".
func TestTurnWithoutUsageUpdatesHasNoUsage(t *testing.T) {
	decoder := journal.NewDecoder("s1")
	turn := journal.NewTurn("t1")
	for _, frame := range frames(t,
		[2]string{"controller", `{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{"_meta":{"p7":{"turn":"t1"}},"prompt":[{"type":"text","text":"go"}]}}`},
		[2]string{"agent", `{"jsonrpc":"2.0","id":1,"result":{"stopReason":"end_turn"}}`},
	) {
		for _, event := range decoder.Decode(frame) {
			turn.Observe(event)
		}
	}
	if usage := turn.Result().Usage; usage != nil {
		t.Errorf("usage = %+v, want none", usage)
	}
}
