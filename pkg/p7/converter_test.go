package p7_test

import (
	"os"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"

	"github.com/gfx-labs/protocol7/pkg/domain/navi"
	"github.com/gfx-labs/protocol7/pkg/p7"
)

// testdata/escalation-outcome.json is an EscalationOutcome result that a
// worker recorded as json/plain from a Go struct before EscalationState was
// a navi.v1 message, trimmed to fewer fields. Live escalation handlers
// replay it, and the SDK's default converter fails to decode it.
func TestDataConverterReadsJSONPlainIntoMessages(t *testing.T) {
	recorded, err := os.ReadFile("testdata/escalation-outcome.json")
	if err != nil {
		t.Fatal(err)
	}
	payload := &commonpb.Payload{Metadata: map[string][]byte{converter.MetadataEncoding: []byte(converter.MetadataEncodingJSON)}, Data: recorded}

	var state *navi.EscalationState
	if err := p7.DataConverter().FromPayload(payload, &state); err != nil {
		t.Fatal(err)
	}
	raised := time.Date(2026, 11, 18, 8, 11, 0, 915211562, time.UTC)
	if state.GetStatus() != navi.EscalationForwarded || !state.GetRaised().AsTime().Equal(raised) || state.GetRequest().GetSession().GetScope().GetPermissions()["contents"] != "write" || state.GetDecision().GetOutcome() != navi.OutcomeForwarded {
		t.Errorf("decoded %v", state)
	}

	var value navi.EscalationState
	if err := p7.DataConverter().FromPayload(payload, &value); err != nil || value.GetHandler() != "01m7pvb3eveppram02cmyyj550" {
		t.Errorf("decode into a value: %v, %v", err, value.GetHandler())
	}

	null := &commonpb.Payload{Metadata: payload.Metadata, Data: []byte("null")}
	state = &navi.EscalationState{Status: navi.EscalationOpen}
	if err := p7.DataConverter().FromPayload(null, &state); err != nil || state != nil {
		t.Errorf("null decoded to %v, %v", state, err)
	}

	var plain struct{ Raised string }
	if err := p7.DataConverter().FromPayload(payload, &plain); err != nil || plain.Raised != "2026-11-18T08:11:00.915211562Z" {
		t.Errorf("decode into a struct: %v, %q", err, plain.Raised)
	}

	encoded, err := p7.DataConverter().ToPayload(&navi.EscalationState{Status: navi.EscalationOpen})
	if err != nil {
		t.Fatal(err)
	}
	if encoding := string(encoded.GetMetadata()[converter.MetadataEncoding]); encoding != converter.MetadataEncodingProtoJSON {
		t.Errorf("a message encodes as %s, want %s", encoding, converter.MetadataEncodingProtoJSON)
	}
}
