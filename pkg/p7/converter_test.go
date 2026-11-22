package p7_test

import (
	"os"
	"slices"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

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

// A worker of an earlier build decodes what a newer navi sends: a field that
// its message does not have is ignored, and not an error that fails the
// chat's start. The older message is the current one without the field.
func TestDataConverterIgnoresFieldsOfANewerContract(t *testing.T) {
	older := olderChatRequest(t, "request_targets", "project_request")
	newer := &navi.ChatRequest{Id: "c1", Title: "t", RequestTargets: []string{"selfdev"}, ProjectRequest: &navi.ProjectOrigin{Project: "oku"}}
	payload, err := p7.DataConverter().ToPayload(newer)
	if err != nil {
		t.Fatal(err)
	}
	// The default converter is what an unpatched worker runs: it fails on the field.
	if err := converter.GetDefaultDataConverter().FromPayload(payload, dynamicpb.NewMessage(older)); err == nil {
		t.Fatal("the default converter decodes an unknown field: the test does not show the failure")
	}
	decoded := dynamicpb.NewMessage(older)
	if err := p7.DataConverter().FromPayload(payload, decoded); err != nil {
		t.Fatalf("decoding a field of a newer contract: %v", err)
	}
	if got := decoded.Get(older.Fields().ByName("id")).String(); got != "c1" {
		t.Errorf("id %q, want c1", got)
	}
}

// olderChatRequest is navi.v1.ChatRequest without the named fields: the
// message a worker built before them has.
func olderChatRequest(t *testing.T, without ...string) protoreflect.MessageDescriptor {
	t.Helper()
	full := protodesc.ToDescriptorProto((&navi.ChatRequest{}).ProtoReflect().Descriptor())
	full.Field = slices.DeleteFunc(full.Field, func(field *descriptorpb.FieldDescriptorProto) bool { return slices.Contains(without, field.GetName()) })
	files := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{}}
	seen := map[string]bool{}
	var add func(file protoreflect.FileDescriptor)
	add = func(file protoreflect.FileDescriptor) {
		if seen[file.Path()] {
			return
		}
		seen[file.Path()] = true
		imports := file.Imports()
		for i := range imports.Len() {
			add(imports.Get(i).FileDescriptor)
		}
		files.File = append(files.File, protodesc.ToFileDescriptorProto(file))
	}
	parent := (&navi.ChatRequest{}).ProtoReflect().Descriptor().ParentFile()
	add(parent)
	file := files.File[len(files.File)-1]
	for i, message := range file.MessageType {
		if message.GetName() == "ChatRequest" {
			file.MessageType[i] = full
		}
	}
	registry, err := protodesc.NewFiles(files)
	if err != nil {
		t.Fatal(err)
	}
	found, err := registry.FindDescriptorByName("navi.v1.ChatRequest")
	if err != nil {
		t.Fatal(err)
	}
	return found.(protoreflect.MessageDescriptor)
}
