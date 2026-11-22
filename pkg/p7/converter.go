package p7

import (
	"bytes"
	"fmt"
	"reflect"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// DataConverter is the Temporal data converter that NewTemporalClient sets
// when the options carry none. It encodes as the SDK's default converter
// does, and it ignores fields of a message that it does not know: navi and a
// tenant's worker deploy separately, so a payload may come from a newer
// contract than the worker's, and a field added to a navi.v1 message must not
// stop an older worker from starting a chat. It also decodes a json/plain payload into a protobuf message with
// protojson. Workflows whose payload types became navi.v1 messages recorded
// their earlier payloads as json/plain from Go structs, and encoding/json
// cannot read those into a message (an RFC 3339 time into a
// timestamppb.Timestamp, for one). A failed decode during replay takes
// another branch than history and the workflow task fails as
// nondeterministic, so replay must read them.
func DataConverter() converter.DataConverter {
	return converter.NewCompositeDataConverter(
		converter.NewNilPayloadConverter(),
		converter.NewByteSlicePayloadConverter(),
		converter.NewProtoJSONPayloadConverterWithOptions(converter.ProtoJSONPayloadConverterOptions{AllowUnknownFields: true}),
		converter.NewProtoPayloadConverter(),
		legacyJSONPayloadConverter{JSONPayloadConverter: converter.NewJSONPayloadConverter()},
	)
}

// legacyJSONPayloadConverter is the SDK's JSON payload converter, except that
// it decodes into a protobuf message with protojson.
type legacyJSONPayloadConverter struct {
	*converter.JSONPayloadConverter
}

var jsonNull = []byte("null")

// FromPayload decodes payload into valuePtr. A *M or **M target, where *M is
// a protobuf message, is read with protojson and ignores unknown fields, as
// the SDK's proto JSON converter does.
func (c legacyJSONPayloadConverter) FromPayload(payload *commonpb.Payload, valuePtr any) error {
	target := reflect.ValueOf(valuePtr)
	if target.Kind() != reflect.Pointer || target.IsNil() {
		return c.JSONPayloadConverter.FromPayload(payload, valuePtr)
	}
	if message, ok := valuePtr.(proto.Message); ok {
		return unmarshalLegacyJSON(payload, message)
	}
	field := target.Elem()
	if field.Kind() != reflect.Pointer || !field.Type().Implements(protoMessageType) {
		return c.JSONPayloadConverter.FromPayload(payload, valuePtr)
	}
	if bytes.Equal(bytes.TrimSpace(payload.GetData()), jsonNull) {
		field.SetZero()
		return nil
	}
	message := reflect.New(field.Type().Elem())
	if err := unmarshalLegacyJSON(payload, message.Interface().(proto.Message)); err != nil {
		return err
	}
	field.Set(message)
	return nil
}

var protoMessageType = reflect.TypeFor[proto.Message]()

func unmarshalLegacyJSON(payload *commonpb.Payload, message proto.Message) error {
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(payload.GetData(), message); err != nil {
		return fmt.Errorf("%w: json/plain into %s: %v", converter.ErrUnableToDecode, message.ProtoReflect().Descriptor().FullName(), err)
	}
	return nil
}
