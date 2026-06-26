package tests

import (
	"bytes"
	"testing"

	. "github.com/elum-utils/go-etp"
)

func TestEventMessagePayloadRoundTrip(t *testing.T) {
	event := []byte("message.get")
	data := []byte(`{"id":42}`)
	buf := make([]byte, 0, EventMessagePayloadSize(event, data, nil))
	encoded, err := EncodeEventMessageInto(buf, event, data)
	if err != nil {
		t.Fatalf("encode event: %v", err)
	}
	decoded, err := DecodeEventMessageView(encoded)
	if err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if !bytes.Equal(decoded.Event, event) || !bytes.Equal(decoded.Data, data) {
		t.Fatalf("decoded event=%q data=%q", decoded.Event, decoded.Data)
	}
	if len(decoded.Event) > 0 && &decoded.Event[0] != &encoded[4] {
		t.Fatalf("event decode copied event name")
	}
	if len(decoded.Data) > 0 && &decoded.Data[0] != &encoded[8+len(event)] {
		t.Fatalf("event decode copied data")
	}
}

func TestEventMessagePayloadRoundTripWithFields(t *testing.T) {
	message := EventMessage{
		Event: "attach.upload",
		Data:  []byte("body"),
		Fields: []TransferField{
			{Key: "dialog", Value: "2f1f7bc8-06f1-40ec-96e8-c75c0f86d890"},
			{Key: "caption", Value: "hello"},
		},
	}
	decoded, err := DecodeEventMessage(EncodeEventMessage(message))
	if err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if decoded.Event != message.Event || !bytes.Equal(decoded.Data, message.Data) {
		t.Fatalf("decoded event=%q data=%q", decoded.Event, decoded.Data)
	}
	if len(decoded.Fields) != 2 || decoded.Fields[1].Value != "hello" {
		t.Fatalf("decoded fields: %+v", decoded.Fields)
	}
}

func TestEventMessagePayloadRejectsInvalidData(t *testing.T) {
	if _, err := DecodeEventMessageView([]byte{1, 2, 3}); err == nil {
		t.Fatalf("expected short event payload error")
	}
	encoded := EncodeEventMessage(EventMessage{Event: "x", Data: []byte("y")})
	encoded[3] = 99
	if _, err := DecodeEventMessageView(encoded); err == nil {
		t.Fatalf("expected invalid event length error")
	}
	encoded = EncodeEventMessage(EventMessage{Event: "x", Data: []byte("y")})
	encoded[7+len("x")] = 99
	if _, err := DecodeEventMessageView(encoded); err == nil {
		t.Fatalf("expected invalid data length error")
	}
}
