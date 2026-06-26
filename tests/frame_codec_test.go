package tests

import (
	"bytes"
	. "github.com/elum-utils/go-etp"
	"testing"
)

func TestFrameCodecRoundTrip(t *testing.T) {
	payload := []byte("hello frame")
	frame := NewFrame(FrameData, SchemaTextMessage, payload)
	frame.Header.Priority = PriorityHigh
	frame.Header.ChannelID = ChannelRealtime
	frame.Header.RequestID = 42
	frame.Header.TransferID = 99
	frame.Header.ChunkID = 3
	frame.Header.Flags = FlagFirst | FlagLast

	encoded, err := EncodeFrame(frame)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	decoded, err := DecodeFrame(encoded)
	if err != nil {
		t.Fatalf("decode frame: %v", err)
	}

	if decoded.Header.FrameType != FrameData {
		t.Fatalf("frame type = %d", decoded.Header.FrameType)
	}
	if decoded.Header.SchemaID != SchemaTextMessage {
		t.Fatalf("schema = %d", decoded.Header.SchemaID)
	}
	if decoded.Header.Priority != PriorityHigh || decoded.Header.ChannelID != ChannelRealtime {
		t.Fatalf("priority/channel = %d/%d", decoded.Header.Priority, decoded.Header.ChannelID)
	}
	if decoded.Header.TransferID != 99 || decoded.Header.ChunkID != 3 {
		t.Fatalf("ids = transfer %d chunk %d", decoded.Header.TransferID, decoded.Header.ChunkID)
	}
	if !bytes.Equal(decoded.Payload, payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestFrameCodecRejectsInvalidFrames(t *testing.T) {
	if _, err := DecodeFrame([]byte{1, 2, 3}); err == nil {
		t.Fatalf("expected small frame error")
	}

	encoded, err := EncodeFrame(NewFrame(FrameData, 0, []byte("x")))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	badVersion := append([]byte(nil), encoded...)
	badVersion[0] = 99
	if _, err := DecodeFrame(badVersion); err == nil {
		t.Fatalf("expected version error")
	}

	badOffset := append([]byte(nil), encoded...)
	badOffset[8] = 0
	badOffset[9] = 1
	if _, err := DecodeFrame(badOffset); err == nil {
		t.Fatalf("expected payload offset error")
	}

	badHeaderLength := append([]byte(nil), encoded...)
	badHeaderLength[5] = HeaderSize + 1
	if _, err := DecodeFrame(badHeaderLength); err == nil {
		t.Fatalf("expected non-canonical header length error")
	}

	trailing := append(append([]byte(nil), encoded...), 0)
	if _, err := DecodeFrame(trailing); err == nil {
		t.Fatalf("expected trailing bytes error")
	}

	shortPayload := encoded[:len(encoded)-1]
	if _, err := DecodeFrame(shortPayload); err == nil {
		t.Fatalf("expected payload length error")
	}
}
