package tests

import (
	"bytes"
	"context"
	. "github.com/elum-utils/go-etp"
	"testing"
)

func TestSessionUnifiedRequestSendsSmallBodyInline(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultSessionConfig(RoleClient)
	config.Payload.MaxInlineBodyBytes = 1024
	session := NewSessionWithConfig(transport, config)

	handle, err := session.Request(context.Background(), MessageOptions{
		Event: "attach.upload",
		Data:  []byte("hello"),
		Fields: []TransferField{
			{Key: "dialog", Value: "dialog-1"},
		},
	})
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if handle.RequestID == 0 || handle.TransferID != 0 {
		t.Fatalf("handle = %+v", handle)
	}
	if err := <-handle.Done(); err != nil {
		t.Fatalf("done: %v", err)
	}

	frames := transport.frames()
	request := requireFrameType(t, frames, FrameRequest)
	if request.Header.RequestID != handle.RequestID {
		t.Fatalf("request id = %d, want %d", request.Header.RequestID, handle.RequestID)
	}
	if frameCount(frames, FrameTransferBegin) != 0 {
		t.Fatalf("small body used transfer path")
	}
	message, err := DecodeEventMessageView(request.Payload)
	if err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if string(message.Event) != "attach.upload" || string(message.Data) != "hello" {
		t.Fatalf("message = event:%q data:%q", message.Event, message.Data)
	}
}

func TestSessionUnifiedRequestSendsLargeBodyAsTransfer(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultSessionConfig(RoleClient)
	config.Payload.MaxInlineBodyBytes = 8
	session := NewSessionWithConfig(transport, config)
	transport.onSend = func(frame Frame) {
		if frame.Header.FrameType != FrameData || frame.Header.TransferID == 0 {
			return
		}
		ack := NewFrame(FrameAck, SchemaAck, EncodeAck(Ack{
			TransferID:    frame.Header.TransferID,
			ChunkFrom:     frame.Header.ChunkID,
			ChunkTo:       frame.Header.ChunkID,
			ReceivedBytes: uint64(len(frame.Payload)),
		}))
		if err := session.HandleAck(ack); err != nil {
			t.Errorf("handle ack: %v", err)
		}
	}

	body := bytes.Repeat([]byte("x"), 32)
	handle, err := session.Request(context.Background(), MessageOptions{
		Event: "attach.upload",
		Data:  body,
		Field: "image",
		Name:  "image.bin",
	})
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if handle.RequestID == 0 || handle.TransferID == 0 {
		t.Fatalf("handle = %+v", handle)
	}
	if err := <-handle.Done(); err != nil {
		t.Fatalf("done: %v", err)
	}

	frames := transport.frames()
	if frameCount(frames, FrameRequest) != 0 {
		t.Fatalf("large body used inline request path")
	}
	beginFrame := requireFrameType(t, frames, FrameTransferBegin)
	if beginFrame.Header.RequestID != handle.RequestID || beginFrame.Header.TransferID != handle.TransferID {
		t.Fatalf("begin header = %+v, handle = %+v", beginFrame.Header, handle)
	}
	begin, err := DecodeTransferBegin(beginFrame.Payload)
	if err != nil {
		t.Fatalf("decode begin: %v", err)
	}
	if begin.Event != "attach.upload" || begin.Field != "image" || begin.Name != "image.bin" {
		t.Fatalf("begin = %+v", begin)
	}
	dataFrame := requireFrameType(t, frames, FrameData)
	if dataFrame.Header.RequestID != handle.RequestID {
		t.Fatalf("data request id = %d, want %d", dataFrame.Header.RequestID, handle.RequestID)
	}
	endFrame := requireFrameType(t, frames, FrameTransferEnd)
	if endFrame.Header.RequestID != handle.RequestID {
		t.Fatalf("end request id = %d, want %d", endFrame.Header.RequestID, handle.RequestID)
	}
}
