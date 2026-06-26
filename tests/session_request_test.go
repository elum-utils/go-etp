package tests

import (
	"bytes"
	"context"
	. "github.com/elum-utils/go-etp"
	"testing"
	"time"
)

func TestSessionSendRequestAssignsRequestIDAndEvent(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSession(transport)

	requestID, err := session.SendRequest("message.get", []byte(`{"id":42}`))
	if err != nil {
		t.Fatalf("send request: %v", err)
	}
	if requestID == 0 {
		t.Fatalf("request id was not assigned")
	}
	frame := requireFrameType(t, transport.frames(), FrameRequest)
	if frame.Header.RequestID != requestID || frame.Header.SchemaID != SchemaEvent {
		t.Fatalf("frame header = %+v", frame.Header)
	}
	message, err := DecodeEventMessageView(frame.Payload)
	if err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if string(message.Event) != "message.get" || string(message.Data) != `{"id":42}` {
		t.Fatalf("message event=%q data=%q", message.Event, message.Data)
	}
}

func TestSessionHandleRequestAndResponseWithSameRequestID(t *testing.T) {
	serverTransport := newRecordingTransport(t)
	var server *Session
	serverConfig := DefaultSessionConfig(RoleServer)
	serverConfig.Receive.RequestHandler = func(ctx context.Context, frame Frame, message EventMessageView) error {
		if frame.Header.RequestID != 123 {
			t.Fatalf("request id = %d", frame.Header.RequestID)
		}
		if !bytes.Equal(message.Event, []byte("message.get")) {
			t.Fatalf("event = %q", message.Event)
		}
		return server.SendResponse(frame.Header.RequestID, "message.get.result", []byte(`{"ok":true}`))
	}
	server = NewSessionWithConfig(serverTransport, serverConfig)

	request := NewFrame(FrameRequest, SchemaEvent, EncodeEventMessage(EventMessage{Event: "message.get", Data: []byte(`{"id":42}`)}))
	request.Header.RequestID = 123
	if err := server.HandleFrame(context.Background(), request); err != nil {
		t.Fatalf("handle request: %v", err)
	}
	response := requireFrameType(t, serverTransport.frames(), FrameResponse)
	if response.Header.RequestID != 123 {
		t.Fatalf("response request id = %d", response.Header.RequestID)
	}

	clientConfig := DefaultSessionConfig(RoleClient)
	received := make(chan EventMessageView, 1)
	clientConfig.Receive.ResponseHandler = func(ctx context.Context, frame Frame, message EventMessageView) error {
		if frame.Header.RequestID != 123 {
			t.Fatalf("client response request id = %d", frame.Header.RequestID)
		}
		received <- message
		return nil
	}
	client := NewSessionWithConfig(newRecordingTransport(t), clientConfig)
	if err := client.HandleFrame(context.Background(), response); err != nil {
		t.Fatalf("handle response: %v", err)
	}
	select {
	case message := <-received:
		if string(message.Event) != "message.get.result" || string(message.Data) != `{"ok":true}` {
			t.Fatalf("response event=%q data=%q", message.Event, message.Data)
		}
	case <-time.After(time.Second):
		t.Fatalf("response handler was not called")
	}
}

func TestSessionRejectsRequestResponseWithoutRequestID(t *testing.T) {
	session := NewSession(newRecordingTransport(t))
	request := NewFrame(FrameRequest, SchemaEvent, EncodeEventMessage(EventMessage{Event: "message.get"}))
	if err := session.HandleFrame(context.Background(), request); err == nil {
		t.Fatalf("expected missing request id error")
	}

	response := NewFrame(FrameResponse, SchemaEvent, EncodeEventMessage(EventMessage{Event: "message.get.result"}))
	if err := session.HandleFrame(context.Background(), response); err == nil {
		t.Fatalf("expected missing response request id error")
	}
}

func TestSessionSendResponseRequiresRequestID(t *testing.T) {
	session := NewSession(newRecordingTransport(t))
	if err := session.SendResponse(0, "message.get.result", nil); err == nil {
		t.Fatalf("expected missing request id error")
	}
}

func TestSessionRejectsOversizedSmallPayloads(t *testing.T) {
	config := DefaultSessionConfig(RoleClient)
	config.Payload.MaxTextBytes = 8
	config.Payload.MaxRequestBytes = 16
	config.Payload.MaxResponseBytes = 16
	session := NewSessionWithConfig(newRecordingTransport(t), config)

	if err := session.SendText("this is too large"); err == nil {
		t.Fatalf("expected oversized text error")
	}
	if _, err := session.SendRequest("event", bytes.Repeat([]byte("x"), 32)); err == nil {
		t.Fatalf("expected oversized request error")
	}
	if err := session.SendResponse(1, "event", bytes.Repeat([]byte("x"), 32)); err == nil {
		t.Fatalf("expected oversized response error")
	}
}

func TestSessionRejectsInboundOversizedSmallPayloads(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultSessionConfig(RoleServer)
	config.Payload.MaxTextBytes = 8
	config.Payload.MaxRequestBytes = 16
	config.Payload.MaxResponseBytes = 16
	session := NewSessionWithConfig(transport, config)

	text := NewFrame(FrameData, SchemaTextMessage, EncodeTextMessage("this is too large"))
	if err := session.HandleFrame(context.Background(), text); err == nil {
		t.Fatalf("expected inbound oversized text error")
	}
	request := NewFrame(FrameRequest, SchemaEvent, EncodeEventMessage(EventMessage{Event: "event", Data: bytes.Repeat([]byte("x"), 32)}))
	request.Header.RequestID = 1
	if err := session.HandleFrame(context.Background(), request); err == nil {
		t.Fatalf("expected inbound oversized request error")
	}
	response := NewFrame(FrameResponse, SchemaEvent, EncodeEventMessage(EventMessage{Event: "event", Data: bytes.Repeat([]byte("x"), 32)}))
	response.Header.RequestID = 1
	if err := session.HandleFrame(context.Background(), response); err == nil {
		t.Fatalf("expected inbound oversized response error")
	}
	if frameCount(transport.frames(), FrameError) < 3 {
		t.Fatalf("expected protocol errors for oversized payloads")
	}
}
