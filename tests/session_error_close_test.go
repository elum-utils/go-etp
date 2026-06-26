package tests

import (
	"context"
	. "github.com/elum-utils/go-etp"
	"testing"
	"time"
)

func TestSessionSendsFrameErrorForInvalidRequest(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSession(transport)
	request := NewFrame(FrameRequest, SchemaEvent, EncodeEventMessage(EventMessage{Event: "message.get"}))
	request.Header.RequestID = 77
	request.Header.SchemaID = 999

	if err := session.HandleFrame(context.Background(), request); err == nil {
		t.Fatalf("expected invalid request error")
	}
	frame := requireFrameType(t, transport.frames(), FrameError)
	if frame.Header.RequestID != 77 {
		t.Fatalf("frame request id = %d", frame.Header.RequestID)
	}
	msg, err := DecodeErrorMessage(frame.Payload)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if msg.Code != ErrorInvalidRequest || msg.RequestID != 77 || msg.FrameType != FrameRequest {
		t.Fatalf("error msg = %+v", msg)
	}
}

func TestSessionHandlesErrorAndGoAwayFrames(t *testing.T) {
	session := NewSession(newRecordingTransport(t))
	events := make(chan ProtocolEvent, 2)
	session.OnProtocolEvent(func(event ProtocolEvent) {
		events <- event
	})

	errFrame := NewFrame(FrameError, SchemaError, EncodeErrorMessage(ErrorMessage{
		Code:      ErrorInvalidRequest,
		FrameType: FrameRequest,
		RequestID: 55,
		Message:   "bad request",
	}))
	if err := session.HandleFrame(context.Background(), errFrame); err != nil {
		t.Fatalf("handle error frame: %v", err)
	}

	goAwayFrame := NewFrame(FrameGoAway, SchemaGoAway, EncodeGoAway(GoAway{
		ReasonCode:         CloseServerShutdown,
		Flags:              CloseFlagDrain,
		DrainTimeoutMillis: 1000,
		Message:            "restart",
	}))
	if err := session.HandleFrame(context.Background(), goAwayFrame); err != nil {
		t.Fatalf("handle goaway: %v", err)
	}
	if session.State() != SessionDraining {
		t.Fatalf("state = %s", session.State())
	}

	var sawError, sawGoAway bool
	timeout := time.After(time.Second)
	for !sawError || !sawGoAway {
		select {
		case event := <-events:
			if event.Code == EventErrorReceived {
				sawError = true
			}
			if event.Code == EventGoAwayReceived {
				sawGoAway = true
			}
		case <-timeout:
			t.Fatalf("missing events error=%t goaway=%t", sawError, sawGoAway)
		}
	}
}

func TestSessionCloseWithSendsClosePayload(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSession(transport)

	if err := session.CloseWith(CloseMessage{ReasonCode: CloseClientShutdown, Flags: CloseFlagDrain, DrainTimeoutMillis: 500}); err != nil {
		t.Fatalf("close with: %v", err)
	}
	frame := requireFrameType(t, transport.frames(), FrameClose)
	closeMsg, err := DecodeCloseMessage(frame.Payload)
	if err != nil {
		t.Fatalf("decode close: %v", err)
	}
	if closeMsg.ReasonCode != CloseClientShutdown || closeMsg.Flags != CloseFlagDrain || closeMsg.DrainTimeoutMillis != 500 {
		t.Fatalf("close msg = %+v", closeMsg)
	}
	if !transport.isClosed() || session.State() != SessionClosed {
		t.Fatalf("closed=%t state=%s", transport.isClosed(), session.State())
	}
}

func TestSessionHandleCloseSendsCloseAck(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSession(transport)
	closeFrame := NewFrame(FrameClose, SchemaClose, EncodeCloseMessage(CloseMessage{ReasonCode: CloseNormal, Flags: CloseFlagImmediate}))

	if err := session.HandleFrame(context.Background(), closeFrame); err != nil {
		t.Fatalf("handle close: %v", err)
	}
	ack := requireFrameType(t, transport.frames(), FrameCloseAck)
	closeMsg, err := DecodeCloseMessage(ack.Payload)
	if err != nil {
		t.Fatalf("decode close ack: %v", err)
	}
	if closeMsg.ReasonCode != CloseNormal || closeMsg.Flags != CloseFlagImmediate {
		t.Fatalf("close ack = %+v", closeMsg)
	}
	if !transport.isClosed() || session.State() != SessionClosed {
		t.Fatalf("closed=%t state=%s", transport.isClosed(), session.State())
	}
}
