package tests

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	. "github.com/elum-utils/go-etp/internal/etp"
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
	if msg.Code != ErrorProtocolViolation || msg.RequestID != 77 || msg.FrameType != FrameRequest {
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

func TestSessionSendsGoAwayWithTerminalStateAndIDs(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSession(transport)
	if err := session.SendGoAway(GoAway{ReasonCode: CloseServerShutdown, Message: "restart"}); err != nil {
		t.Fatalf("send goaway: %v", err)
	}
	frame := requireFrameType(t, transport.frames(), FrameGoAway)
	message, err := DecodeGoAway(frame.Payload)
	if err != nil {
		t.Fatalf("decode goaway: %v", err)
	}
	if message.Flags != CloseFlagImmediate || message.LastAcceptedRequestID == 0 || message.LastAcceptedTransferID == 0 {
		t.Fatalf("goaway = %+v", message)
	}
	if session.State() != SessionClosing {
		t.Fatalf("state = %s", session.State())
	}
}

func TestSessionSendGoAwayFailureMarksSessionFailed(t *testing.T) {
	transport := newRecordingTransport(t)
	transport.err = errors.New("carrier write failed")
	session := NewSession(transport)
	if err := session.SendGoAway(GoAway{ReasonCode: CloseServerShutdown}); err == nil {
		t.Fatal("goaway write failure was hidden")
	}
	if session.State() != SessionFailed {
		t.Fatalf("state after goaway write failure = %s", session.State())
	}
}

func TestGracefulCloseCapabilityIsEnforcedForLocalAndRemoteDrain(t *testing.T) {
	transport := &recordingTransport{t: t, caps: DefaultCapabilities &^ CapabilityGracefulClose}
	session := NewSessionWithConfig(transport, DefaultServerConfig())

	if err := session.SendGoAway(GoAway{ReasonCode: CloseServerShutdown, Flags: CloseFlagDrain}); err == nil {
		t.Fatal("local draining goaway was accepted without capability")
	}
	if err := session.CloseWith(CloseMessage{ReasonCode: CloseServerShutdown, Flags: CloseFlagDrain}); err == nil {
		t.Fatal("local draining close was accepted without capability")
	}

	goAway := NewFrame(FrameGoAway, SchemaGoAway, EncodeGoAway(GoAway{ReasonCode: CloseServerShutdown, Flags: CloseFlagDrain}))
	if err := session.HandleFrame(context.Background(), goAway); err == nil {
		t.Fatal("remote draining goaway was accepted without capability")
	}
	closeFrame := NewFrame(FrameClose, SchemaClose, EncodeCloseMessage(CloseMessage{ReasonCode: CloseServerShutdown, Flags: CloseFlagDrain}))
	if err := session.HandleFrame(context.Background(), closeFrame); err == nil {
		t.Fatal("remote draining close was accepted without capability")
	}
	if session.State() != SessionEstablished {
		t.Fatalf("state changed after rejected drain: %s", session.State())
	}
	errorsSent := 0
	for _, frame := range transport.frames() {
		if frame.Header.FrameType != FrameError {
			continue
		}
		message, err := DecodeErrorMessage(frame.Payload)
		if err != nil || message.Code != ErrorUnsupportedFeature {
			t.Fatalf("capability error = %+v err=%v", message, err)
		}
		errorsSent++
	}
	if errorsSent != 2 {
		t.Fatalf("unsupported feature errors = %d, want 2", errorsSent)
	}
}

func TestSessionCloseWithSendsClosePayload(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSession(transport)
	transport.onSend = func(frame Frame) {
		if frame.Header.FrameType != FrameClose {
			return
		}
		ack := NewFrame(FrameCloseAck, SchemaClose, frame.Payload)
		if err := session.HandleFrame(context.Background(), ack); err != nil {
			t.Errorf("handle close ack: %v", err)
		}
	}

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
	ack := requireFrameType(t, waitForFrameCount(t, transport, 1), FrameCloseAck)
	closeMsg, err := DecodeCloseMessage(ack.Payload)
	if err != nil {
		t.Fatalf("decode close ack: %v", err)
	}
	if closeMsg.ReasonCode != CloseNormal || closeMsg.Flags != CloseFlagImmediate {
		t.Fatalf("close ack = %+v", closeMsg)
	}
	deadline := time.Now().Add(time.Second)
	for session.State() != SessionClosed && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !transport.isClosed() || session.State() != SessionClosed {
		t.Fatalf("closed=%t state=%s", transport.isClosed(), session.State())
	}
}

func TestImmediateCloseMakesBothRunLoopsReturnCleanly(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewSessionWithConfig(NewStreamTransport(left), DefaultClientConfig())
	server := NewSessionWithConfig(NewStreamTransport(right), DefaultServerConfig())
	clientRun := make(chan error, 1)
	serverRun := make(chan error, 1)
	go func() { clientRun <- client.Run(ctx) }()
	go func() { serverRun <- server.Run(ctx) }()
	handshakeSessions(t, client, server)

	if err := client.CloseWith(CloseMessage{ReasonCode: CloseClientShutdown, Flags: CloseFlagImmediate}); err != nil {
		t.Fatalf("close client: %v", err)
	}
	for name, result := range map[string]<-chan error{"client": clientRun, "server": serverRun} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("%s clean close returned an error: %v (state=%s)", name, err, map[string]*Session{"client": client, "server": server}[name].State())
			}
		case <-time.After(time.Second):
			t.Fatal("Run did not return after clean close")
		}
	}
}

func TestImmediateCloseTimesOutAndStillClosesCarrierWithoutAck(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultClientConfig()
	config.Close.AckTimeout = 10 * time.Millisecond
	session := NewSessionWithConfig(transport, config)

	err := session.Close()
	if err == nil {
		t.Fatal("close without peer acknowledgement unexpectedly succeeded")
	}
	if !transport.isClosed() || session.State() != SessionClosed {
		t.Fatalf("close timeout left resources open: closed=%t state=%s", transport.isClosed(), session.State())
	}
	if err := session.Close(); err != nil {
		t.Fatalf("second close was not idempotent: %v", err)
	}
}

func TestSimultaneousImmediateCloseDoesNotDeadlock(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewSessionWithConfig(NewStreamTransport(left), DefaultClientConfig())
	server := NewSessionWithConfig(NewStreamTransport(right), DefaultServerConfig())
	clientRun := make(chan error, 1)
	serverRun := make(chan error, 1)
	go func() { clientRun <- client.Run(ctx) }()
	go func() { serverRun <- server.Run(ctx) }()
	handshakeSessions(t, client, server)

	closeResults := make(chan error, 2)
	go func() { closeResults <- client.Close() }()
	go func() { closeResults <- server.Close() }()
	for i := 0; i < 2; i++ {
		select {
		case err := <-closeResults:
			if err != nil {
				t.Fatalf("simultaneous close: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("simultaneous close deadlocked")
		}
	}
	for name, result := range map[string]<-chan error{"client": clientRun, "server": serverRun} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("%s Run after simultaneous close: %v", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s Run did not stop after simultaneous close", name)
		}
	}
}
