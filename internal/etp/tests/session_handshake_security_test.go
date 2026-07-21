package tests

import (
	"context"
	"testing"

	. "github.com/elum-utils/go-etp/internal/etp"
)

func TestHandshakeRejectsApplicationFramesBeforeHello(t *testing.T) {
	transport := newUnnegotiatedRecordingTransport(t)
	session := NewSessionWithConfig(transport, DefaultServerConfig())
	request := NewFrame(FrameRequest, SchemaEvent, EncodeEventMessage(EventMessage{Event: "message.get"}))
	request.Header.RequestID = 1
	if err := session.HandleFrame(context.Background(), request); err == nil {
		t.Fatal("application frame was accepted before hello")
	}
	if session.State() != SessionNew {
		t.Fatalf("state = %s", session.State())
	}
	reported := requireFrameType(t, transport.frames(), FrameError)
	message, err := DecodeErrorMessage(reported.Payload)
	if err != nil || message.Code != ErrorBadState {
		t.Fatalf("protocol error = %+v err=%v", message, err)
	}
}

func TestHandshakeRejectsWrongRoleUnknownCapabilitiesAndInvalidLimits(t *testing.T) {
	tests := []struct {
		name  string
		hello Hello
	}{
		{name: "wrong role", hello: validHello(RoleServer)},
		{name: "unknown capability", hello: func() Hello { h := validHello(RoleClient); h.Capabilities = 1 << 63; return h }()},
		{name: "zero frame limit", hello: func() Hello { h := validHello(RoleClient); h.MaxFrameBytes = 0; return h }()},
		{name: "chunk exceeds frame", hello: func() Hello { h := validHello(RoleClient); h.MaxChunkSize = h.MaxFrameBytes; return h }()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			transport := newUnnegotiatedRecordingTransport(t)
			session := NewSessionWithConfig(transport, DefaultServerConfig())
			frame := NewFrame(FrameHello, SchemaHello, EncodeHelloMessage(tc.hello))
			if err := session.HandleFrame(context.Background(), frame); err == nil {
				t.Fatal("invalid hello was accepted")
			}
			if session.State() == SessionEstablished {
				t.Fatal("invalid hello established the session")
			}
		})
	}
}

func TestHandshakeEnforcesNegotiatedRemoteLimits(t *testing.T) {
	transport := newUnnegotiatedRecordingTransport(t)
	session := NewSessionWithConfig(transport, DefaultClientConfig())
	if err := session.SendHello(RoleClient); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	hello := validHello(RoleServer)
	hello.MaxFrameBytes = 128
	hello.MaxChunkSize = 64
	hello.MaxTransferBytes = 4
	ack := NewFrame(FrameHelloAck, SchemaHello, EncodeHelloMessage(hello))
	if err := session.HandleFrame(context.Background(), ack); err != nil {
		t.Fatalf("hello ack: %v", err)
	}
	if _, err := session.SendRequest("large.event", make([]byte, 256)); err == nil {
		t.Fatal("frame larger than negotiated remote limit was sent")
	}
	if _, err := session.SendTransfer("large.bin", ContentFile, &zeroReader{}, 5); err == nil {
		t.Fatal("transfer larger than negotiated remote limit was sent")
	}
}

func TestCapabilitiesUseIntersectionAndRejectUnnegotiatedFrames(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultServerConfig()
	config.Capabilities &^= CapabilityHeartbeat
	session := NewSessionWithConfig(transport, config)

	if session.NegotiatedCapabilities()&CapabilityHeartbeat != 0 {
		t.Fatal("out-of-band capability negotiation kept a locally disabled feature")
	}
	if err := session.SendPing(); err == nil {
		t.Fatal("locally disabled heartbeat was sent")
	}
	if err := session.HandleFrame(t.Context(), NewFrame(FramePing, 0, nil)); err == nil {
		t.Fatal("unnegotiated incoming heartbeat was accepted")
	}
	errorFrame := requireFrameType(t, transport.frames(), FrameError)
	message, err := DecodeErrorMessage(errorFrame.Payload)
	if err != nil || message.Code != ErrorUnsupportedFeature {
		t.Fatalf("unsupported feature error = %+v err=%v", message, err)
	}
}

func TestHelloNegotiatesCapabilityIntersection(t *testing.T) {
	transport := newUnnegotiatedRecordingTransport(t)
	config := DefaultServerConfig()
	config.Capabilities &^= CapabilityTransferResume
	session := NewSessionWithConfig(transport, config)
	hello := validHello(RoleClient)
	hello.Capabilities |= CapabilityTransferResume
	frame := NewFrame(FrameHello, SchemaHello, EncodeHelloMessage(hello))
	if err := session.HandleFrame(t.Context(), frame); err != nil {
		t.Fatalf("hello: %v", err)
	}
	if session.RemoteSupports(CapabilityTransferResume) || session.NegotiatedCapabilities()&CapabilityTransferResume != 0 {
		t.Fatal("locally disabled resume survived capability negotiation")
	}
}

type zeroReader struct{}

func (*zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
