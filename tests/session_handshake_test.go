package tests

import (
	. "github.com/elum-utils/go-etp"
	"testing"
)

func TestSessionHandshakeSendsCapabilities(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSessionWithConfig(transport, SessionConfig{
		Role:         "client",
		Capabilities: CapabilityAck | CapabilityHeartbeat,
	})

	if err := session.SendHello(""); err != nil {
		t.Fatalf("send hello: %v", err)
	}

	frame := requireFrameType(t, transport.frames(), FrameHello)
	hello, err := DecodeHelloMessage(frame.Payload)
	if err != nil {
		t.Fatalf("decode hello: %v", err)
	}
	if hello.Role != RoleClient {
		t.Fatalf("role = %q", hello.Role)
	}
	if hello.Capabilities != CapabilityAck|CapabilityHeartbeat {
		t.Fatalf("capabilities = %d", hello.Capabilities)
	}
	if session.State() != SessionHelloSent {
		t.Fatalf("state = %s", session.State())
	}
}

func TestSessionHelloAckEstablishesSession(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSession(transport)

	if err := session.SendHelloAck(RoleServer); err != nil {
		t.Fatalf("send hello ack: %v", err)
	}

	requireFrameType(t, transport.frames(), FrameHelloAck)
	if session.State() != SessionEstablished {
		t.Fatalf("state = %s", session.State())
	}
}
