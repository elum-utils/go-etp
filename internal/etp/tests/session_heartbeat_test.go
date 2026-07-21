package tests

import (
	"context"
	. "github.com/elum-utils/go-etp/internal/etp"
	"testing"
	"time"
)

func TestSessionHeartbeatPingPong(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSession(transport)

	if err := session.SendPing(); err != nil {
		t.Fatalf("send ping: %v", err)
	}
	if err := session.HandlePing(); err != nil {
		t.Fatalf("handle ping: %v", err)
	}
	session.HandlePong()

	frames := transport.frames()
	requireFrameType(t, frames, FramePing)
	requireFrameType(t, frames, FramePong)
}

func TestSessionHeartbeatTimeoutEmitsEvent(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSessionWithConfig(transport, SessionConfig{
		Role: RoleServer,
		Heartbeat: HeartbeatConfig{
			Interval: 10 * time.Millisecond,
			Timeout:  20 * time.Millisecond,
		},
	})
	time.Sleep(30 * time.Millisecond)

	events := make(chan ProtocolEvent, 1)
	session.OnProtocolEvent(func(event ProtocolEvent) {
		events <- event
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session.StartHeartbeat(ctx)

	select {
	case event := <-events:
		if event.Message != "heartbeat timeout" {
			t.Fatalf("event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for heartbeat event")
	}
	if session.State() != SessionFailed {
		t.Fatalf("state = %s", session.State())
	}
}

func TestClientHeartbeatSendsPingAfterWriteIdle(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSessionWithConfig(transport, SessionConfig{
		Role: RoleClient,
		Heartbeat: HeartbeatConfig{
			Interval: 10 * time.Millisecond,
			Timeout:  200 * time.Millisecond,
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session.StartHeartbeat(ctx)

	frames := waitForFrameCount(t, transport, 1)
	if frames[0].Header.FrameType != FramePing {
		t.Fatalf("frame = %+v", frames[0].Header)
	}
}

func TestServerHeartbeatDoesNotSendPing(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSessionWithConfig(transport, SessionConfig{
		Role: RoleServer,
		Heartbeat: HeartbeatConfig{
			Interval: 10 * time.Millisecond,
			Timeout:  200 * time.Millisecond,
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session.StartHeartbeat(ctx)
	time.Sleep(35 * time.Millisecond)

	if frames := transport.frames(); len(frames) != 0 {
		t.Fatalf("server sent frames: %+v", frames)
	}
}

func TestHeartbeatTimeoutIsExtendedByAnyIncomingFrame(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSessionWithConfig(transport, SessionConfig{
		Role: RoleServer,
		Heartbeat: HeartbeatConfig{
			Interval: 10 * time.Millisecond,
			Timeout:  35 * time.Millisecond,
		},
	})
	events := make(chan ProtocolEvent, 1)
	session.OnProtocolEvent(func(event ProtocolEvent) {
		events <- event
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session.StartHeartbeat(ctx)
	time.Sleep(20 * time.Millisecond)
	if err := session.HandleFrame(ctx, NewFrame(FramePing, 0, nil)); err != nil {
		t.Fatalf("handle ping: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	select {
	case event := <-events:
		t.Fatalf("unexpected heartbeat event: %+v", event)
	default:
	}
	if session.State() == SessionFailed {
		t.Fatalf("session failed despite incoming frame")
	}
}
