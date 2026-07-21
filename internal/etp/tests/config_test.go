package tests

import (
	. "github.com/elum-utils/go-etp/internal/etp"
	"testing"
	"time"
)

func TestDefaultSessionConfigNormalizesFlowControl(t *testing.T) {
	session := NewSessionWithConfig(nil, SessionConfig{
		Role: "test",
		Heartbeat: HeartbeatConfig{
			Interval: time.Second,
			Timeout:  2 * time.Second,
		},
	})

	if session.Config().FlowControl.MaxInFlightChunks == 0 {
		t.Fatalf("MaxInFlightChunks was not normalized")
	}
	if session.Config().FlowControl.MaxInFlightBytes == 0 {
		t.Fatalf("MaxInFlightBytes was not normalized")
	}
	if session.Config().FlowControl.MaxSendBufferBytes == 0 {
		t.Fatalf("MaxSendBufferBytes was not normalized")
	}
	if session.Config().FlowControl.MaxReceiveBufferBytes == 0 {
		t.Fatalf("MaxReceiveBufferBytes was not normalized")
	}
	if session.Config().FlowControl.TransferCommitTimeout == 0 {
		t.Fatalf("TransferCommitTimeout was not normalized")
	}
	if session.Config().Capabilities != DefaultCapabilities {
		t.Fatalf("capabilities were not normalized")
	}
}

func TestSessionConfigKeepsExplicitFlowControl(t *testing.T) {
	session := NewSessionWithConfig(nil, SessionConfig{
		Role:         "test",
		Capabilities: CapabilityAck,
		FlowControl: FlowControlConfig{
			MaxConcurrentTransfers: 2,
			MaxInFlightChunks:      3,
			MaxInFlightBytes:       4,
			MaxSendBufferBytes:     5,
			MaxReceiveBufferBytes:  6,
			AckTimeout:             time.Second,
			RetryLimit:             7,
			MaxTransferBytes:       8,
			MaxChunkSize:           9,
			TransferCommitTimeout:  10 * time.Second,
		},
		Heartbeat: HeartbeatConfig{Interval: time.Second, Timeout: 2 * time.Second},
	})

	if session.Config().Capabilities != CapabilityAck {
		t.Fatalf("capabilities overwritten")
	}
	if session.Config().FlowControl.MaxInFlightBytes != 4 {
		t.Fatalf("explicit flow control overwritten")
	}
	if session.Config().FlowControl.TransferCommitTimeout != 10*time.Second {
		t.Fatalf("explicit transfer commit timeout overwritten")
	}
}

func TestSessionConfigClampsHeartbeatToWireRange(t *testing.T) {
	tooSmall := NewSessionWithConfig(nil, SessionConfig{
		Heartbeat: HeartbeatConfig{Interval: time.Nanosecond, Timeout: time.Second},
	})
	if got := tooSmall.Config().Heartbeat.Interval; got != time.Millisecond {
		t.Fatalf("small heartbeat interval = %s", got)
	}

	wireMax := time.Duration(1<<32-1) * time.Millisecond
	tooLarge := NewSessionWithConfig(nil, SessionConfig{
		Heartbeat: HeartbeatConfig{Interval: wireMax + time.Millisecond, Timeout: wireMax + time.Hour},
	})
	if got := tooLarge.Config().Heartbeat.Interval; got != wireMax {
		t.Fatalf("large heartbeat interval = %s, want %s", got, wireMax)
	}
}
