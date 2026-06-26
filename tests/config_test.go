package tests

import (
	. "github.com/elum-utils/go-etp"
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
		},
		Heartbeat: HeartbeatConfig{Interval: time.Second, Timeout: 2 * time.Second},
	})

	if session.Config().Capabilities != CapabilityAck {
		t.Fatalf("capabilities overwritten")
	}
	if session.Config().FlowControl.MaxInFlightBytes != 4 {
		t.Fatalf("explicit flow control overwritten")
	}
}
