package tests

import (
	"bytes"
	"context"
	. "github.com/elum-utils/go-etp/internal/etp"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSessionTransferCompletesAfterAck(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSession(transport)
	installTransferAcker(t, transport, session)

	transferID, err := session.SendTransfer("hello.txt", ContentFile, strings.NewReader("hello"), 5)
	if err != nil {
		t.Fatalf("send transfer: %v", err)
	}
	if transferID == 0 {
		t.Fatalf("transfer id was not assigned")
	}

	frames := transport.frames()
	requireFrameType(t, frames, FrameTransferBegin)
	requireFrameType(t, frames, FrameData)
	requireFrameType(t, frames, FrameTransferEnd)
}

func TestSessionTransferRejectsOversizedTransfer(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultSessionConfig(RoleClient)
	config.FlowControl.MaxTransferBytes = 4
	session := NewSessionWithConfig(transport, config)

	_, err := session.SendTransfer("too-big.bin", ContentFile, strings.NewReader("hello"), 5)
	if err == nil {
		t.Fatalf("expected oversized transfer error")
	}
}

func TestSessionTransferCancelSendsCancelFrame(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSession(transport)
	handle := session.StartTransfer(context.Background(), TransferOptions{
		Name:          "cancel.bin",
		ContentType:   ContentFile,
		Reader:        bytes.NewReader(bytes.Repeat([]byte("x"), DefaultChunkSize*4)),
		TotalSize:     uint64(DefaultChunkSize * 4),
		DelayPerChunk: 20 * time.Millisecond,
	})

	time.Sleep(10 * time.Millisecond)
	if err := handle.Cancel(CancelUser, CancelDeletePartial); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := <-handle.Done(); err == nil {
		t.Fatalf("expected canceled transfer error")
	}

	frame := requireFrameType(t, transport.frames(), FrameCancel)
	cancel, err := DecodeCancel(frame.Payload)
	if err != nil {
		t.Fatalf("decode cancel: %v", err)
	}
	if cancel.TransferID != handle.TransferID || cancel.ReasonCode != CancelUser {
		t.Fatalf("cancel mismatch: %+v", cancel)
	}
}

func TestSessionTransferRetriesAfterAckTimeout(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSessionWithConfig(transport, SessionConfig{
		FlowControl: FlowControlConfig{
			MaxConcurrentTransfers: 1,
			MaxInFlightChunks:      1,
			MaxInFlightBytes:       1024,
			MaxSendBufferBytes:     1024,
			MaxReceiveBufferBytes:  1024,
			AckTimeout:             10 * time.Millisecond,
			RetryLimit:             2,
			MaxTransferBytes:       1024,
			MaxChunkSize:           1024,
		},
		Heartbeat: HeartbeatConfig{Interval: time.Second, Timeout: 2 * time.Second},
	})

	var dataSends atomic.Int32
	transport.onSend = func(frame Frame) {
		if frame.Header.FrameType == FrameTransferEnd {
			completeOutgoingTransfer(t, session, frame.Header.TransferID, 5, 1)
			return
		}
		if frame.Header.FrameType != FrameData {
			return
		}
		if dataSends.Add(1) != 2 {
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

	_, err := session.SendTransfer("retry.txt", ContentFile, strings.NewReader("hello"), 5)
	if err != nil {
		t.Fatalf("send transfer: %v", err)
	}
	if dataSends.Load() < 2 {
		t.Fatalf("expected resend, got %d sends", dataSends.Load())
	}
}

func TestSessionTransferFailsAfterRetryLimit(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSessionWithConfig(transport, SessionConfig{
		FlowControl: FlowControlConfig{
			MaxConcurrentTransfers: 1,
			MaxInFlightChunks:      1,
			MaxInFlightBytes:       1024,
			MaxSendBufferBytes:     1024,
			MaxReceiveBufferBytes:  1024,
			AckTimeout:             10 * time.Millisecond,
			RetryLimit:             1,
			MaxTransferBytes:       1024,
			MaxChunkSize:           1024,
		},
		Heartbeat: HeartbeatConfig{Interval: time.Second, Timeout: 2 * time.Second},
	})

	events := make(chan ProtocolEvent, 1)
	session.OnProtocolEvent(func(event ProtocolEvent) {
		if event.Code == EventAckTimeout {
			events <- event
		}
	})

	_, err := session.SendTransfer("fail.txt", ContentFile, strings.NewReader("hello"), 5)
	if err == nil {
		t.Fatalf("expected ack timeout error")
	}
	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatalf("expected ack timeout event")
	}
}

func TestSessionTransferRetriesAfterNack(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSession(transport)
	var dataSends atomic.Int32
	var nacked atomic.Bool

	transport.onSend = func(frame Frame) {
		if frame.Header.FrameType == FrameTransferEnd {
			completeOutgoingTransfer(t, session, frame.Header.TransferID, 5, 1)
			return
		}
		if frame.Header.FrameType != FrameData {
			return
		}
		count := dataSends.Add(1)
		if count == 1 && nacked.CompareAndSwap(false, true) {
			nack := NewFrame(FrameNack, SchemaNack, EncodeNack(Nack{
				TransferID: frame.Header.TransferID,
				ChunkFrom:  frame.Header.ChunkID,
				ChunkTo:    frame.Header.ChunkID,
				ReasonCode: NackMissingChunk,
			}))
			if err := session.HandleNack(nack); err != nil {
				t.Errorf("handle nack: %v", err)
			}
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

	_, err := session.SendTransfer("nack.txt", ContentFile, strings.NewReader("hello"), 5)
	if err != nil {
		t.Fatalf("send transfer: %v", err)
	}
	if dataSends.Load() < 2 {
		t.Fatalf("expected nack resend")
	}
}

func TestSessionTransferConcurrentLimitRejectsBeforeBegin(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultSessionConfig(RoleClient)
	config.FlowControl.MaxConcurrentTransfers = 1
	config.FlowControl.AckTimeout = time.Second
	session := NewSessionWithConfig(transport, config)

	first := session.StartTransfer(context.Background(), TransferOptions{
		Name:        "first.txt",
		ContentType: ContentFile,
		Reader:      strings.NewReader("hello"),
		TotalSize:   5,
	})
	requireFrameType(t, waitFrames(t, transport, FrameTransferBegin), FrameTransferBegin)

	second := session.StartTransfer(context.Background(), TransferOptions{
		Name:        "second.txt",
		ContentType: ContentFile,
		Reader:      strings.NewReader("hello"),
		TotalSize:   5,
	})
	if err := <-second.Done(); err == nil {
		t.Fatalf("expected concurrent transfer limit error")
	}
	if count := frameCount(transport.frames(), FrameTransferBegin); count != 1 {
		t.Fatalf("transfer begin count = %d", count)
	}
	if err := first.Cancel(CancelUser, CancelDeletePartial); err != nil {
		t.Fatalf("cancel first: %v", err)
	}
	<-first.Done()
}
