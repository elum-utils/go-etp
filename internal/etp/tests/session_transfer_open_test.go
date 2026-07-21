package tests

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/elum-utils/go-etp/internal/etp"
)

func TestTransferOpenTimeoutIsHardAndKeepsLeakedHandlerAccounted(t *testing.T) {
	transport := newRecordingTransport(t)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	firstWriter := &memoryTransferWriter{}
	var calls atomic.Int32
	config := DefaultServerConfig()
	config.FlowControl.MaxConcurrentTransfers = 1
	config.FlowControl.TransferOpenTimeout = 20 * time.Millisecond
	config.Receive.TransferHandler = func(context.Context, IncomingTransferInfo) (IncomingTransferWriter, error) {
		call := calls.Add(1)
		if call == 1 {
			entered <- struct{}{}
			<-release
			return firstWriter, nil
		}
		return &memoryTransferWriter{}, nil
	}
	session := NewSessionWithConfig(transport, config)

	started := time.Now()
	err := session.HandleFrame(context.Background(), transferBeginFrame(1))
	if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("first begin error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("transfer handler timeout took %s", elapsed)
	}
	<-entered

	if err := session.HandleFrame(context.Background(), transferBeginFrame(2)); err == nil || !strings.Contains(err.Error(), "too many incoming transfers") {
		t.Fatalf("second begin bypassed opening limit: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("handler calls = %d, want 1", calls.Load())
	}

	close(release)
	deadline := time.Now().Add(time.Second)
	for {
		err = session.HandleFrame(context.Background(), transferBeginFrame(3))
		if err == nil {
			break
		}
		if !strings.Contains(err.Error(), "too many incoming transfers") || time.Now().After(deadline) {
			t.Fatalf("opening slot was not released: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	_, _, aborted := firstWriter.snapshot()
	if !aborted {
		t.Fatal("writer returned after timeout was not aborted")
	}
}

func TestTransferHandlerPanicReturnsNack(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultServerConfig()
	config.Receive.TransferHandler = func(context.Context, IncomingTransferInfo) (IncomingTransferWriter, error) {
		panic("storage opener failed")
	}
	session := NewSessionWithConfig(transport, config)
	if err := session.HandleFrame(context.Background(), transferBeginFrame(10)); err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("transfer handler panic error = %v", err)
	}
	frame := requireFrameType(t, transport.frames(), FrameNack)
	nack, err := DecodeNack(frame.Payload)
	if err != nil || nack.ReasonCode != NackWriteFailed {
		t.Fatalf("panic nack = %+v err=%v", nack, err)
	}
}

func transferBeginFrame(transferID uint64) Frame {
	frame := NewFrame(FrameTransferBegin, SchemaTransferBegin, EncodeTransferBegin(TransferBegin{
		TotalSize:  1,
		ChunkSize:  1,
		ChunkCount: 1,
	}))
	frame.Header.TransferID = transferID
	return frame
}
