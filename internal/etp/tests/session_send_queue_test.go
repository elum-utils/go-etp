package tests

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	. "github.com/elum-utils/go-etp/internal/etp"
)

type gatedTransport struct {
	mu      sync.Mutex
	gate    chan struct{}
	frames  []Frame
	entered chan struct{}
}

func newGatedTransport() *gatedTransport {
	return &gatedTransport{
		gate:    make(chan struct{}),
		entered: make(chan struct{}, 1),
	}
}

func (g *gatedTransport) SendFrame(data []byte) error {
	select {
	case g.entered <- struct{}{}:
	default:
	}
	<-g.gate
	frame, err := DecodeFrame(data)
	if err != nil {
		return err
	}
	g.mu.Lock()
	g.frames = append(g.frames, frame)
	g.mu.Unlock()
	return nil
}

func (g *gatedTransport) ReadFrame() ([]byte, error)     { return nil, nil }
func (g *gatedTransport) Close() error                   { return nil }
func (g *gatedTransport) NegotiatedCapabilities() uint64 { return DefaultCapabilities }

func (g *gatedTransport) release(n int) {
	for i := 0; i < n; i++ {
		g.gate <- struct{}{}
	}
}

func (g *gatedTransport) snapshot() []Frame {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]Frame, len(g.frames))
	copy(out, g.frames)
	return out
}

func TestSessionWriterPrioritizesControlDuringBulkTransfer(t *testing.T) {
	transport := newGatedTransport()
	config := DefaultSessionConfig("sender")
	config.FlowControl.MaxInFlightChunks = 16
	config.FlowControl.MaxInFlightBytes = 1 << 20
	config.FlowControl.MaxTransferBytes = 1 << 20
	config.FlowControl.MaxChunkSize = 32 * 1024
	config.SendQueue.MaxFrames = 64
	config.SendQueue.MaxBytes = 2 << 20
	session := NewSessionWithConfig(transport, config)

	payload := blockingReader{remaining: 96 * 1024}
	handle := session.StartTransfer(context.Background(), TransferOptions{
		Name:        "bulk.bin",
		ContentType: ContentFile,
		Reader:      &payload,
		TotalSize:   96 * 1024,
		ChunkSize:   32 * 1024,
	})
	defer handle.Cancel(CancelUser, 0)

	select {
	case <-transport.entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not start first bulk frame")
	}

	start := time.Now()
	if err := session.SendPing(); err != nil {
		t.Fatalf("send ping: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 25*time.Millisecond {
		t.Fatalf("send ping blocked behind bulk write for %s", elapsed)
	}

	transport.release(2)
	deadline := time.Now().Add(time.Second)
	for {
		frames := transport.snapshot()
		if len(frames) >= 2 {
			if frames[0].Header.FrameType != FrameTransferBegin {
				t.Fatalf("first frame = %d, want transfer begin", frames[0].Header.FrameType)
			}
			if frames[1].Header.FrameType != FramePing {
				t.Fatalf("second frame = %d, want ping before bulk data", frames[1].Header.FrameType)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("frames were not written: %+v", frames)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSessionWriterFailureFailsSessionImmediately(t *testing.T) {
	transport := newRecordingTransport(t)
	transport.mu.Lock()
	transport.err = errors.New("write failed")
	transport.mu.Unlock()
	session := NewSession(transport)
	events := make(chan ProtocolEvent, 1)
	session.OnProtocolEvent(func(event ProtocolEvent) {
		if event.Code == EventWriteFailed {
			events <- event
		}
	})

	if err := session.SendText("message"); err != nil {
		t.Fatalf("enqueue text: %v", err)
	}
	select {
	case event := <-events:
		if event.Message != "write failed" {
			t.Fatalf("write event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("session did not surface the asynchronous write failure")
	}
	if session.State() != SessionFailed || !transport.isClosed() {
		t.Fatalf("state=%s transport closed=%t", session.State(), transport.isClosed())
	}
}

type blockingReader struct {
	remaining int
}

func (r *blockingReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	if len(p) > r.remaining {
		p = p[:r.remaining]
	}
	for i := range p {
		p[i] = byte(i)
	}
	r.remaining -= len(p)
	return len(p), nil
}
