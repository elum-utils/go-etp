package tests

import (
	"context"
	. "github.com/elum-utils/go-etp"
	"io"
	"sync"
	"testing"
	"time"
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

func (g *gatedTransport) ReadFrame() ([]byte, error) { return nil, nil }
func (g *gatedTransport) Close() error               { return nil }

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
