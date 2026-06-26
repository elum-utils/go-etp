package tests

import (
	. "github.com/elum-utils/go-etp"
	"sync"
	"testing"
	"time"
)

type recordingTransport struct {
	t      *testing.T
	mu     sync.Mutex
	sent   []Frame
	raw    [][]byte
	onSend func(Frame)
	closed bool
	err    error
}

func newRecordingTransport(t *testing.T) *recordingTransport {
	return &recordingTransport{t: t}
}

func (r *recordingTransport) SendFrame(data []byte) error {
	r.mu.Lock()
	if r.err != nil {
		err := r.err
		r.mu.Unlock()
		return err
	}
	raw := append([]byte(nil), data...)
	frame, err := DecodeFrame(raw)
	if err != nil {
		r.mu.Unlock()
		return err
	}
	r.raw = append(r.raw, raw)
	r.sent = append(r.sent, frame)
	onSend := r.onSend
	r.mu.Unlock()
	if onSend != nil {
		onSend(frame)
	}
	return nil
}

func (r *recordingTransport) ReadFrame() ([]byte, error) {
	r.t.Helper()
	r.t.Fatalf("ReadFrame is not implemented by recordingTransport")
	return nil, nil
}

func (r *recordingTransport) Close() error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	return nil
}

func (r *recordingTransport) frames() []Frame {
	deadline := time.Now().Add(50 * time.Millisecond)
	lastN := -1
	stableSince := time.Time{}
	for {
		r.mu.Lock()
		n := len(r.sent)
		r.mu.Unlock()
		now := time.Now()
		if n != lastN {
			lastN = n
			stableSince = now
		}
		if (n > 0 && now.Sub(stableSince) >= 2*time.Millisecond) || now.After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Frame, len(r.sent))
	copy(out, r.sent)
	return out
}

func waitForFrameCount(t *testing.T, r *recordingTransport, count int) []Frame {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		frames := r.frames()
		if len(frames) >= count {
			return frames
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected at least %d frames, got %d", count, len(frames))
		}
		time.Sleep(time.Millisecond)
	}
}

func (r *recordingTransport) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

func requireFrameType(t *testing.T, frames []Frame, frameType uint8) Frame {
	t.Helper()
	for _, frame := range frames {
		if frame.Header.FrameType == frameType {
			return frame
		}
	}
	t.Fatalf("frame type %d was not sent", frameType)
	return Frame{}
}
