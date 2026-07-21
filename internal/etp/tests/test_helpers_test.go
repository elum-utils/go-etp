package tests

import (
	. "github.com/elum-utils/go-etp/internal/etp"
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
	caps   uint64
}

func newRecordingTransport(t *testing.T) *recordingTransport {
	return &recordingTransport{t: t, caps: DefaultCapabilities}
}

func newUnnegotiatedRecordingTransport(t *testing.T) *recordingTransport {
	return &recordingTransport{t: t}
}

func (r *recordingTransport) NegotiatedCapabilities() uint64 { return r.caps }

func validHello(role string) Hello {
	return Hello{
		Role:              role,
		Capabilities:      DefaultCapabilities,
		MaxFrameBytes:     MaxFrameBytes,
		MaxChunkSize:      64 * 1024,
		MaxTransferBytes:  512 << 20,
		MaxInFlightChunks: 16,
		HeartbeatMillis:   10_000,
	}
}

func installTransferAcker(t *testing.T, transport *recordingTransport, session *Session) {
	t.Helper()
	type counters struct {
		bytes uint64
		next  uint32
	}
	var mu sync.Mutex
	transfers := make(map[uint64]counters)
	transport.onSend = func(frame Frame) {
		switch frame.Header.FrameType {
		case FrameData:
			if frame.Header.TransferID == 0 {
				return
			}
			mu.Lock()
			state := transfers[frame.Header.TransferID]
			state.bytes += uint64(len(frame.Payload))
			if next := frame.Header.ChunkID + 1; next > state.next {
				state.next = next
			}
			transfers[frame.Header.TransferID] = state
			mu.Unlock()
			ack := NewFrame(FrameAck, SchemaAck, EncodeAck(Ack{
				TransferID:    frame.Header.TransferID,
				ChunkFrom:     frame.Header.ChunkID,
				ChunkTo:       frame.Header.ChunkID,
				ReceivedBytes: state.bytes,
			}))
			ack.Header.TransferID = frame.Header.TransferID
			if err := session.HandleAck(ack); err != nil {
				t.Errorf("handle ack: %v", err)
			}
		case FrameTransferEnd:
			mu.Lock()
			state := transfers[frame.Header.TransferID]
			mu.Unlock()
			completed := NewFrame(FrameTransferState, SchemaTransferState, EncodeTransferStateMessage(TransferStateMessage{
				TransferID:    frame.Header.TransferID,
				ReceivedBytes: state.bytes,
				NextChunk:     state.next,
				Flags:         TransferStateFlagCompleted,
			}))
			completed.Header.TransferID = frame.Header.TransferID
			if err := session.HandleFrame(t.Context(), completed); err != nil {
				t.Errorf("handle transfer completion: %v", err)
			}
		}
	}
}

func completeOutgoingTransfer(t *testing.T, session *Session, transferID uint64, receivedBytes uint64, nextChunk uint32) {
	t.Helper()
	frame := NewFrame(FrameTransferState, SchemaTransferState, EncodeTransferStateMessage(TransferStateMessage{
		TransferID:    transferID,
		ReceivedBytes: receivedBytes,
		NextChunk:     nextChunk,
		Flags:         TransferStateFlagCompleted,
	}))
	frame.Header.TransferID = transferID
	if err := session.HandleFrame(t.Context(), frame); err != nil {
		t.Errorf("complete outgoing transfer: %v", err)
	}
}

func handshakeSessions(t *testing.T, client *Session, server *Session) {
	t.Helper()
	if err := client.SendHello(RoleClient); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for client.State() != SessionEstablished || server.State() != SessionEstablished {
		if client.State() == SessionFailed || server.State() == SessionFailed {
			t.Fatalf("handshake failed: client=%s server=%s", client.State(), server.State())
		}
		if time.Now().After(deadline) {
			t.Fatalf("handshake timeout: client=%s server=%s", client.State(), server.State())
		}
		time.Sleep(time.Millisecond)
	}
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
