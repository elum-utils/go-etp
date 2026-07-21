package tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	. "github.com/elum-utils/go-etp/internal/etp"
)

type partialDeadlineStream struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	max    int
}

type closeAfterReadStream struct {
	buffer bytes.Buffer
}

func (s *closeAfterReadStream) Read(p []byte) (int, error)     { return s.buffer.Read(p) }
func (*closeAfterReadStream) Write([]byte) (int, error)        { return 0, io.ErrClosedPipe }
func (*closeAfterReadStream) Close() error                     { return nil }
func (*closeAfterReadStream) SetWriteDeadline(time.Time) error { return nil }
func (s *closeAfterReadStream) SetReadDeadline(deadline time.Time) error {
	if deadline.IsZero() && s.buffer.Len() == 0 {
		return io.ErrClosedPipe
	}
	return nil
}

func (s *partialDeadlineStream) Read(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.buffer.Len() == 0 {
		return 0, io.EOF
	}
	return s.buffer.Read(p)
}

func (s *partialDeadlineStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(p) > s.max {
		p = p[:s.max]
	}
	return s.buffer.Write(p)
}

func (*partialDeadlineStream) Close() error                     { return nil }
func (*partialDeadlineStream) SetReadDeadline(time.Time) error  { return nil }
func (*partialDeadlineStream) SetWriteDeadline(time.Time) error { return nil }

func TestStreamTransportCompletesPartialWrites(t *testing.T) {
	stream := &partialDeadlineStream{max: 1}
	transport := NewStreamTransportForStream(stream, DefaultSlowlorisConfig())
	encoded, err := EncodeFrame(NewFrame(FramePing, 0, nil))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := transport.SendFrame(encoded); err != nil {
		t.Fatalf("send: %v", err)
	}
	stream.mu.Lock()
	written := append([]byte(nil), stream.buffer.Bytes()...)
	stream.mu.Unlock()
	if len(written) != 4+len(encoded) || binary.BigEndian.Uint32(written[:4]) != uint32(len(encoded)) || !bytes.Equal(written[4:], encoded) {
		t.Fatalf("partial writes corrupted frame: bytes=%d", len(written))
	}
}

func TestStreamTransportKeepsFullyReadFrameWhenPeerCloses(t *testing.T) {
	encoded, err := EncodeFrame(NewFrame(FrameCloseAck, SchemaClose, EncodeCloseMessage(CloseMessage{Flags: CloseFlagImmediate})))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	stream := &closeAfterReadStream{}
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(encoded)))
	stream.buffer.Write(prefix[:])
	stream.buffer.Write(encoded)

	transport := NewStreamTransportForStream(stream, DefaultSlowlorisConfig())
	got, err := transport.ReadFrame()
	if err != nil {
		t.Fatalf("fully read frame was lost when deadline clear observed peer close: %v", err)
	}
	if !bytes.Equal(got, encoded) {
		t.Fatal("frame changed while handling peer close")
	}
}

func TestStreamTransportIdleIsNotSlowlorisUntilPrefixStarts(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	transport := NewStreamTransportWithSlowlorisGuard(right, SlowlorisConfig{LengthTimeout: 15 * time.Millisecond, FrameGrace: time.Second, MinReadRate: 1024})
	done := make(chan error, 1)
	go func() {
		_, err := transport.ReadFrame()
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("idle connection was classified as slowloris: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	_ = left.Close()
	<-done
}

func TestSessionAppliesSmallPreHandshakeFrameLimit(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	transport := NewStreamTransport(right)
	config := DefaultServerConfig()
	config.Payload.PreHandshakeBytes = 128
	_ = NewSessionWithConfig(transport, config)
	go func() {
		var prefix [4]byte
		binary.BigEndian.PutUint32(prefix[:], 129)
		_, _ = left.Write(prefix[:])
	}()
	if _, err := transport.ReadFrame(); err == nil {
		t.Fatal("pre-handshake frame limit was not enforced before allocation")
	}
}

func TestSessionSurfacesMalformedCarrierFrameAsProtocolViolation(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	session := NewSessionWithConfig(NewStreamTransport(right), DefaultServerConfig())
	events := make(chan ProtocolEvent, 1)
	session.OnProtocolEvent(func(event ProtocolEvent) {
		if event.Code == EventProtocolViolation {
			events <- event
		}
	})
	runResult := make(chan error, 1)
	go func() { runResult <- session.Run(context.Background()) }()
	go func() {
		var prefix [4]byte
		binary.BigEndian.PutUint32(prefix[:], HeaderSize)
		_, _ = left.Write(prefix[:])
		_, _ = left.Write(make([]byte, HeaderSize))
	}()

	select {
	case err := <-runResult:
		if !errors.Is(err, ErrInvalidFrame) {
			t.Fatalf("Run error = %v, want ErrInvalidFrame", err)
		}
	case <-time.After(time.Second):
		t.Fatal("malformed frame did not stop the session")
	}
	select {
	case event := <-events:
		if event.Message == "" {
			t.Fatal("protocol violation event did not contain decoder context")
		}
	case <-time.After(time.Second):
		t.Fatal("malformed frame was not surfaced to the policy layer")
	}
}
