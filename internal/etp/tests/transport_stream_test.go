package tests

import (
	"encoding/binary"
	"errors"
	. "github.com/elum-utils/go-etp/internal/etp"
	"net"
	"testing"
	"time"
)

func TestStreamTransportRoundTrip(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	sender := NewStreamTransport(left)
	receiver := NewStreamTransport(right)

	want, err := EncodeFrame(NewFrame(FrameData, SchemaTextMessage, EncodeTextMessage("stream frame")))
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- sender.SendFrame(want)
	}()

	got, err := receiver.ReadFrame()
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("frame mismatch: %q", got)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("send frame: %v", err)
	}
}

func TestStreamTransportRejectsOversizedIncomingFrame(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	receiver := NewStreamTransport(right)

	go func() {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], MaxFrameBytes+1)
		_, _ = left.Write(length[:])
	}()

	if _, err := receiver.ReadFrame(); err == nil {
		t.Fatalf("expected oversized frame error")
	}
}

func TestStreamTransportDetectsSlowlorisLengthTimeout(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	receiver := NewStreamTransportWithSlowlorisGuard(right, SlowlorisConfig{
		LengthTimeout: 20 * time.Millisecond,
		FrameGrace:    time.Second,
		MinReadRate:   1024,
	})
	go func() {
		_, _ = left.Write([]byte{0})
	}()

	if _, err := receiver.ReadFrame(); !errors.Is(err, ErrSlowloris) {
		t.Fatalf("expected ErrSlowloris, got %v", err)
	}
}

func TestStreamTransportDetectsSlowlorisBodyTimeout(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	receiver := NewStreamTransportWithSlowlorisGuard(right, SlowlorisConfig{
		LengthTimeout: time.Second,
		FrameGrace:    20 * time.Millisecond,
		MinReadRate:   1024 * 1024,
	})

	go func() {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], HeaderSize)
		_, _ = left.Write(length[:])
		_, _ = left.Write([]byte{1})
	}()

	if _, err := receiver.ReadFrame(); !errors.Is(err, ErrSlowloris) {
		t.Fatalf("expected ErrSlowloris, got %v", err)
	}
}

func TestStreamTransportDetectsSlowlorisDuringAuthFrame(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	receiver := NewStreamTransportWithSlowlorisGuard(right, SlowlorisConfig{
		LengthTimeout: time.Second,
		FrameGrace:    20 * time.Millisecond,
		MinReadRate:   1024 * 1024,
	})
	auth := NewFrame(FrameAuth, SchemaAuth, EncodeAuthRequest(AuthRequest{
		Method:  AuthMethodBearer,
		Payload: []byte("token"),
	}))
	encoded, err := EncodeFrame(auth)
	if err != nil {
		t.Fatalf("encode auth frame: %v", err)
	}

	go func() {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(encoded)))
		_, _ = left.Write(length[:])
		_, _ = left.Write(encoded[:HeaderSize/2])
	}()

	if _, err := receiver.ReadFrame(); !errors.Is(err, ErrSlowloris) {
		t.Fatalf("expected ErrSlowloris for partial auth frame, got %v", err)
	}
}
