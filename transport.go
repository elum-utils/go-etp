package etp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const MaxFrameBytes = 8 << 20

var ErrSlowloris = errors.New("slowloris detected")

type SlowlorisConfig struct {
	LengthTimeout    time.Duration
	FrameGrace       time.Duration
	MinReadRate      int64
	DisableDeadlines bool
}

func DefaultSlowlorisConfig() SlowlorisConfig {
	return SlowlorisConfig{
		LengthTimeout: 2 * time.Second,
		FrameGrace:    2 * time.Second,
		MinReadRate:   8 * 1024,
	}
}

type FrameTransport interface {
	SendFrame(frame []byte) error
	ReadFrame() ([]byte, error)
	Close() error
}

type DeadlineStream interface {
	io.Reader
	io.Writer
	io.Closer
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
}

type deadlineStream = DeadlineStream

type StreamTransport struct {
	conn      deadlineStream
	mu        sync.Mutex
	writeLen  [4]byte
	readMu    sync.Mutex
	readLen   [4]byte
	guard     SlowlorisConfig
	maxFrame  uint32
	deadlines bool
}

func NewStreamTransport(conn net.Conn) *StreamTransport {
	return NewStreamTransportWithSlowlorisGuard(conn, DefaultSlowlorisConfig())
}

func NewStreamTransportWithSlowlorisGuard(conn net.Conn, guard SlowlorisConfig) *StreamTransport {
	return NewStreamTransportForStream(conn, guard)
}

func NewStreamTransportForStream(conn deadlineStream, guard SlowlorisConfig) *StreamTransport {
	if guard.LengthTimeout <= 0 {
		guard.LengthTimeout = 2 * time.Second
	}
	if guard.FrameGrace <= 0 {
		guard.FrameGrace = 2 * time.Second
	}
	if guard.MinReadRate <= 0 {
		guard.MinReadRate = 8 * 1024
	}
	return &StreamTransport{
		conn:      conn,
		guard:     guard,
		maxFrame:  MaxFrameBytes,
		deadlines: !guard.DisableDeadlines,
	}
}

func (t *StreamTransport) SendFrame(frame []byte) error {
	if len(frame) > MaxFrameBytes {
		return errors.New("frame exceeds max size")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	binary.BigEndian.PutUint32(t.writeLen[:], uint32(len(frame)))
	if _, err := t.conn.Write(t.writeLen[:]); err != nil {
		return err
	}
	_, err := t.conn.Write(frame)
	return err
}

func (t *StreamTransport) SetWriteDeadline(deadline time.Time) error {
	if !t.deadlines {
		return nil
	}
	return t.conn.SetWriteDeadline(deadline)
}

func (t *StreamTransport) ReadFrame() ([]byte, error) {
	return t.ReadFrameInto(nil)
}

func (t *StreamTransport) ReadFrameInto(dst []byte) ([]byte, error) {
	t.readMu.Lock()
	defer t.readMu.Unlock()
	length := t.readLen[:]
	if err := t.setReadDeadline(t.guard.LengthTimeout); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(t.conn, length); err != nil {
		return nil, t.classifyReadError("read frame length", err)
	}

	n := binary.BigEndian.Uint32(length)
	if n > t.maxFrame {
		return nil, fmt.Errorf("incoming frame exceeds max size: %d > %d", n, t.maxFrame)
	}

	if err := t.setReadDeadline(t.frameReadBudget(n)); err != nil {
		return nil, err
	}
	if cap(dst) < int(n) {
		dst = make([]byte, n)
	} else {
		dst = dst[:n]
	}
	frame := dst
	if _, err := io.ReadFull(t.conn, frame); err != nil {
		return nil, t.classifyReadError("read frame body", err)
	}
	if err := t.clearReadDeadline(); err != nil {
		return nil, err
	}
	return frame, nil
}

func (t *StreamTransport) Close() error {
	return t.conn.Close()
}

func (t *StreamTransport) frameReadBudget(n uint32) time.Duration {
	bodyBudget := time.Duration(int64(n) * int64(time.Second) / t.guard.MinReadRate)
	return t.guard.FrameGrace + bodyBudget
}

func (t *StreamTransport) setReadDeadline(after time.Duration) error {
	if !t.deadlines {
		return nil
	}
	return t.conn.SetReadDeadline(time.Now().Add(after))
}

func (t *StreamTransport) clearReadDeadline() error {
	if !t.deadlines {
		return nil
	}
	return t.conn.SetReadDeadline(time.Time{})
}

func (t *StreamTransport) classifyReadError(op string, err error) error {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return fmt.Errorf("%w: %s timeout", ErrSlowloris, op)
	}
	return err
}

func Send(t FrameTransport, frame Frame) error {
	size := EncodedFrameSize(frame)
	bufp := getFrameBuffer(size)
	data, err := EncodeFrameInto((*bufp)[:0], frame)
	if err != nil {
		putFrameBuffer(bufp)
		return err
	}
	err = t.SendFrame(data)
	putFrameBuffer(bufp)
	return err
}

func Read(t FrameTransport) (Frame, error) {
	data, err := t.ReadFrame()
	if err != nil {
		return Frame{}, err
	}
	return DecodeFrameView(data)
}
