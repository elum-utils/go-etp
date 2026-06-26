package etp

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"
)

var ErrMultiStreamClosed = errors.New("multistream transport closed")

const multiStreamPrefaceSize = 2

type MultiStreamTransportConfig struct {
	Context      context.Context
	OpenStream   func(context.Context) (deadlineStream, error)
	AcceptStream func(context.Context) (deadlineStream, error)
	Guard        SlowlorisConfig
}

type MultiStreamTransport struct {
	ctx    context.Context
	cancel context.CancelFunc
	open   func(context.Context) (deadlineStream, error)
	guard  SlowlorisConfig

	mu            sync.Mutex
	outgoing      map[uint16]*StreamTransport
	incoming      map[uint16]*StreamTransport
	reads         chan []byte
	errs          chan error
	closed        bool
	writeDeadline time.Time
}

func NewMultiStreamTransport(config MultiStreamTransportConfig) *MultiStreamTransport {
	ctx := config.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	t := &MultiStreamTransport{
		ctx:      ctx,
		cancel:   cancel,
		open:     config.OpenStream,
		guard:    config.Guard,
		outgoing: make(map[uint16]*StreamTransport),
		incoming: make(map[uint16]*StreamTransport),
		reads:    make(chan []byte, 1024),
		errs:     make(chan error, 1),
	}
	if config.AcceptStream != nil {
		go t.acceptLoop(config.AcceptStream)
	}
	return t
}

func (t *MultiStreamTransport) SendFrame(frame []byte) error {
	header, err := DecodeHeader(frame)
	if err != nil {
		return err
	}
	return t.SendFrameOnChannel(header.ChannelID, frame)
}

func (t *MultiStreamTransport) SendFrameOnChannel(channelID uint16, frame []byte) error {
	stream, err := t.streamForChannel(channelID)
	if err != nil {
		return err
	}
	return stream.SendFrame(frame)
}

func (t *MultiStreamTransport) ReadFrame() ([]byte, error) {
	select {
	case frame := <-t.reads:
		return frame, nil
	case err := <-t.errs:
		return nil, err
	case <-t.ctx.Done():
		return nil, t.ctx.Err()
	}
}

func (t *MultiStreamTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.cancel()
	for _, stream := range t.outgoing {
		_ = stream.Close()
	}
	for _, stream := range t.incoming {
		_ = stream.Close()
	}
	t.mu.Unlock()
	return nil
}

func (t *MultiStreamTransport) SetWriteDeadline(deadline time.Time) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.writeDeadline = deadline
	for _, stream := range t.outgoing {
		if err := stream.SetWriteDeadline(deadline); err != nil {
			return err
		}
	}
	for _, stream := range t.incoming {
		if err := stream.SetWriteDeadline(deadline); err != nil {
			return err
		}
	}
	return nil
}

func (t *MultiStreamTransport) streamForChannel(channelID uint16) (*StreamTransport, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, ErrMultiStreamClosed
	}
	if stream := t.incoming[channelID]; stream != nil {
		t.mu.Unlock()
		return stream, nil
	}
	if stream := t.outgoing[channelID]; stream != nil {
		t.mu.Unlock()
		return stream, nil
	}
	open := t.open
	t.mu.Unlock()
	if open == nil {
		return nil, ErrMultiStreamClosed
	}
	raw, err := open(t.ctx)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	deadline := t.writeDeadline
	t.mu.Unlock()
	if !deadline.IsZero() {
		if err := raw.SetWriteDeadline(deadline); err != nil {
			_ = raw.Close()
			return nil, err
		}
	}
	if err := writeStreamPreface(raw, channelID); err != nil {
		_ = raw.Close()
		return nil, err
	}
	stream := NewStreamTransportForStream(raw, t.guard)
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = stream.Close()
		return nil, ErrMultiStreamClosed
	}
	if existing := t.incoming[channelID]; existing != nil {
		t.mu.Unlock()
		_ = stream.Close()
		return existing, nil
	}
	if existing := t.outgoing[channelID]; existing != nil {
		t.mu.Unlock()
		_ = stream.Close()
		return existing, nil
	}
	t.outgoing[channelID] = stream
	t.mu.Unlock()
	go t.readLoop(channelID, stream)
	return stream, nil
}

func (t *MultiStreamTransport) acceptLoop(accept func(context.Context) (deadlineStream, error)) {
	for {
		raw, err := accept(t.ctx)
		if err != nil {
			t.reportErr(err)
			return
		}
		channelID, err := readStreamPreface(raw)
		if err != nil {
			_ = raw.Close()
			t.reportErr(err)
			continue
		}
		stream := NewStreamTransportForStream(raw, t.guard)
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			_ = stream.Close()
			return
		}
		existing := t.incoming[channelID]
		t.incoming[channelID] = stream
		t.mu.Unlock()
		if existing != nil {
			_ = existing.Close()
		}
		go t.readLoop(channelID, stream)
	}
}

func (t *MultiStreamTransport) readLoop(_ uint16, stream *StreamTransport) {
	for {
		frame, err := stream.ReadFrame()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.reportErr(err)
			}
			return
		}
		select {
		case t.reads <- frame:
		case <-t.ctx.Done():
			return
		}
	}
}

func (t *MultiStreamTransport) reportErr(err error) {
	if err == nil {
		return
	}
	select {
	case t.errs <- err:
	default:
	}
}

func writeStreamPreface(w io.Writer, channelID uint16) error {
	var preface [multiStreamPrefaceSize]byte
	binary.BigEndian.PutUint16(preface[:], channelID)
	_, err := w.Write(preface[:])
	return err
}

func readStreamPreface(r io.Reader) (uint16, error) {
	var preface [multiStreamPrefaceSize]byte
	if _, err := io.ReadFull(r, preface[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(preface[:]), nil
}
