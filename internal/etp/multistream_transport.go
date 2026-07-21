package etp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
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
	ReadQueue    int
	PrefaceLimit int
}

type MultiStreamTransport struct {
	ctx    context.Context
	cancel context.CancelFunc
	open   func(context.Context) (deadlineStream, error)
	guard  SlowlorisConfig

	mu            sync.Mutex
	openMu        [ChannelBackground + 1]sync.Mutex
	readWG        sync.WaitGroup
	prefaceWG     sync.WaitGroup
	outgoing      map[uint16]*StreamTransport
	incoming      map[uint16]*StreamTransport
	pending       map[*pendingMultiStream]struct{}
	reads         chan *FrameLease
	errs          chan error
	prefaces      chan struct{}
	closed        bool
	writeDeadline time.Time
}

type pendingMultiStream struct {
	raw deadlineStream
}

func NewMultiStreamTransport(config MultiStreamTransportConfig) *MultiStreamTransport {
	ctx := config.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	if config.ReadQueue <= 0 {
		config.ReadQueue = 64
	}
	if config.PrefaceLimit <= 0 {
		config.PrefaceLimit = 8
	}
	t := &MultiStreamTransport{
		ctx:      ctx,
		cancel:   cancel,
		open:     config.OpenStream,
		guard:    config.Guard,
		outgoing: make(map[uint16]*StreamTransport),
		incoming: make(map[uint16]*StreamTransport),
		pending:  make(map[*pendingMultiStream]struct{}),
		reads:    make(chan *FrameLease, config.ReadQueue),
		errs:     make(chan error, 1),
		prefaces: make(chan struct{}, config.PrefaceLimit),
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
	if !validChannel(channelID) {
		return fmt.Errorf("invalid multistream channel: %d", channelID)
	}
	header, err := DecodeHeader(frame)
	if err != nil {
		return err
	}
	if header.ChannelID != channelID {
		return fmt.Errorf("frame channel %d does not match stream channel %d", header.ChannelID, channelID)
	}
	stream, err := t.streamForChannel(channelID)
	if err != nil {
		return err
	}
	return stream.SendFrame(frame)
}

func (t *MultiStreamTransport) ReadFrame() ([]byte, error) {
	lease, err := t.ReadFrameLease()
	if err != nil {
		return nil, err
	}
	frame := append([]byte(nil), lease.Data...)
	lease.Release()
	return frame, nil
}

func (t *MultiStreamTransport) ReadFrameLease() (*FrameLease, error) {
	select {
	case lease := <-t.reads:
		return lease, nil
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
	streams := make([]*StreamTransport, 0, len(t.outgoing)+len(t.incoming))
	pending := make([]deadlineStream, 0, len(t.pending))
	for _, stream := range t.outgoing {
		streams = append(streams, stream)
	}
	for _, stream := range t.incoming {
		streams = append(streams, stream)
	}
	for stream := range t.pending {
		pending = append(pending, stream.raw)
	}
	t.mu.Unlock()
	for _, stream := range pending {
		_ = stream.Close()
	}
	for _, stream := range streams {
		_ = stream.Close()
	}
	t.prefaceWG.Wait()
	t.readWG.Wait()
	for {
		select {
		case lease := <-t.reads:
			lease.Release()
		default:
			return nil
		}
	}
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

func (t *MultiStreamTransport) SetMaxFrameBytes(max uint32) {
	if max == 0 || max > MaxFrameBytes {
		max = MaxFrameBytes
	}
	t.mu.Lock()
	t.guard.MaxFrameBytes = max
	for _, stream := range t.outgoing {
		stream.SetMaxFrameBytes(max)
	}
	for _, stream := range t.incoming {
		stream.SetMaxFrameBytes(max)
	}
	t.mu.Unlock()
}

func (t *MultiStreamTransport) streamForChannel(channelID uint16) (*StreamTransport, error) {
	if !validChannel(channelID) {
		return nil, fmt.Errorf("invalid multistream channel: %d", channelID)
	}
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
	t.openMu[channelID].Lock()
	defer t.openMu[channelID].Unlock()
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
	open = t.open
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
	stream := NewStreamTransportForStream(raw, t.guardConfig())
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
	t.startReadLoop(channelID, stream)
	return stream, nil
}

func (t *MultiStreamTransport) acceptLoop(accept func(context.Context) (deadlineStream, error)) {
	for {
		select {
		case t.prefaces <- struct{}{}:
		case <-t.ctx.Done():
			return
		}
		raw, err := accept(t.ctx)
		if err != nil {
			<-t.prefaces
			if t.ctx.Err() != nil {
				return
			}
			t.reportErr(err)
			return
		}
		pending := &pendingMultiStream{raw: raw}
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			_ = raw.Close()
			<-t.prefaces
			return
		}
		t.pending[pending] = struct{}{}
		t.prefaceWG.Add(1)
		t.mu.Unlock()
		go t.acceptStream(pending)
	}
}

func (t *MultiStreamTransport) acceptStream(pending *pendingMultiStream) {
	raw := pending.raw
	defer func() {
		t.mu.Lock()
		delete(t.pending, pending)
		t.mu.Unlock()
		t.prefaceWG.Done()
		<-t.prefaces
	}()
	guard := t.guardConfig()
	timeout := guard.LengthTimeout
	if timeout <= 0 {
		timeout = DefaultSlowlorisConfig().LengthTimeout
	}
	if !guard.DisableDeadlines {
		if err := raw.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			_ = raw.Close()
			t.reportErr(err)
			return
		}
	}
	channelID, err := readStreamPreface(raw)
	if !guard.DisableDeadlines {
		_ = raw.SetReadDeadline(time.Time{})
	}
	if err != nil {
		_ = raw.Close()
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			err = fmt.Errorf("%w: stream preface timeout", ErrSlowloris)
		}
		t.reportErr(err)
		return
	}
	if !validChannel(channelID) {
		_ = raw.Close()
		t.reportErr(fmt.Errorf("%w: invalid multistream channel preface: %d", ErrInvalidFrame, channelID))
		return
	}
	stream := NewStreamTransportForStream(raw, guard)
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = stream.Close()
		return
	}
	existing := t.incoming[channelID]
	if existing == nil {
		t.incoming[channelID] = stream
	}
	t.mu.Unlock()
	if existing != nil {
		_ = stream.Close()
		t.reportErr(fmt.Errorf("%w: duplicate incoming stream for channel %d", ErrInvalidFrame, channelID))
		return
	}
	t.startReadLoop(channelID, stream)
}

func (t *MultiStreamTransport) guardConfig() SlowlorisConfig {
	t.mu.Lock()
	guard := t.guard
	t.mu.Unlock()
	return guard
}

func (t *MultiStreamTransport) startReadLoop(channelID uint16, stream *StreamTransport) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = stream.Close()
		return
	}
	t.readWG.Add(1)
	t.mu.Unlock()
	go func() {
		defer t.readWG.Done()
		t.readLoop(channelID, stream)
	}()
}

func (t *MultiStreamTransport) readLoop(channelID uint16, stream *StreamTransport) {
	defer t.removeStream(channelID, stream)
	for {
		lease, err := stream.ReadFrameLease()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.reportErr(err)
			}
			return
		}
		header, err := DecodeHeader(lease.Data)
		if err != nil {
			lease.Release()
			t.reportErr(fmt.Errorf("%w: %v", ErrInvalidFrame, err))
			return
		}
		if header.ChannelID != channelID {
			lease.Release()
			t.reportErr(fmt.Errorf("%w: frame channel %d does not match stream preface %d", ErrInvalidFrame, header.ChannelID, channelID))
			return
		}
		select {
		case t.reads <- lease:
		case <-t.ctx.Done():
			lease.Release()
			return
		}
	}
}

func (t *MultiStreamTransport) removeStream(channelID uint16, stream *StreamTransport) {
	t.mu.Lock()
	if t.incoming[channelID] == stream {
		delete(t.incoming, channelID)
	}
	if t.outgoing[channelID] == stream {
		delete(t.outgoing, channelID)
	}
	t.mu.Unlock()
	_ = stream.Close()
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
	return writeFull(w, preface[:])
}

func validChannel(channelID uint16) bool {
	return channelID <= ChannelBackground
}

func readStreamPreface(r io.Reader) (uint16, error) {
	var preface [multiStreamPrefaceSize]byte
	if _, err := io.ReadFull(r, preface[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(preface[:]), nil
}
