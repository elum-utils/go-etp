package tests

import (
	"bytes"
	"context"
	. "github.com/elum-utils/go-etp"
	"io"
	"sync"
	"testing"
	"time"
)

func TestMultiStreamTransportRoutesChannelsToDifferentStreams(t *testing.T) {
	opener := &memoryStreamOpener{}
	transport := NewMultiStreamTransport(MultiStreamTransportConfig{
		Context: context.Background(),
		Guard:   DefaultSlowlorisConfig(),
		OpenStream: func(context.Context) (DeadlineStream, error) {
			return opener.open(), nil
		},
	})
	defer transport.Close()

	control := NewFrame(FramePing, 0, nil)
	control.Header.ChannelID = ChannelControl
	control.Header.Priority = PriorityCritical
	bulk := NewFrame(FrameData, 0, []byte("bulk"))
	bulk.Header.ChannelID = ChannelBulk
	bulk.Header.Priority = PriorityLow

	if err := transport.SendFrameOnChannel(control.Header.ChannelID, mustEncodeFrame(t, control)); err != nil {
		t.Fatalf("send control: %v", err)
	}
	if err := transport.SendFrameOnChannel(bulk.Header.ChannelID, mustEncodeFrame(t, bulk)); err != nil {
		t.Fatalf("send bulk: %v", err)
	}

	streams := opener.streams()
	if len(streams) != 2 {
		t.Fatalf("streams = %d, want 2", len(streams))
	}
	if got := streamChannel(t, streams[0]); got != ChannelControl {
		t.Fatalf("first stream channel = %d", got)
	}
	if got := streamChannel(t, streams[1]); got != ChannelBulk {
		t.Fatalf("second stream channel = %d", got)
	}
}

func mustEncodeFrame(t *testing.T, frame Frame) []byte {
	t.Helper()
	data, err := EncodeFrame(frame)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	return data
}

func streamChannel(t *testing.T, stream *memoryDeadlineStream) uint16 {
	t.Helper()
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.buf.Bytes()) < 2 {
		t.Fatalf("stream preface is missing")
	}
	return uint16(stream.buf.Bytes()[0])<<8 | uint16(stream.buf.Bytes()[1])
}

type memoryStreamOpener struct {
	mu     sync.Mutex
	opened []*memoryDeadlineStream
}

func (o *memoryStreamOpener) open() *memoryDeadlineStream {
	stream := &memoryDeadlineStream{}
	o.mu.Lock()
	o.opened = append(o.opened, stream)
	o.mu.Unlock()
	return stream
}

func (o *memoryStreamOpener) streams() []*memoryDeadlineStream {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]*memoryDeadlineStream, len(o.opened))
	copy(out, o.opened)
	return out
}

type memoryDeadlineStream struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *memoryDeadlineStream) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (s *memoryDeadlineStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *memoryDeadlineStream) Close() error { return nil }

func (s *memoryDeadlineStream) SetReadDeadline(time.Time) error  { return nil }
func (s *memoryDeadlineStream) SetWriteDeadline(time.Time) error { return nil }
