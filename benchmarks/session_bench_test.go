package benchmarks

import (
	"strings"
	"sync"
	"testing"

	. "github.com/elum-utils/go-etp"
)

type benchTransport struct{}

func (benchTransport) SendFrame([]byte) error { return nil }
func (benchTransport) ReadFrame() ([]byte, error) {
	return nil, nil
}
func (benchTransport) Close() error { return nil }

type benchAckTransport struct {
	mu      sync.Mutex
	session *Session
}

func (t *benchAckTransport) SendFrame(data []byte) error {
	frame, err := DecodeFrame(data)
	if err != nil {
		return err
	}
	if frame.Header.FrameType == FrameData && frame.Header.TransferID != 0 {
		ack := NewFrame(FrameAck, SchemaAck, EncodeAck(Ack{
			TransferID:    frame.Header.TransferID,
			ChunkFrom:     frame.Header.ChunkID,
			ChunkTo:       frame.Header.ChunkID,
			ReceivedBytes: uint64(len(frame.Payload)),
		}))
		t.mu.Lock()
		session := t.session
		t.mu.Unlock()
		if session != nil {
			return session.HandleAck(ack)
		}
	}
	return nil
}

func (t *benchAckTransport) ReadFrame() ([]byte, error) { return nil, nil }
func (t *benchAckTransport) Close() error               { return nil }

func BenchmarkSessionSendText(b *testing.B) {
	session := NewSession(benchTransport{})

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := session.SendText("hello"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSessionTransferStartupAcked(b *testing.B) {
	transport := &benchAckTransport{}
	session := NewSessionWithConfig(transport, SessionConfig{
		FlowControl: FlowControlConfig{
			MaxConcurrentTransfers: 1,
			MaxInFlightChunks:      64,
			MaxInFlightBytes:       1 << 20,
			MaxSendBufferBytes:     1 << 20,
			MaxReceiveBufferBytes:  1 << 20,
			AckTimeout:             1,
			RetryLimit:             1,
			MaxTransferBytes:       1 << 20,
			MaxChunkSize:           16 * 1024,
		},
	})
	transport.session = session

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := session.SendTransfer("small.txt", ContentFile, strings.NewReader("hello"), 5); err != nil {
			b.Fatal(err)
		}
	}
}
