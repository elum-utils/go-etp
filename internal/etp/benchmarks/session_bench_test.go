package benchmarks

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	. "github.com/elum-utils/go-etp/internal/etp"
)

type benchTransport struct{}

func (benchTransport) SendFrame([]byte) error { return nil }
func (benchTransport) ReadFrame() ([]byte, error) {
	return nil, nil
}
func (benchTransport) Close() error                   { return nil }
func (benchTransport) NegotiatedCapabilities() uint64 { return DefaultCapabilities }

type benchAckTransport struct {
	session       *Session
	transferID    uint64
	receivedBytes uint64
	nextChunk     uint32
	ackPayload    [24]byte
	statePayload  [24]byte
}

func (t *benchAckTransport) SendFrame(data []byte) error {
	frame, err := DecodeFrameView(data)
	if err != nil {
		return err
	}
	switch frame.Header.FrameType {
	case FrameTransferBegin:
		t.transferID = frame.Header.TransferID
		t.receivedBytes = 0
		t.nextChunk = 0
	case FrameData:
		if frame.Header.TransferID == 0 {
			return nil
		}
		t.receivedBytes += uint64(len(frame.Payload))
		t.nextChunk = frame.Header.ChunkID + 1
		binary.BigEndian.PutUint64(t.ackPayload[0:8], frame.Header.TransferID)
		binary.BigEndian.PutUint32(t.ackPayload[8:12], frame.Header.ChunkID)
		binary.BigEndian.PutUint32(t.ackPayload[12:16], frame.Header.ChunkID)
		binary.BigEndian.PutUint64(t.ackPayload[16:24], t.receivedBytes)
		ack := NewFrame(FrameAck, SchemaAck, t.ackPayload[:])
		ack.Header.TransferID = frame.Header.TransferID
		return t.session.HandleAck(ack)
	case FrameTransferEnd:
		binary.BigEndian.PutUint64(t.statePayload[0:8], frame.Header.TransferID)
		binary.BigEndian.PutUint64(t.statePayload[8:16], t.receivedBytes)
		binary.BigEndian.PutUint32(t.statePayload[16:20], t.nextChunk)
		binary.BigEndian.PutUint16(t.statePayload[20:22], TransferStateFlagCompleted)
		state := NewFrame(FrameTransferState, SchemaTransferState, t.statePayload[:])
		state.Header.TransferID = frame.Header.TransferID
		return t.session.HandleFrame(nil, state)
	}
	return nil
}

func (*benchAckTransport) ReadFrame() ([]byte, error)     { return nil, nil }
func (*benchAckTransport) Close() error                   { return nil }
func (*benchAckTransport) NegotiatedCapabilities() uint64 { return DefaultCapabilities }

func BenchmarkSessionSendText(b *testing.B) {
	session := NewSession(benchTransport{})

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := session.SendText("hello"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSessionTransferSmallCommitted(b *testing.B) {
	transport := &benchAckTransport{}
	session := NewSessionWithConfig(transport, SessionConfig{
		FlowControl: FlowControlConfig{
			MaxConcurrentTransfers: 1,
			MaxInFlightChunks:      64,
			MaxInFlightBytes:       1 << 20,
			MaxSendBufferBytes:     1 << 20,
			MaxReceiveBufferBytes:  1 << 20,
			AckTimeout:             time.Second,
			RetryLimit:             1,
			MaxTransferBytes:       1 << 20,
			MaxChunkSize:           16 * 1024,
		},
		RateLimit: RateLimitConfig{
			MaxFramesPerSecond:    int(^uint(0) >> 1),
			MaxBytesPerSecond:     ^uint64(0),
			MaxAuthAttempts:       int(^uint(0) >> 1),
			MaxBadFramesPerSecond: int(^uint(0) >> 1),
		},
	})
	transport.session = session
	var reader strings.Reader

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		reader.Reset("hello")
		if _, err := session.SendTransfer("small.txt", ContentFile, &reader, 5); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSessionTransferCommitted32KiB(b *testing.B) {
	transport := &benchAckTransport{}
	session := NewSessionWithConfig(transport, SessionConfig{
		FlowControl: FlowControlConfig{
			MaxConcurrentTransfers: 1,
			MaxInFlightChunks:      64,
			MaxInFlightBytes:       1 << 20,
			MaxSendBufferBytes:     1 << 20,
			MaxReceiveBufferBytes:  1 << 20,
			AckTimeout:             time.Second,
			RetryLimit:             1,
			MaxTransferBytes:       1 << 20,
			MaxChunkSize:           16 * 1024,
		},
		RateLimit: RateLimitConfig{
			MaxFramesPerSecond:    int(^uint(0) >> 1),
			MaxBytesPerSecond:     ^uint64(0),
			MaxAuthAttempts:       int(^uint(0) >> 1),
			MaxBadFramesPerSecond: int(^uint(0) >> 1),
		},
	})
	transport.session = session
	payload := make([]byte, 32*1024)
	reader := bytes.NewReader(payload)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		reader.Reset(payload)
		if _, err := session.SendTransfer("chunked.bin", ContentFile, reader, uint64(len(payload))); err != nil {
			b.Fatal(err)
		}
	}
}
