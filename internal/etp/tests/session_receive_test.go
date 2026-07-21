package tests

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	. "github.com/elum-utils/go-etp/internal/etp"
	"sync"
	"testing"
	"time"
)

type memoryTransferWriter struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	closed    bool
	aborted   bool
	failWrite bool
}

func TestSessionValidatesCancelAcknowledgement(t *testing.T) {
	session := NewSession(newRecordingTransport(t))
	valid := NewFrame(FrameCancelAck, SchemaCancel, EncodeCancelAck(50, CancelAckOK))
	valid.Header.TransferID = 50
	if err := session.HandleFrame(context.Background(), valid); err != nil {
		t.Fatalf("valid cancel ack: %v", err)
	}

	mismatched := NewFrame(FrameCancelAck, SchemaCancel, EncodeCancelAck(50, CancelAckCompleted))
	mismatched.Header.TransferID = 51
	if err := session.HandleFrame(context.Background(), mismatched); err == nil {
		t.Fatal("mismatched cancel ack was accepted")
	}

	malformedPayload := EncodeCancelAck(50, CancelAckNotFound)
	malformedPayload[15] = 1
	malformed := NewFrame(FrameCancelAck, SchemaCancel, malformedPayload)
	malformed.Header.TransferID = 50
	if err := session.HandleFrame(context.Background(), malformed); err == nil {
		t.Fatal("cancel ack with non-zero reserved byte was accepted")
	}
}

func (w *memoryTransferWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failWrite {
		return 0, errors.New("write failed")
	}
	return w.buf.Write(p)
}

func (w *memoryTransferWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}

func (w *memoryTransferWriter) Abort() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.aborted = true
	return nil
}

func (w *memoryTransferWriter) snapshot() (string, bool, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String(), w.closed, w.aborted
}

func TestSessionHandleFrameReceivesTransfer(t *testing.T) {
	transport := newRecordingTransport(t)
	var writer *memoryTransferWriter
	config := DefaultSessionConfig(RoleServer)
	config.Receive.TransferHandler = func(ctx context.Context, info IncomingTransferInfo) (IncomingTransferWriter, error) {
		writer = &memoryTransferWriter{}
		if info.TransferID != 99 || info.Meta.Name != "hello.txt" {
			t.Fatalf("incoming info = %+v", info)
		}
		return writer, nil
	}
	session := NewSessionWithConfig(transport, config)
	payload := []byte("hello")
	checksum := sha256.Sum256(payload)

	begin := NewFrame(FrameTransferBegin, SchemaTransferBegin, EncodeTransferBegin(TransferBegin{
		TotalSize:   uint64(len(payload)),
		ChunkSize:   16,
		ChunkCount:  1,
		ContentType: ContentFile,
		Flags:       TransferFlagChecksumSHA256,
		Checksum:    checksum,
		Name:        "hello.txt",
	}))
	begin.Header.TransferID = 99
	if err := session.HandleFrame(context.Background(), begin); err != nil {
		t.Fatalf("handle transfer begin: %v", err)
	}

	data := NewFrame(FrameData, 0, payload)
	data.Header.TransferID = 99
	data.Header.ChunkID = 0
	data.Header.Flags = FlagAckRequest | FlagFirst | FlagLast
	if err := session.HandleFrame(context.Background(), data); err != nil {
		t.Fatalf("handle transfer data: %v", err)
	}

	end := NewFrame(FrameTransferEnd, 0, nil)
	end.Header.TransferID = 99
	if err := session.HandleFrame(context.Background(), end); err != nil {
		t.Fatalf("handle transfer end: %v", err)
	}
	stored, closed, aborted := writer.snapshot()
	if writer == nil || stored != "hello" || !closed || aborted {
		t.Fatalf("writer state buf=%q closed=%t aborted=%t", stored, closed, aborted)
	}
	requireFrameType(t, transport.frames(), FrameAck)
}

func TestSessionHandleFrameNacksMissingDuplicateAndUnknownTransferChunks(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultSessionConfig(RoleServer)
	config.Receive.TransferHandler = func(ctx context.Context, info IncomingTransferInfo) (IncomingTransferWriter, error) {
		return &memoryTransferWriter{}, nil
	}
	session := NewSessionWithConfig(transport, config)
	begin := NewFrame(FrameTransferBegin, SchemaTransferBegin, EncodeTransferBegin(TransferBegin{
		TotalSize:  10,
		ChunkSize:  8,
		ChunkCount: 2,
		Name:       "x.bin",
	}))
	begin.Header.TransferID = 10
	if err := session.HandleFrame(context.Background(), begin); err != nil {
		t.Fatalf("begin: %v", err)
	}

	missing := NewFrame(FrameData, 0, []byte("late"))
	missing.Header.TransferID = 10
	missing.Header.ChunkID = 1
	if err := session.HandleFrame(context.Background(), missing); err != nil {
		t.Fatalf("missing chunk handling: %v", err)
	}
	nack := requireFrameType(t, transport.frames(), FrameNack)
	decodedNack, err := DecodeNack(nack.Payload)
	if err != nil {
		t.Fatalf("decode nack: %v", err)
	}
	if decodedNack.ReasonCode != NackMissingChunk {
		t.Fatalf("nack = %+v", decodedNack)
	}

	first := NewFrame(FrameData, 0, []byte("hello123"))
	first.Header.TransferID = 10
	first.Header.ChunkID = 0
	first.Header.Flags = FlagAckRequest
	if err := session.HandleFrame(context.Background(), first); err != nil {
		t.Fatalf("first chunk: %v", err)
	}
	if err := session.HandleFrame(context.Background(), first); err != nil {
		t.Fatalf("duplicate chunk: %v", err)
	}
	requireFrameType(t, transport.frames(), FrameAck)

	unknown := NewFrame(FrameData, 0, []byte("x"))
	unknown.Header.TransferID = 404
	unknown.Header.ChunkID = 0
	if err := session.HandleFrame(context.Background(), unknown); err != nil {
		t.Fatalf("unknown chunk handling: %v", err)
	}
	frames := transport.frames()
	lastNack := frames[len(frames)-1]
	decodedNack, err = DecodeNack(lastNack.Payload)
	if err != nil {
		t.Fatalf("decode unknown nack: %v", err)
	}
	if lastNack.Header.FrameType != FrameNack || decodedNack.ReasonCode != NackTransferUnknown {
		t.Fatalf("last frame=%+v nack=%+v", lastNack.Header, decodedNack)
	}
}

func TestSessionHandleFrameAbortsIncomingTransferOnChecksumMismatch(t *testing.T) {
	transport := newRecordingTransport(t)
	var writer *memoryTransferWriter
	config := DefaultSessionConfig(RoleServer)
	config.Receive.TransferHandler = func(ctx context.Context, info IncomingTransferInfo) (IncomingTransferWriter, error) {
		writer = &memoryTransferWriter{}
		return writer, nil
	}
	session := NewSessionWithConfig(transport, config)
	badChecksum := sha256.Sum256([]byte("not-the-payload"))
	begin := NewFrame(FrameTransferBegin, SchemaTransferBegin, EncodeTransferBegin(TransferBegin{
		TotalSize:  5,
		ChunkSize:  16,
		ChunkCount: 1,
		Flags:      TransferFlagChecksumSHA256,
		Checksum:   badChecksum,
		Name:       "bad.bin",
	}))
	begin.Header.TransferID = 77
	if err := session.HandleFrame(context.Background(), begin); err != nil {
		t.Fatalf("begin: %v", err)
	}
	data := NewFrame(FrameData, 0, []byte("hello"))
	data.Header.TransferID = 77
	data.Header.ChunkID = 0
	if err := session.HandleFrame(context.Background(), data); err != nil {
		t.Fatalf("data: %v", err)
	}
	end := NewFrame(FrameTransferEnd, 0, nil)
	end.Header.TransferID = 77
	if err := session.HandleFrame(context.Background(), end); err != nil {
		t.Fatalf("end mismatch sends nack but should not fail transport: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	_, _, aborted := writer.snapshot()
	for writer != nil && !aborted && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
		_, _, aborted = writer.snapshot()
	}
	if writer == nil || !aborted {
		t.Fatalf("writer was not aborted")
	}
	nack := requireFrameType(t, transport.frames(), FrameNack)
	decoded, err := DecodeNack(nack.Payload)
	if err != nil {
		t.Fatalf("decode nack: %v", err)
	}
	if decoded.ReasonCode != NackInvalidChunk {
		t.Fatalf("nack = %+v", decoded)
	}
}

func TestSessionHandleFrameCancelsIncomingTransfer(t *testing.T) {
	transport := newRecordingTransport(t)
	var writer *memoryTransferWriter
	config := DefaultSessionConfig(RoleServer)
	config.Receive.TransferHandler = func(ctx context.Context, info IncomingTransferInfo) (IncomingTransferWriter, error) {
		writer = &memoryTransferWriter{}
		return writer, nil
	}
	session := NewSessionWithConfig(transport, config)
	begin := NewFrame(FrameTransferBegin, SchemaTransferBegin, EncodeTransferBegin(TransferBegin{
		TotalSize:  10,
		ChunkSize:  8,
		ChunkCount: 2,
		Name:       "cancel.bin",
	}))
	begin.Header.TransferID = 50
	if err := session.HandleFrame(context.Background(), begin); err != nil {
		t.Fatalf("begin: %v", err)
	}
	cancel := NewFrame(FrameCancel, SchemaCancel, EncodeCancel(Cancel{TransferID: 50, ReasonCode: CancelUser, Flags: CancelDeletePartial}))
	cancel.Header.TransferID = 50
	if err := session.HandleFrame(context.Background(), cancel); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	_, _, aborted := writer.snapshot()
	for writer != nil && !aborted && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
		_, _, aborted = writer.snapshot()
	}
	if writer == nil || !aborted {
		t.Fatalf("writer was not aborted")
	}
	ack := requireFrameType(t, transport.frames(), FrameCancelAck)
	transferID, status, err := DecodeCancelAck(ack.Payload)
	if err != nil {
		t.Fatalf("decode cancel ack: %v", err)
	}
	if transferID != 50 || status != CancelAckOK {
		t.Fatalf("cancel ack transfer=%d status=%d", transferID, status)
	}
}
