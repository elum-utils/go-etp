package tests

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/elum-utils/go-etp/internal/etp"
)

type controlledTransferWriter struct {
	mu       sync.Mutex
	buf      bytes.Buffer
	entered  chan struct{}
	release  chan struct{}
	closeErr error
	writes   int
	closes   int
	closed   bool
	aborted  bool
}

type panicTransferWriter struct{}

func (panicTransferWriter) Write([]byte) (int, error) { panic("storage write failed") }
func (panicTransferWriter) Close() error              { return nil }
func (panicTransferWriter) Abort() error              { return nil }

type panicTransferReader struct{}

func (panicTransferReader) Read([]byte) (int, error) { panic("source read failed") }

type blockingCloseWriter struct {
	closeEntered chan struct{}
	releaseClose chan struct{}
	aborted      chan struct{}
	closeOnce    sync.Once
	abortOnce    sync.Once
}

func (*blockingCloseWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *blockingCloseWriter) Close() error {
	w.closeOnce.Do(func() { close(w.closeEntered) })
	<-w.releaseClose
	return nil
}
func (w *blockingCloseWriter) Abort() error {
	w.abortOnce.Do(func() { close(w.aborted) })
	return nil
}

func (w *controlledTransferWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.writes++
	w.mu.Unlock()
	select {
	case w.entered <- struct{}{}:
	default:
	}
	if w.release != nil {
		<-w.release
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.aborted {
		return 0, errors.New("write after abort")
	}
	return w.buf.Write(p)
}

func (w *controlledTransferWriter) Close() error {
	w.mu.Lock()
	w.closes++
	w.closed = true
	err := w.closeErr
	w.mu.Unlock()
	return err
}

func (w *controlledTransferWriter) closeCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closes
}

func (w *controlledTransferWriter) Abort() error {
	w.mu.Lock()
	w.aborted = true
	w.mu.Unlock()
	return nil
}

func (w *controlledTransferWriter) snapshot() (string, int, bool, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String(), w.writes, w.closed, w.aborted
}

func beginTransferFrame(id uint64, total uint64, chunk uint32) Frame {
	frame := NewFrame(FrameTransferBegin, SchemaTransferBegin, EncodeTransferBegin(TransferBegin{
		TotalSize:  total,
		ChunkSize:  chunk,
		ChunkCount: uint32((total + uint64(chunk) - 1) / uint64(chunk)),
		Name:       "payload.bin",
	}))
	frame.Header.TransferID = id
	return frame
}

func transferDataFrame(id uint64, chunkID uint32, payload string) Frame {
	frame := NewFrame(FrameData, 0, []byte(payload))
	frame.Header.TransferID = id
	frame.Header.ChunkID = chunkID
	frame.Header.Flags = FlagAckRequest
	return frame
}

func TestTransferBackpressureDoesNotAdvanceRejectedChunk(t *testing.T) {
	transport := newRecordingTransport(t)
	writer := &controlledTransferWriter{entered: make(chan struct{}, 1), release: make(chan struct{})}
	config := DefaultServerConfig()
	config.FlowControl.MaxInFlightChunks = 1
	config.FlowControl.MaxInFlightBytes = 4
	config.FlowControl.MaxReceiveBufferBytes = 4
	config.FlowControl.MaxChunkSize = 4
	config.Receive.TransferHandler = func(context.Context, IncomingTransferInfo) (IncomingTransferWriter, error) { return writer, nil }
	session := NewSessionWithConfig(transport, config)

	if err := session.HandleFrame(t.Context(), beginTransferFrame(10, 8, 4)); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := session.HandleFrame(t.Context(), transferDataFrame(10, 0, "aaaa")); err != nil {
		t.Fatalf("first chunk: %v", err)
	}
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not start")
	}
	if err := session.HandleFrame(t.Context(), transferDataFrame(10, 1, "bbbb")); err != nil {
		t.Fatalf("flow-control nack: %v", err)
	}
	nackFrame := requireFrameType(t, transport.frames(), FrameNack)
	nack, err := DecodeNack(nackFrame.Payload)
	if err != nil || nack.ReasonCode != NackFlowControl {
		t.Fatalf("flow-control nack = %+v err=%v", nack, err)
	}
	close(writer.release)
	waitForFrameCount(t, transport, 3)
	if err := session.HandleFrame(t.Context(), transferDataFrame(10, 1, "bbbb")); err != nil {
		t.Fatalf("retried chunk: %v", err)
	}
	end := NewFrame(FrameTransferEnd, 0, nil)
	end.Header.TransferID = 10
	if err := session.HandleFrame(t.Context(), end); err != nil {
		t.Fatalf("end: %v", err)
	}
	got, writes, closed, aborted := writer.snapshot()
	if got != "aaaabbbb" || writes != 2 || !closed || aborted {
		t.Fatalf("writer state data=%q writes=%d closed=%t aborted=%t", got, writes, closed, aborted)
	}
}

func TestGlobalReceiveBufferReleaseReopensBlockedTransferWindow(t *testing.T) {
	transport := newRecordingTransport(t)
	writerA := &controlledTransferWriter{entered: make(chan struct{}, 1), release: make(chan struct{})}
	config := DefaultServerConfig()
	config.FlowControl.MaxInFlightChunks = 1
	config.FlowControl.MaxInFlightBytes = 4
	config.FlowControl.MaxReceiveBufferBytes = 4
	config.FlowControl.MaxChunkSize = 4
	config.Receive.TransferHandler = func(_ context.Context, info IncomingTransferInfo) (IncomingTransferWriter, error) {
		if info.TransferID == 1 {
			return writerA, nil
		}
		return &memoryTransferWriter{}, nil
	}
	session := NewSessionWithConfig(transport, config)
	if err := session.HandleFrame(t.Context(), beginTransferFrame(1, 4, 4)); err != nil {
		t.Fatalf("begin A: %v", err)
	}
	if err := session.HandleFrame(t.Context(), beginTransferFrame(2, 4, 4)); err != nil {
		t.Fatalf("begin B: %v", err)
	}
	if err := session.HandleFrame(t.Context(), transferDataFrame(1, 0, "aaaa")); err != nil {
		t.Fatalf("data A: %v", err)
	}
	<-writerA.entered
	if err := session.HandleFrame(t.Context(), transferDataFrame(2, 0, "bbbb")); err != nil {
		t.Fatalf("flow-control B: %v", err)
	}
	requireFrameType(t, transport.frames(), FrameNack)
	beforeRelease := len(transport.frames())
	close(writerA.release)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		frames := transport.frames()
		if len(frames) < beforeRelease {
			continue
		}
		for _, frame := range frames[beforeRelease:] {
			if frame.Header.FrameType != FrameWindow || frame.Header.TransferID != 2 {
				continue
			}
			window, err := DecodeWindow(frame.Payload)
			if err == nil && window.WindowBytes >= 4 && window.WindowChunks > 0 {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("blocked transfer did not receive a reopened window")
}

func TestTransferAckMeansWriterCommittedBytes(t *testing.T) {
	transport := newRecordingTransport(t)
	writer := &controlledTransferWriter{entered: make(chan struct{}, 1), release: make(chan struct{})}
	config := DefaultServerConfig()
	config.Receive.TransferHandler = func(context.Context, IncomingTransferInfo) (IncomingTransferWriter, error) { return writer, nil }
	session := NewSessionWithConfig(transport, config)
	if err := session.HandleFrame(t.Context(), beginTransferFrame(11, 4, 4)); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := session.HandleFrame(t.Context(), transferDataFrame(11, 0, "data")); err != nil {
		t.Fatalf("data: %v", err)
	}
	<-writer.entered
	if frameCount(transport.frames(), FrameAck) != 0 {
		t.Fatal("chunk was acknowledged before writer.Write completed")
	}
	close(writer.release)
	deadline := time.Now().Add(time.Second)
	for frameCount(transport.frames(), FrameAck) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if frameCount(transport.frames(), FrameAck) != 1 {
		t.Fatal("committed chunk was not acknowledged")
	}
}

func TestTransferWriterPanicFailsOnlyTransfer(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultServerConfig()
	config.Receive.TransferHandler = func(context.Context, IncomingTransferInfo) (IncomingTransferWriter, error) {
		return panicTransferWriter{}, nil
	}
	session := NewSessionWithConfig(transport, config)
	if err := session.HandleFrame(t.Context(), beginTransferFrame(115, 4, 4)); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := session.HandleFrame(t.Context(), transferDataFrame(115, 0, "data")); err != nil {
		t.Fatalf("data enqueue: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for frameCount(transport.frames(), FrameNack) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	end := NewFrame(FrameTransferEnd, 0, nil)
	end.Header.TransferID = 115
	if err := session.HandleFrame(t.Context(), end); err != nil {
		t.Fatalf("end after writer panic: %v", err)
	}
	stateFrame := requireFrameType(t, transport.frames(), FrameTransferState)
	state, err := DecodeTransferStateMessage(stateFrame.Payload)
	if err != nil || state.Flags != TransferStateFlagFailed || state.ReasonCode != NackWriteFailed {
		t.Fatalf("writer panic state = %+v err=%v", state, err)
	}
	if session.State() != SessionEstablished {
		t.Fatalf("session state = %s", session.State())
	}
}

func TestTransferReaderPanicReturnsHandleErrorAndReleasesSlot(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultClientConfig()
	config.FlowControl.MaxConcurrentTransfers = 1
	session := NewSessionWithConfig(transport, config)
	installTransferAcker(t, transport, session)
	handle := session.StartTransfer(t.Context(), TransferOptions{Reader: panicTransferReader{}, TotalSize: 1})
	select {
	case err := <-handle.Done():
		if err == nil || !strings.Contains(err.Error(), "panic") {
			t.Fatalf("reader panic error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader panic left transfer blocked")
	}
	next := session.StartTransfer(t.Context(), TransferOptions{Reader: bytes.NewReader([]byte("x")), TotalSize: 1})
	if err := <-next.Done(); err != nil {
		t.Fatalf("slot was not released after reader panic: %v", err)
	}
}

func TestSequentialTransfersReleaseSlotBeforeDone(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultClientConfig()
	config.FlowControl.MaxConcurrentTransfers = 1
	session := NewSessionWithConfig(transport, config)
	installTransferAcker(t, transport, session)
	for i := 0; i < 100; i++ {
		handle := session.StartTransfer(t.Context(), TransferOptions{
			Reader:    bytes.NewReader([]byte("x")),
			TotalSize: 1,
			ChunkSize: 1,
		})
		if err := <-handle.Done(); err != nil {
			t.Fatalf("sequential transfer %d: %v", i, err)
		}
	}
}

func TestTransferEndStopsWaitingWhenWriteContextIsCanceled(t *testing.T) {
	transport := newRecordingTransport(t)
	writer := &controlledTransferWriter{entered: make(chan struct{}, 1), release: make(chan struct{})}
	config := DefaultServerConfig()
	config.Receive.TransferHandler = func(context.Context, IncomingTransferInfo) (IncomingTransferWriter, error) { return writer, nil }
	session := NewSessionWithConfig(transport, config)
	if err := session.HandleFrame(t.Context(), beginTransferFrame(116, 4, 4)); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := session.HandleFrame(t.Context(), transferDataFrame(116, 0, "data")); err != nil {
		t.Fatalf("data: %v", err)
	}
	<-writer.entered

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		end := NewFrame(FrameTransferEnd, 0, nil)
		end.Header.TransferID = 116
		result <- session.HandleFrame(ctx, end)
	}()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("transfer end cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("transfer end remained blocked after context cancellation")
	}
	close(writer.release)
}

func TestTransferCommitTimeoutBoundsStuckWriterClose(t *testing.T) {
	transport := newRecordingTransport(t)
	writer := &blockingCloseWriter{
		closeEntered: make(chan struct{}),
		releaseClose: make(chan struct{}),
		aborted:      make(chan struct{}),
	}
	config := DefaultServerConfig()
	config.FlowControl.MaxConcurrentTransfers = 1
	config.FlowControl.TransferCommitTimeout = 10 * time.Millisecond
	config.Receive.TransferHandler = func(context.Context, IncomingTransferInfo) (IncomingTransferWriter, error) { return writer, nil }
	session := NewSessionWithConfig(transport, config)
	if err := session.HandleFrame(t.Context(), beginTransferFrame(117, 4, 4)); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := session.HandleFrame(t.Context(), transferDataFrame(117, 0, "data")); err != nil {
		t.Fatalf("data: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for frameCount(transport.frames(), FrameAck) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	end := NewFrame(FrameTransferEnd, 0, nil)
	end.Header.TransferID = 117
	started := time.Now()
	if err := session.HandleFrame(t.Context(), end); err != nil {
		t.Fatalf("commit timeout signaling: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("commit timeout took %s", elapsed)
	}
	<-writer.closeEntered
	stateFrame := requireFrameType(t, transport.frames(), FrameTransferState)
	state, err := DecodeTransferStateMessage(stateFrame.Payload)
	if err != nil || state.Flags != TransferStateFlagFailed || state.ReasonCode != NackWriteFailed {
		t.Fatalf("commit timeout state = %+v err=%v", state, err)
	}
	if err := session.HandleFrame(t.Context(), beginTransferFrame(118, 1, 1)); err == nil {
		t.Fatal("stuck close was not counted against concurrent transfer limit")
	}

	close(writer.releaseClose)
	select {
	case <-writer.aborted:
	case <-time.After(time.Second):
		t.Fatal("late close result was not aborted")
	}
	deadline = time.Now().Add(time.Second)
	for {
		if err := session.HandleFrame(t.Context(), beginTransferFrame(119, 1, 1)); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("commit slot was not released after close returned")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestTransferCompletionWaitsForRemoteCommit(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSession(transport)
	endSeen := make(chan uint64, 1)
	transport.onSend = func(frame Frame) {
		switch frame.Header.FrameType {
		case FrameData:
			ack := NewFrame(FrameAck, SchemaAck, EncodeAck(Ack{TransferID: frame.Header.TransferID, ChunkFrom: frame.Header.ChunkID, ChunkTo: frame.Header.ChunkID, ReceivedBytes: uint64(len(frame.Payload))}))
			ack.Header.TransferID = frame.Header.TransferID
			if err := session.HandleAck(ack); err != nil {
				t.Errorf("ack: %v", err)
			}
		case FrameTransferEnd:
			endSeen <- frame.Header.TransferID
		}
	}
	handle := session.StartTransfer(t.Context(), TransferOptions{Reader: bytes.NewReader([]byte("data")), TotalSize: 4})
	id := <-endSeen
	select {
	case err := <-handle.Done():
		t.Fatalf("transfer completed before remote commit: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	completeOutgoingTransfer(t, session, id, 4, 1)
	if err := <-handle.Done(); err != nil {
		t.Fatalf("transfer completion: %v", err)
	}
}

func TestEmptyTransferCompletesWithoutDataFrame(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSession(transport)
	installTransferAcker(t, transport, session)
	handle := session.StartTransfer(t.Context(), TransferOptions{Reader: bytes.NewReader(nil), TotalSize: 0})
	select {
	case err := <-handle.Done():
		if err != nil {
			t.Fatalf("empty transfer: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("empty transfer did not complete")
	}
	frames := transport.frames()
	if frameCount(frames, FrameTransferBegin) != 1 || frameCount(frames, FrameTransferEnd) != 1 || frameCount(frames, FrameData) != 0 {
		t.Fatalf("empty transfer frames: begin=%d data=%d end=%d", frameCount(frames, FrameTransferBegin), frameCount(frames, FrameData), frameCount(frames, FrameTransferEnd))
	}
}

func TestConcurrentTransferEndFinalizesWriterOnce(t *testing.T) {
	transport := newRecordingTransport(t)
	writer := &controlledTransferWriter{entered: make(chan struct{}, 1)}
	config := DefaultServerConfig()
	config.Receive.TransferHandler = func(context.Context, IncomingTransferInfo) (IncomingTransferWriter, error) { return writer, nil }
	session := NewSessionWithConfig(transport, config)
	if err := session.HandleFrame(t.Context(), beginTransferFrame(120, 4, 4)); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := session.HandleFrame(t.Context(), transferDataFrame(120, 0, "data")); err != nil {
		t.Fatalf("data: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for frameCount(transport.frames(), FrameAck) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	const callers = 16
	results := make(chan error, callers)
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		go func() {
			<-start
			end := NewFrame(FrameTransferEnd, 0, nil)
			end.Header.TransferID = 120
			results <- session.HandleFrame(t.Context(), end)
		}()
	}
	close(start)
	for i := 0; i < callers; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("concurrent end: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent transfer end deadlocked")
		}
	}
	if got := writer.closeCount(); got != 1 {
		t.Fatalf("writer close calls = %d, want 1", got)
	}
}

func TestIncomingTransferIDsCannotBeReusedAfterCancel(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultServerConfig()
	config.Receive.TransferHandler = func(context.Context, IncomingTransferInfo) (IncomingTransferWriter, error) {
		return &memoryTransferWriter{}, nil
	}
	session := NewSessionWithConfig(transport, config)
	if err := session.HandleFrame(t.Context(), beginTransferFrame(200, 1, 1)); err != nil {
		t.Fatalf("begin: %v", err)
	}
	cancel := NewFrame(FrameCancel, SchemaCancel, EncodeCancel(Cancel{TransferID: 200, ReasonCode: CancelUser}))
	cancel.Header.TransferID = 200
	if err := session.HandleFrame(t.Context(), cancel); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := session.HandleFrame(t.Context(), beginTransferFrame(200, 1, 1)); err == nil {
		t.Fatal("canceled transfer id was reused")
	}
	if err := session.HandleFrame(t.Context(), beginTransferFrame(199, 1, 1)); err == nil {
		t.Fatal("decreasing transfer id was accepted")
	}
	if err := session.HandleFrame(t.Context(), beginTransferFrame(201, 1, 1)); err != nil {
		t.Fatalf("next increasing transfer id: %v", err)
	}
}

func TestExpiredTerminalTransferDoesNotEvictNewState(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultServerConfig()
	config.FlowControl.MaxCompletedTransfers = 1
	config.FlowControl.CompletedTransferTTL = 10 * time.Millisecond
	config.Receive.TransferHandler = func(context.Context, IncomingTransferInfo) (IncomingTransferWriter, error) {
		return &memoryTransferWriter{}, nil
	}
	session := NewSessionWithConfig(transport, config)
	completeIncomingTransfer(t, session, 300)
	time.Sleep(20 * time.Millisecond)
	oldEnd := NewFrame(FrameTransferEnd, 0, nil)
	oldEnd.Header.TransferID = 300
	if err := session.HandleFrame(t.Context(), oldEnd); err != nil {
		t.Fatalf("expired terminal replay: %v", err)
	}

	completeIncomingTransfer(t, session, 301)
	before := len(transport.frames())
	newEnd := NewFrame(FrameTransferEnd, 0, nil)
	newEnd.Header.TransferID = 301
	if err := session.HandleFrame(t.Context(), newEnd); err != nil {
		t.Fatalf("new terminal replay: %v", err)
	}
	frames := waitForFrameCount(t, transport, before+1)
	if frames[len(frames)-1].Header.FrameType != FrameTransferState {
		t.Fatalf("terminal replay frame type = %d", frames[len(frames)-1].Header.FrameType)
	}
}

func completeIncomingTransfer(t *testing.T, session *Session, transferID uint64) {
	t.Helper()
	if err := session.HandleFrame(t.Context(), beginTransferFrame(transferID, 1, 1)); err != nil {
		t.Fatalf("begin transfer %d: %v", transferID, err)
	}
	if err := session.HandleFrame(t.Context(), transferDataFrame(transferID, 0, "x")); err != nil {
		t.Fatalf("data transfer %d: %v", transferID, err)
	}
	end := NewFrame(FrameTransferEnd, 0, nil)
	end.Header.TransferID = transferID
	if err := session.HandleFrame(t.Context(), end); err != nil {
		t.Fatalf("end transfer %d: %v", transferID, err)
	}
}

func TestTransferCloseFailureReturnsFailedCommit(t *testing.T) {
	transport := newRecordingTransport(t)
	writer := &controlledTransferWriter{entered: make(chan struct{}, 1), closeErr: errors.New("storage commit failed")}
	config := DefaultServerConfig()
	config.Receive.TransferHandler = func(context.Context, IncomingTransferInfo) (IncomingTransferWriter, error) { return writer, nil }
	session := NewSessionWithConfig(transport, config)
	if err := session.HandleFrame(t.Context(), beginTransferFrame(12, 4, 4)); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := session.HandleFrame(t.Context(), transferDataFrame(12, 0, "data")); err != nil {
		t.Fatalf("data: %v", err)
	}
	end := NewFrame(FrameTransferEnd, 0, nil)
	end.Header.TransferID = 12
	if err := session.HandleFrame(t.Context(), end); err != nil {
		t.Fatalf("failed end signaling: %v", err)
	}
	stateFrame := requireFrameType(t, transport.frames(), FrameTransferState)
	state, err := DecodeTransferStateMessage(stateFrame.Payload)
	if err != nil || state.Flags != TransferStateFlagFailed || state.ReasonCode != NackWriteFailed {
		t.Fatalf("terminal state = %+v err=%v", state, err)
	}
}

func TestTransferCancelNeverAbortsConcurrentlyWithWrite(t *testing.T) {
	transport := newRecordingTransport(t)
	writer := &controlledTransferWriter{entered: make(chan struct{}, 1), release: make(chan struct{})}
	config := DefaultServerConfig()
	config.Receive.TransferHandler = func(context.Context, IncomingTransferInfo) (IncomingTransferWriter, error) { return writer, nil }
	session := NewSessionWithConfig(transport, config)
	if err := session.HandleFrame(t.Context(), beginTransferFrame(13, 4, 4)); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := session.HandleFrame(t.Context(), transferDataFrame(13, 0, "data")); err != nil {
		t.Fatalf("data: %v", err)
	}
	<-writer.entered
	cancel := NewFrame(FrameCancel, SchemaCancel, EncodeCancel(Cancel{TransferID: 13, ReasonCode: CancelUser}))
	cancel.Header.TransferID = 13
	if err := session.HandleFrame(t.Context(), cancel); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	_, _, _, aborted := writer.snapshot()
	if aborted {
		t.Fatal("writer was aborted while Write was still active")
	}
	close(writer.release)
	deadline := time.Now().Add(time.Second)
	for {
		_, writes, _, aborted := writer.snapshot()
		if aborted {
			if writes != 1 {
				t.Fatalf("writes after cancellation = %d", writes)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("writer was not aborted after active write returned")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestTransferEndIsRetriedUntilRemoteCommitArrives(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultClientConfig()
	config.FlowControl.AckTimeout = 10 * time.Millisecond
	config.FlowControl.RetryLimit = 3
	session := NewSessionWithConfig(transport, config)

	var endCount atomic.Int32
	transport.onSend = func(frame Frame) {
		switch frame.Header.FrameType {
		case FrameData:
			ack := NewFrame(FrameAck, SchemaAck, EncodeAck(Ack{
				TransferID:    frame.Header.TransferID,
				ChunkFrom:     frame.Header.ChunkID,
				ChunkTo:       frame.Header.ChunkID,
				ReceivedBytes: uint64(len(frame.Payload)),
			}))
			ack.Header.TransferID = frame.Header.TransferID
			if err := session.HandleAck(ack); err != nil {
				t.Errorf("ack: %v", err)
			}
		case FrameTransferEnd:
			if endCount.Add(1) == 2 {
				completeOutgoingTransfer(t, session, frame.Header.TransferID, 4, 1)
			}
		}
	}

	handle := session.StartTransfer(t.Context(), TransferOptions{
		Reader:    bytes.NewReader([]byte("data")),
		TotalSize: 4,
		ChunkSize: 4,
	})
	select {
	case err := <-handle.Done():
		if err != nil {
			t.Fatalf("transfer failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("transfer did not retry its final commit request")
	}
	if got := endCount.Load(); got != 2 {
		t.Fatalf("transfer end sends = %d, want 2", got)
	}
}

func TestCompletedTransferAnswersDuplicateEndWithoutClosingAgain(t *testing.T) {
	transport := newRecordingTransport(t)
	writer := &controlledTransferWriter{entered: make(chan struct{}, 1)}
	config := DefaultServerConfig()
	config.Receive.TransferHandler = func(context.Context, IncomingTransferInfo) (IncomingTransferWriter, error) { return writer, nil }
	session := NewSessionWithConfig(transport, config)

	if err := session.HandleFrame(t.Context(), beginTransferFrame(14, 4, 4)); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := session.HandleFrame(t.Context(), transferDataFrame(14, 0, "data")); err != nil {
		t.Fatalf("data: %v", err)
	}
	end := NewFrame(FrameTransferEnd, 0, nil)
	end.Header.TransferID = 14
	if err := session.HandleFrame(t.Context(), end); err != nil {
		t.Fatalf("first end: %v", err)
	}
	if err := session.HandleFrame(t.Context(), end); err != nil {
		t.Fatalf("duplicate end: %v", err)
	}

	var completed int
	for _, frame := range transport.frames() {
		if frame.Header.FrameType != FrameTransferState {
			continue
		}
		state, err := DecodeTransferStateMessage(frame.Payload)
		if err != nil {
			t.Fatalf("decode terminal state: %v", err)
		}
		if state.Flags == TransferStateFlagCompleted {
			completed++
		}
	}
	if completed != 2 {
		t.Fatalf("completed responses = %d, want 2", completed)
	}
}

func TestCanceledTransferIsNotRemembered(t *testing.T) {
	transport := newRecordingTransport(t)
	writer := &controlledTransferWriter{entered: make(chan struct{}, 1)}
	config := DefaultServerConfig()
	config.Receive.TransferHandler = func(context.Context, IncomingTransferInfo) (IncomingTransferWriter, error) { return writer, nil }
	session := NewSessionWithConfig(transport, config)

	if err := session.HandleFrame(t.Context(), beginTransferFrame(15, 4, 4)); err != nil {
		t.Fatalf("begin: %v", err)
	}
	cancel := NewFrame(FrameCancel, SchemaCancel, EncodeCancel(Cancel{TransferID: 15, ReasonCode: CancelUser}))
	cancel.Header.TransferID = 15
	if err := session.HandleFrame(t.Context(), cancel); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	end := NewFrame(FrameTransferEnd, 0, nil)
	end.Header.TransferID = 15
	if err := session.HandleFrame(t.Context(), end); err != nil {
		t.Fatalf("end after cancel: %v", err)
	}

	nackFrame := requireFrameType(t, transport.frames(), FrameNack)
	nack, err := DecodeNack(nackFrame.Payload)
	if err != nil {
		t.Fatalf("decode nack: %v", err)
	}
	if nack.TransferID != 15 || nack.ReasonCode != NackTransferUnknown {
		t.Fatalf("nack after canceled transfer = %+v", nack)
	}
}
