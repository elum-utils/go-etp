package tests

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	. "github.com/elum-utils/go-etp/internal/etp"
)

func TestSessionResumeTransferContinuesFromReceiverCommittedOffset(t *testing.T) {
	transport := newRecordingTransport(t)
	transport.caps |= CapabilityTransferResume
	config := DefaultClientConfig()
	config.Capabilities |= CapabilityTransferResume
	session := NewSessionWithConfig(transport, config)

	const transferID = 9001
	source := []byte("abcdefgh")
	var mu sync.Mutex
	var dataFrames []Frame
	transport.onSend = func(frame Frame) {
		switch frame.Header.FrameType {
		case FrameTransferResume:
			resume, err := DecodeTransferResumeView(frame.Payload)
			if err != nil {
				t.Errorf("decode resume: %v", err)
				return
			}
			if resume.TransferID != transferID || resume.ReceivedBytes != 0 || resume.NextChunk != 0 || frame.Header.RequestID != 77 {
				t.Errorf("resume request = %+v header=%+v", resume, frame.Header)
			}
			accepted := NewFrame(FrameTransferState, SchemaTransferState, EncodeTransferStateMessage(TransferStateMessage{
				TransferID:    transferID,
				ReceivedBytes: 4,
				NextChunk:     1,
				Flags:         TransferStateFlagResumeAccepted,
			}))
			accepted.Header.TransferID = transferID
			if err := session.HandleFrame(t.Context(), accepted); err != nil {
				t.Errorf("accept resume: %v", err)
			}
		case FrameData:
			mu.Lock()
			dataFrames = append(dataFrames, frame)
			mu.Unlock()
			ack := NewFrame(FrameAck, SchemaAck, EncodeAck(Ack{
				TransferID:    transferID,
				ChunkFrom:     frame.Header.ChunkID,
				ChunkTo:       frame.Header.ChunkID,
				ReceivedBytes: 4 + uint64(len(frame.Payload)),
			}))
			ack.Header.TransferID = transferID
			if err := session.HandleAck(ack); err != nil {
				t.Errorf("ack resumed data: %v", err)
			}
		case FrameTransferEnd:
			completeOutgoingTransfer(t, session, transferID, uint64(len(source)), 2)
		}
	}

	var openedAt uint64
	handle := session.ResumeTransfer(t.Context(), ResumeTransferOptions{
		TransferID:    transferID,
		ReceivedBytes: 0,
		NextChunk:     0,
		Token:         []byte("resume-token"),
		OpenReader: func(offset uint64) (io.Reader, error) {
			openedAt = offset
			return bytes.NewReader(source[offset:]), nil
		},
		TransferOptions: TransferOptions{
			RequestID: 77,
			Event:     "attach.upload",
			TotalSize: uint64(len(source)),
			ChunkSize: 4,
		},
	})
	select {
	case err := <-handle.Done():
		if err != nil {
			t.Fatalf("resume transfer: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("resumed transfer did not complete")
	}
	if openedAt != 4 {
		t.Fatalf("reader opened at %d, want authoritative offset 4", openedAt)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dataFrames) != 1 || dataFrames[0].Header.ChunkID != 1 || !bytes.Equal(dataFrames[0].Payload, []byte("efgh")) {
		t.Fatalf("resumed data frames = %+v", dataFrames)
	}
	if frameCount(transport.frames(), FrameTransferBegin) != 0 {
		t.Fatal("resume sent a new TransferBegin")
	}
}

type resumableDisconnectWriter struct {
	entered   chan struct{}
	suspended chan struct{}
	aborted   chan struct{}
}

type abortTrackingWriter struct {
	aborted bool
}

type resumeTimeoutWriter struct {
	aborted chan struct{}
	once    sync.Once
}

func (*resumeTimeoutWriter) Write(p []byte) (int, error) { return len(p), nil }
func (*resumeTimeoutWriter) Close() error                { return nil }
func (w *resumeTimeoutWriter) Abort() error {
	w.once.Do(func() { close(w.aborted) })
	return nil
}

type blockingResumeStore struct {
	mu          sync.Mutex
	calls       int
	entered     chan struct{}
	release     chan struct{}
	firstWriter IncomingTransferWriter
}

type panicResumeStore struct{}

type blockingTokenResumeStore struct {
	entered  chan struct{}
	release  chan struct{}
	observed chan string
}

func (panicResumeStore) ResumeIncoming(context.Context, TransferResumeView) (TransferResumeDecision, error) {
	panic("resume storage failed")
}

func (s *blockingTokenResumeStore) ResumeIncoming(_ context.Context, resume TransferResumeView) (TransferResumeDecision, error) {
	close(s.entered)
	<-s.release
	s.observed <- string(resume.Token)
	return TransferResumeDecision{}, nil
}

func (s *blockingResumeStore) ResumeIncoming(context.Context, TransferResumeView) (TransferResumeDecision, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call == 1 {
		close(s.entered)
		<-s.release
		return acceptedResumeDecision(s.firstWriter), nil
	}
	return acceptedResumeDecision(&memoryTransferWriter{}), nil
}

func (s *blockingResumeStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func acceptedResumeDecision(writer IncomingTransferWriter) TransferResumeDecision {
	return TransferResumeDecision{
		Accepted: true,
		Meta: TransferBegin{
			TotalSize:  1,
			ChunkSize:  1,
			ChunkCount: 1,
		},
		Writer: writer,
	}
}

func (*abortTrackingWriter) Write(p []byte) (int, error) { return len(p), nil }
func (*abortTrackingWriter) Close() error                { return nil }
func (w *abortTrackingWriter) Abort() error {
	w.aborted = true
	return nil
}

func TestSessionResumeRejectsMisalignedStoredOffset(t *testing.T) {
	transport := newRecordingTransport(t)
	transport.caps |= CapabilityTransferResume
	writer := &abortTrackingWriter{}
	config := DefaultServerConfig()
	config.Capabilities |= CapabilityTransferResume
	config.Resume.Store = &resumeStore{decision: TransferResumeDecision{
		Accepted: true,
		Meta: TransferBegin{
			TotalSize:  8,
			ChunkSize:  4,
			ChunkCount: 2,
		},
		Writer:        writer,
		ReceivedBytes: 3,
		NextChunk:     1,
	}}
	session := NewSessionWithConfig(transport, config)
	frame := NewFrame(FrameTransferResume, SchemaTransferState, EncodeTransferResume(TransferResume{TransferID: 55}))
	frame.Header.TransferID = 55

	if err := session.HandleFrame(t.Context(), frame); err != nil {
		t.Fatalf("handle rejected resume: %v", err)
	}
	stateFrame := requireFrameType(t, transport.frames(), FrameTransferState)
	state, err := DecodeTransferStateMessage(stateFrame.Payload)
	if err != nil {
		t.Fatalf("decode resume state: %v", err)
	}
	if state.Flags != TransferStateFlagResumeRejected || state.ReasonCode != NackProtocolError {
		t.Fatalf("resume state = %+v", state)
	}
	if !writer.aborted {
		t.Fatal("invalid resumed writer was not aborted")
	}
}

func TestResumeStoreTimeoutIsHardAndKeepsOpeningReserved(t *testing.T) {
	transport := newRecordingTransport(t)
	transport.caps |= CapabilityTransferResume
	writer := &resumeTimeoutWriter{aborted: make(chan struct{})}
	store := &blockingResumeStore{
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
		firstWriter: writer,
	}
	config := DefaultServerConfig()
	config.Capabilities |= CapabilityTransferResume
	config.FlowControl.MaxConcurrentTransfers = 1
	config.FlowControl.TransferOpenTimeout = 20 * time.Millisecond
	config.Resume.Store = store
	session := NewSessionWithConfig(transport, config)

	started := time.Now()
	if err := session.HandleFrame(t.Context(), resumeFrame(55)); err != nil {
		t.Fatalf("timed out resume: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("resume store timeout took %s", elapsed)
	}
	<-store.entered
	if err := session.HandleFrame(t.Context(), resumeFrame(56)); err != nil {
		t.Fatalf("resume rejected by opening limit: %v", err)
	}
	if got := store.callCount(); got != 1 {
		t.Fatalf("store calls while first call leaked = %d", got)
	}

	close(store.release)
	select {
	case <-writer.aborted:
	case <-time.After(time.Second):
		t.Fatal("writer returned after resume timeout was not aborted")
	}
	deadline := time.Now().Add(time.Second)
	for store.callCount() == 1 {
		if err := session.HandleFrame(t.Context(), resumeFrame(57)); err != nil {
			t.Fatalf("resume after opening release: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("resume opening slot was not released")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestResumeStorePanicRejectsOnlyResume(t *testing.T) {
	transport := newRecordingTransport(t)
	transport.caps |= CapabilityTransferResume
	config := DefaultServerConfig()
	config.Capabilities |= CapabilityTransferResume
	config.Resume.Store = panicResumeStore{}
	session := NewSessionWithConfig(transport, config)
	if err := session.HandleFrame(t.Context(), resumeFrame(70)); err != nil {
		t.Fatalf("resume store panic escaped: %v", err)
	}
	stateFrame := requireFrameType(t, transport.frames(), FrameTransferState)
	state, err := DecodeTransferStateMessage(stateFrame.Payload)
	if err != nil || state.Flags != TransferStateFlagResumeRejected || state.ReasonCode != NackWriteFailed {
		t.Fatalf("panic resume state = %+v err=%v", state, err)
	}
	if session.State() != SessionEstablished {
		t.Fatalf("session state = %s", session.State())
	}
}

func TestResumeStoreKeepsTokenStableAfterOpenTimeout(t *testing.T) {
	transport := newRecordingTransport(t)
	transport.caps |= CapabilityTransferResume
	store := &blockingTokenResumeStore{
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
		observed: make(chan string, 1),
	}
	config := DefaultServerConfig()
	config.Capabilities |= CapabilityTransferResume
	config.FlowControl.TransferOpenTimeout = 10 * time.Millisecond
	config.Resume.Store = store
	session := NewSessionWithConfig(transport, config)

	payload := EncodeTransferResume(TransferResume{TransferID: 80, Token: []byte("stable-token")})
	frame := NewFrame(FrameTransferResume, SchemaTransferState, payload)
	frame.Header.TransferID = 80
	if err := session.HandleFrame(t.Context(), frame); err != nil {
		t.Fatalf("timed out resume: %v", err)
	}
	<-store.entered
	for i := 24; i < len(payload); i++ {
		payload[i] = 'x'
	}
	close(store.release)
	select {
	case got := <-store.observed:
		if got != "stable-token" {
			t.Fatalf("resume store observed recycled token %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out resume store did not return")
	}
}

func TestRequestTransferResumeRejectsOversizedToken(t *testing.T) {
	transport := newRecordingTransport(t)
	transport.caps |= CapabilityTransferResume
	config := DefaultClientConfig()
	config.Capabilities |= CapabilityTransferResume
	session := NewSessionWithConfig(transport, config)
	err := session.RequestTransferResume(TransferResume{
		TransferID: 1,
		Token:      make([]byte, MaxFrameBytes-HeaderSize-24+1),
	})
	if err == nil {
		t.Fatal("oversized resume token was sent")
	}
	if len(transport.frames()) != 0 {
		t.Fatal("transport observed oversized resume token")
	}
}

func resumeFrame(transferID uint64) Frame {
	frame := NewFrame(FrameTransferResume, SchemaTransferState, EncodeTransferResume(TransferResume{TransferID: transferID}))
	frame.Header.TransferID = transferID
	return frame
}

func (*resumableDisconnectWriter) Write([]byte) (int, error) { return 0, context.Canceled }
func (w *resumableDisconnectWriter) WriteContext(ctx context.Context, _ []byte) (int, error) {
	close(w.entered)
	<-ctx.Done()
	return 0, ctx.Err()
}
func (*resumableDisconnectWriter) Close() error { return nil }
func (w *resumableDisconnectWriter) Suspend() error {
	close(w.suspended)
	return nil
}
func (w *resumableDisconnectWriter) Abort() error {
	close(w.aborted)
	return nil
}

func TestNetworkDisconnectSuspendsResumableIncomingWriter(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer := &resumableDisconnectWriter{
		entered:   make(chan struct{}),
		suspended: make(chan struct{}),
		aborted:   make(chan struct{}),
	}
	serverConfig := DefaultServerConfig()
	serverConfig.Receive.TransferHandler = func(context.Context, IncomingTransferInfo) (IncomingTransferWriter, error) {
		return writer, nil
	}
	client := NewSessionWithConfig(NewStreamTransport(left), DefaultClientConfig())
	server := NewSessionWithConfig(NewStreamTransport(right), serverConfig)
	go func() { _ = client.Run(ctx) }()
	go func() { _ = server.Run(ctx) }()
	handshakeSessions(t, client, server)

	payload := bytes.Repeat([]byte("x"), 1<<20)
	handle := client.StartTransfer(ctx, TransferOptions{
		Reader:    bytes.NewReader(payload),
		TotalSize: uint64(len(payload)),
		ChunkSize: 32 << 10,
	})
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("resumable writer did not receive a chunk")
	}
	if err := right.Close(); err != nil {
		t.Fatalf("disconnect: %v", err)
	}
	select {
	case <-writer.suspended:
	case <-time.After(time.Second):
		t.Fatal("resumable writer was not suspended after network loss")
	}
	select {
	case <-writer.aborted:
		t.Fatal("network loss aborted resumable partial data")
	default:
	}
	select {
	case err := <-handle.Done():
		if err == nil {
			t.Fatal("outgoing transfer succeeded after disconnect")
		}
	case <-time.After(time.Second):
		t.Fatal("outgoing transfer remained blocked after disconnect")
	}
}
