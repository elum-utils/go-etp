package tests

import (
	"bytes"
	"context"
	. "github.com/elum-utils/go-etp/internal/etp"
	"io"
	"sync"
	"testing"
	"time"
)

type gateReader struct {
	gate <-chan struct{}
	data []byte
	done bool
}

type resumeStore struct {
	decision TransferResumeDecision
	err      error
	called   bool
	calls    int
}

func (s *resumeStore) ResumeIncoming(ctx context.Context, resume TransferResumeView) (TransferResumeDecision, error) {
	s.called = true
	s.calls++
	return s.decision, s.err
}

type bufferIncomingWriter struct {
	bytes.Buffer
	mu     sync.Mutex
	closed bool
}

func (w *bufferIncomingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.Buffer.Write(p)
}

func (w *bufferIncomingWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}

func (w *bufferIncomingWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.Buffer.String()
}

func (w *bufferIncomingWriter) Closed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

func (r *gateReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	<-r.gate
	r.done = true
	return copy(p, r.data), nil
}

func TestSessionWindowBlocksAndReleasesTransferSend(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSession(transport)
	gate := make(chan struct{})
	handle := session.StartTransfer(context.Background(), TransferOptions{
		Name:             "window.txt",
		ContentType:      ContentFile,
		Reader:           &gateReader{gate: gate, data: []byte("hello")},
		TotalSize:        5,
		MaxInFlight:      1,
		MaxInFlightBytes: 1024,
		AckTimeout:       time.Second,
	})
	begin := requireFrameType(t, waitFrames(t, transport, FrameTransferBegin), FrameTransferBegin)
	if err := session.HandleWindow(NewFrame(FrameWindow, SchemaWindow, EncodeWindow(Window{
		TransferID:   begin.Header.TransferID,
		WindowBytes:  0,
		WindowChunks: 0,
		Flags:        WindowFlagTransfer,
	}))); err != nil {
		t.Fatalf("handle zero window: %v", err)
	}
	close(gate)
	time.Sleep(20 * time.Millisecond)
	if frameCount(transport.frames(), FrameData) != 0 {
		t.Fatalf("data was sent while remote window is zero")
	}
	if err := session.HandleWindow(NewFrame(FrameWindow, SchemaWindow, EncodeWindow(Window{
		TransferID:   begin.Header.TransferID,
		WindowBytes:  5,
		WindowChunks: 1,
		Flags:        WindowFlagTransfer,
	}))); err != nil {
		t.Fatalf("handle open window: %v", err)
	}
	requireFrameType(t, waitFrames(t, transport, FrameData), FrameData)
	data := requireFrameType(t, transport.frames(), FrameData)
	ack := NewFrame(FrameAck, SchemaAck, EncodeAck(Ack{
		TransferID:    data.Header.TransferID,
		ChunkFrom:     data.Header.ChunkID,
		ChunkTo:       data.Header.ChunkID,
		ReceivedBytes: uint64(len(data.Payload)),
	}))
	if err := session.HandleAck(ack); err != nil {
		t.Fatalf("ack: %v", err)
	}
	requireFrameType(t, waitFrames(t, transport, FrameTransferEnd), FrameTransferEnd)
	completeOutgoingTransfer(t, session, data.Header.TransferID, uint64(len(data.Payload)), data.Header.ChunkID+1)
	if err := <-handle.Done(); err != nil {
		t.Fatalf("transfer done: %v", err)
	}
}

func TestSessionCapabilityEnforcementRejectsUnsupportedRequest(t *testing.T) {
	session := NewSession(newRecordingTransport(t))
	remoteHello := validHello(RoleServer)
	remoteHello.Capabilities = CapabilityAck
	hello := NewFrame(FrameHelloAck, SchemaHello, EncodeHelloMessage(remoteHello))
	if err := session.HandleHelloAck(hello); err != nil {
		t.Fatalf("hello ack: %v", err)
	}
	if _, err := session.SendRequest("message.get", nil); err == nil {
		t.Fatalf("expected unsupported request/response capability error")
	}
}

func TestSessionRateLimitRejectsTooManyFrames(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultSessionConfig(RoleServer)
	config.RateLimit.MaxFramesPerSecond = 1
	session := NewSessionWithConfig(transport, config)
	ping := NewFrame(FramePing, 0, nil)
	if err := session.HandleFrame(context.Background(), ping); err != nil {
		t.Fatalf("first ping: %v", err)
	}
	if err := session.HandleFrame(context.Background(), ping); err == nil {
		t.Fatalf("expected rate limit error")
	}
	requireFrameType(t, transport.frames(), FrameError)
}

func TestSessionRateLimitRejectsTooManyBadFrames(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultSessionConfig(RoleServer)
	config.RateLimit.MaxBadFramesPerSecond = 1
	session := NewSessionWithConfig(transport, config)
	bad := NewFrame(FrameRequest, SchemaEvent, EncodeEventMessage(EventMessage{Event: "bad"}))
	bad.Header.RequestID = 0
	if err := session.HandleFrame(context.Background(), bad); err == nil {
		t.Fatalf("expected first bad request error")
	}
	if err := session.HandleFrame(context.Background(), bad); err == nil {
		t.Fatalf("expected bad frame rate limit error")
	}
	requireFrameType(t, transport.frames(), FrameError)
}

func TestSessionTransferResumeWithoutNegotiatedCapabilityIsRejected(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSession(transport)
	resume := NewFrame(FrameTransferResume, SchemaTransferState, EncodeTransferResume(TransferResume{
		TransferID:    99,
		ReceivedBytes: 1024,
		NextChunk:     4,
		Token:         []byte("token"),
	}))
	resume.Header.TransferID = 99
	if err := session.HandleFrame(context.Background(), resume); err == nil {
		t.Fatal("unnegotiated resume was accepted")
	}
	errorFrame := requireFrameType(t, transport.frames(), FrameError)
	message, err := DecodeErrorMessage(errorFrame.Payload)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if message.Code != ErrorUnsupportedFeature || message.TransferID != 99 {
		t.Fatalf("error = %+v", message)
	}
}

func TestSessionTransferResumeStoreAcceptsAndContinuesTransfer(t *testing.T) {
	transport := newRecordingTransport(t)
	writer := &bufferIncomingWriter{}
	store := &resumeStore{
		decision: TransferResumeDecision{
			Accepted:      true,
			Meta:          TransferBegin{TotalSize: 8, ChunkSize: 4, ChunkCount: 2, Name: "resume.bin"},
			Writer:        writer,
			ReceivedBytes: 4,
			NextChunk:     1,
		},
	}
	config := DefaultSessionConfig(RoleServer)
	config.Capabilities |= CapabilityTransferResume
	config.Resume.Store = store
	transport.caps |= CapabilityTransferResume
	session := NewSessionWithConfig(transport, config)
	resume := NewFrame(FrameTransferResume, SchemaTransferState, EncodeTransferResume(TransferResume{
		TransferID:    44,
		ReceivedBytes: 4,
		NextChunk:     1,
	}))
	resume.Header.TransferID = 44
	if err := session.HandleFrame(context.Background(), resume); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !store.called {
		t.Fatalf("resume store was not called")
	}
	stateFrame := requireFrameType(t, transport.frames(), FrameTransferState)
	state, err := DecodeTransferStateMessage(stateFrame.Payload)
	if err != nil {
		t.Fatalf("decode state: %v", err)
	}
	if state.Flags != TransferStateFlagResumeAccepted || state.NextChunk != 1 || state.ReceivedBytes != 4 {
		t.Fatalf("state = %+v", state)
	}
	data := NewFrame(FrameData, 0, []byte("done"))
	data.Header.TransferID = 44
	data.Header.ChunkID = 1
	data.Header.Flags = FlagAckRequest | FlagLast
	if err := session.HandleFrame(context.Background(), data); err != nil {
		t.Fatalf("data: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for writer.String() != "done" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if writer.String() != "done" {
		t.Fatalf("writer = %q", writer.String())
	}
	end := NewFrame(FrameTransferEnd, 0, nil)
	end.Header.TransferID = 44
	if err := session.HandleFrame(context.Background(), end); err != nil {
		t.Fatalf("end: %v", err)
	}
	if !writer.Closed() {
		t.Fatalf("writer was not closed")
	}
}

func TestSessionTransferResumeRejectsDuplicateActiveTransfer(t *testing.T) {
	transport := newRecordingTransport(t)
	writer := &bufferIncomingWriter{}
	store := &resumeStore{
		decision: TransferResumeDecision{
			Accepted: true,
			Meta:     TransferBegin{TotalSize: 8, ChunkSize: 4, ChunkCount: 2, Name: "resume.bin"},
			Writer:   writer,
		},
	}
	config := DefaultSessionConfig(RoleServer)
	config.Capabilities |= CapabilityTransferResume
	config.Resume.Store = store
	transport.caps |= CapabilityTransferResume
	session := NewSessionWithConfig(transport, config)
	resume := NewFrame(FrameTransferResume, SchemaTransferState, EncodeTransferResume(TransferResume{
		TransferID: 55,
	}))
	resume.Header.TransferID = 55
	if err := session.HandleFrame(context.Background(), resume); err != nil {
		t.Fatalf("first resume: %v", err)
	}
	if err := session.HandleFrame(context.Background(), resume); err != nil {
		t.Fatalf("duplicate resume: %v", err)
	}
	if store.calls != 1 {
		t.Fatalf("resume store calls = %d, want 1", store.calls)
	}
	var states []TransferStateMessage
	for _, frame := range transport.frames() {
		if frame.Header.FrameType != FrameTransferState {
			continue
		}
		state, err := DecodeTransferStateMessage(frame.Payload)
		if err != nil {
			t.Fatalf("decode state: %v", err)
		}
		states = append(states, state)
	}
	if len(states) != 2 {
		t.Fatalf("transfer states = %d, want 2", len(states))
	}
	if states[0].Flags != TransferStateFlagResumeAccepted {
		t.Fatalf("first state = %+v", states[0])
	}
	if states[1].Flags != TransferStateFlagResumeRejected || states[1].ReasonCode != NackProtocolError {
		t.Fatalf("duplicate state = %+v", states[1])
	}
}

func TestSessionIncomingTransferSendsAutomaticWindows(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultSessionConfig(RoleServer)
	config.FlowControl.MaxInFlightBytes = 16
	config.FlowControl.MaxInFlightChunks = 2
	config.Receive.TransferHandler = func(ctx context.Context, info IncomingTransferInfo) (IncomingTransferWriter, error) {
		return &bufferIncomingWriter{}, nil
	}
	session := NewSessionWithConfig(transport, config)
	begin := NewFrame(FrameTransferBegin, SchemaTransferBegin, EncodeTransferBegin(TransferBegin{
		TotalSize:  4,
		ChunkSize:  4,
		ChunkCount: 1,
		Name:       "window.bin",
	}))
	begin.Header.TransferID = 77
	if err := session.HandleFrame(context.Background(), begin); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if frameCount(transport.frames(), FrameWindow) != 1 {
		t.Fatalf("expected initial window, frames=%v", frameCount(transport.frames(), FrameWindow))
	}
	data := NewFrame(FrameData, 0, []byte("data"))
	data.Header.TransferID = 77
	data.Header.ChunkID = 0
	data.Header.Flags = FlagAckRequest | FlagLast
	if err := session.HandleFrame(context.Background(), data); err != nil {
		t.Fatalf("data: %v", err)
	}
	if frameCount(transport.frames(), FrameWindow) < 2 {
		t.Fatalf("expected replenishment window")
	}
	windowFrame := requireFrameType(t, transport.frames(), FrameWindow)
	window, err := DecodeWindow(windowFrame.Payload)
	if err != nil {
		t.Fatalf("decode window: %v payload=%x", err, windowFrame.Payload)
	}
	if window.TransferID != 77 || window.WindowBytes != 16 || window.WindowChunks != 2 {
		t.Fatalf("window = %+v", window)
	}
}

func frameCount(frames []Frame, frameType uint8) int {
	var count int
	for _, frame := range frames {
		if frame.Header.FrameType == frameType {
			count++
		}
	}
	return count
}

func waitFrames(t *testing.T, transport *recordingTransport, frameType uint8) []Frame {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		frames := transport.frames()
		for _, frame := range frames {
			if frame.Header.FrameType == frameType {
				return frames
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("frame type %d was not sent", frameType)
	return nil
}
