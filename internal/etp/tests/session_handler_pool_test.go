package tests

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	. "github.com/elum-utils/go-etp/internal/etp"
)

type observingTransport struct {
	inner  *StreamTransport
	frames chan Frame
}

type cancellableReadTransport struct {
	reads  chan []byte
	closed chan struct{}
	once   sync.Once
}

func newCancellableReadTransport() *cancellableReadTransport {
	return &cancellableReadTransport{reads: make(chan []byte, 1), closed: make(chan struct{})}
}

func (*cancellableReadTransport) SendFrame([]byte) error { return nil }
func (t *cancellableReadTransport) ReadFrame() ([]byte, error) {
	select {
	case frame := <-t.reads:
		return frame, nil
	case <-t.closed:
		return nil, io.EOF
	}
}
func (t *cancellableReadTransport) Close() error {
	t.once.Do(func() { close(t.closed) })
	return nil
}
func (*cancellableReadTransport) NegotiatedCapabilities() uint64 { return DefaultCapabilities }

func (t *observingTransport) SendFrame(frame []byte) error { return t.inner.SendFrame(frame) }
func (t *observingTransport) Close() error                 { return t.inner.Close() }
func (t *observingTransport) SetMaxFrameBytes(max uint32)  { t.inner.SetMaxFrameBytes(max) }
func (t *observingTransport) ReadFrame() ([]byte, error) {
	data, err := t.inner.ReadFrame()
	if err != nil {
		return nil, err
	}
	frame, err := DecodeFrameView(data)
	if err == nil {
		select {
		case t.frames <- frame:
		default:
		}
	}
	return data, nil
}

func newObservedSessionPair(t *testing.T, serverConfig SessionConfig) (*Session, *Session, *observingTransport, context.CancelFunc) {
	t.Helper()
	left, right := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	clientTransport := &observingTransport{inner: NewStreamTransport(left), frames: make(chan Frame, 32)}
	serverTransport := NewStreamTransport(right)
	client := NewSessionWithConfig(clientTransport, DefaultClientConfig())
	server := NewSessionWithConfig(serverTransport, serverConfig)
	go func() { _ = client.Run(ctx) }()
	go func() { _ = server.Run(ctx) }()
	handshakeSessions(t, client, server)
	return client, server, clientTransport, func() {
		cancel()
		_ = left.Close()
		_ = right.Close()
	}
}

func TestHandlerPoolKeepsHeartbeatResponsiveWhileHandlerBlocks(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	config := DefaultServerConfig()
	config.Handlers.Workers = 1
	config.Handlers.Queue = 1
	config.Receive.RequestHandler = func(context.Context, Frame, EventMessageView) error {
		entered <- struct{}{}
		<-release
		return nil
	}
	client, _, observed, closePair := newObservedSessionPair(t, config)
	defer closePair()
	if _, err := client.SendRequest("slow.request", nil); err != nil {
		t.Fatalf("request: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	if err := client.SendPing(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	deadline := time.After(time.Second)
	for {
		select {
		case frame := <-observed.frames:
			if frame.Header.FrameType == FramePong {
				close(release)
				return
			}
		case <-deadline:
			close(release)
			t.Fatal("pong was blocked behind application handler")
		}
	}
}

func TestHandlerPoolRejectsOverflowWithoutGrowingUnbounded(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	config := DefaultServerConfig()
	config.Handlers.Workers = 1
	config.Handlers.Queue = 1
	config.Receive.RequestHandler = func(context.Context, Frame, EventMessageView) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return nil
	}
	client, _, observed, closePair := newObservedSessionPair(t, config)
	defer closePair()
	if _, err := client.SendRequest("slow.1", nil); err != nil {
		t.Fatalf("request 1: %v", err)
	}
	<-entered
	if _, err := client.SendRequest("slow.2", nil); err != nil {
		t.Fatalf("request 2: %v", err)
	}
	if _, err := client.SendRequest("slow.3", nil); err != nil {
		t.Fatalf("request 3: %v", err)
	}
	defer close(release)
	deadline := time.After(time.Second)
	for {
		select {
		case frame := <-observed.frames:
			if frame.Header.FrameType != FrameError {
				continue
			}
			message, err := DecodeErrorMessage(frame.Payload)
			if err != nil || message.Code != ErrorRateLimited {
				t.Fatalf("overflow error = %+v err=%v", message, err)
			}
			return
		case <-deadline:
			t.Fatal("handler queue overflow was not rejected")
		}
	}
}

func TestHandlerPanicBecomesProtocolEventAndSessionStaysAlive(t *testing.T) {
	config := DefaultServerConfig()
	config.Receive.RequestHandler = func(context.Context, Frame, EventMessageView) error {
		panic("handler failed")
	}
	client, server, observed, closePair := newObservedSessionPair(t, config)
	defer closePair()
	events := make(chan ProtocolEvent, 1)
	server.OnProtocolEvent(func(event ProtocolEvent) {
		if event.Code == EventHandlerFailed {
			events <- event
		}
	})
	if _, err := client.SendRequest("panic.request", nil); err != nil {
		t.Fatalf("request: %v", err)
	}
	select {
	case event := <-events:
		if event.Message == "" {
			t.Fatalf("empty panic event: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("handler panic was not surfaced")
	}
	if server.State() != SessionEstablished {
		t.Fatalf("server state after handler panic = %s", server.State())
	}
	if err := client.SendPing(); err != nil {
		t.Fatalf("ping after handler panic: %v", err)
	}
	deadline := time.After(time.Second)
	for {
		select {
		case frame := <-observed.frames:
			if frame.Header.FrameType == FramePong {
				return
			}
		case <-deadline:
			t.Fatal("session stopped after handler panic")
		}
	}
}

func TestRunReturnsWhenApplicationHandlerIgnoresCancellation(t *testing.T) {
	transport := newCancellableReadTransport()
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	config := DefaultServerConfig()
	config.Handlers.Workers = 1
	config.Receive.RequestHandler = func(context.Context, Frame, EventMessageView) error {
		close(entered)
		<-release
		return nil
	}
	session := NewSessionWithConfig(transport, config)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- session.Run(ctx) }()
	request := NewFrame(FrameRequest, SchemaEvent, EncodeEventMessage(EventMessage{Event: "blocked.handler"}))
	request.Header.RequestID = 1
	encoded, err := EncodeFrame(request)
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}
	transport.reads <- encoded
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancel()
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("Run waited forever for an uncooperative handler")
	}
}
