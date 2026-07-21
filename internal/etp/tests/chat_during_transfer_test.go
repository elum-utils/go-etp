package tests

import (
	"bytes"
	"context"
	. "github.com/elum-utils/go-etp/internal/etp"
	"net"
	"sync"
	"testing"
	"time"
)

func TestChatRequestsStayResponsiveDuringLargeTransfer(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientTransport := NewStreamTransport(left)
	serverTransport := NewStreamTransport(right)

	clientConfig := DefaultSessionConfig(RoleClient)
	clientConfig.FlowControl.MaxChunkSize = 32 * 1024
	clientConfig.FlowControl.MaxInFlightChunks = 8
	clientConfig.FlowControl.MaxInFlightBytes = 256 * 1024
	clientConfig.FlowControl.MaxReceiveBufferBytes = 4 << 20
	clientConfig.FlowControl.MaxTransferBytes = 4 << 20

	transferStarted := make(chan struct{})
	transferWrites := make(chan struct{}, 64)
	clientConfig.Receive.TransferHandler = func(ctx context.Context, info IncomingTransferInfo) (IncomingTransferWriter, error) {
		close(transferStarted)
		return &slowIncomingWriter{delay: 2 * time.Millisecond, writes: transferWrites}, nil
	}

	var startsMu sync.Mutex
	starts := map[uint64]time.Time{}
	responses := make(chan time.Duration, 32)
	clientConfig.Receive.ResponseHandler = func(ctx context.Context, frame Frame, message EventMessageView) error {
		startsMu.Lock()
		start := starts[frame.Header.RequestID]
		startsMu.Unlock()
		responses <- time.Since(start)
		return nil
	}

	var server *Session
	serverConfig := DefaultSessionConfig(RoleServer)
	serverConfig.FlowControl.MaxChunkSize = 32 * 1024
	serverConfig.FlowControl.MaxInFlightChunks = 8
	serverConfig.FlowControl.MaxInFlightBytes = 256 * 1024
	serverConfig.FlowControl.MaxTransferBytes = 4 << 20
	serverConfig.Receive.RequestHandler = func(ctx context.Context, frame Frame, message EventMessageView) error {
		if !bytes.Equal(message.Event, []byte("chat.ping")) {
			t.Errorf("unexpected event: %q", message.Event)
		}
		return server.SendResponse(frame.Header.RequestID, "chat.pong", nil)
	}

	client := NewSessionWithConfig(clientTransport, clientConfig)
	server = NewSessionWithConfig(serverTransport, serverConfig)

	errs := make(chan error, 2)
	go func() { errs <- client.Run(ctx) }()
	go func() { errs <- server.Run(ctx) }()
	handshakeSessions(t, client, server)

	file := bytes.Repeat([]byte("x"), 1<<20)
	transferDone := make(chan error, 1)
	go func() {
		_, err := server.SendTransfer("video.bin", ContentMedia, bytes.NewReader(file), uint64(len(file)))
		transferDone <- err
	}()

	select {
	case <-transferStarted:
	case <-time.After(time.Second):
		t.Fatal("transfer did not start")
	}
	select {
	case <-transferWrites:
	case <-time.After(time.Second):
		t.Fatal("transfer did not write first chunk")
	}

	const requestCount = 10
	for i := 0; i < requestCount; i++ {
		start := time.Now()
		requestID, err := client.SendRequest("chat.ping", nil)
		if err != nil {
			t.Fatalf("send request: %v", err)
		}
		startsMu.Lock()
		starts[requestID] = start
		startsMu.Unlock()
		time.Sleep(3 * time.Millisecond)
	}

	var maxLatency time.Duration
	for i := 0; i < requestCount; i++ {
		select {
		case latency := <-responses:
			if latency > maxLatency {
				maxLatency = latency
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("chat response %d did not arrive during transfer", i+1)
		}
	}
	if maxLatency > 150*time.Millisecond {
		t.Fatalf("chat response latency too high during transfer: %s", maxLatency)
	}

	select {
	case err := <-transferDone:
		if err != nil {
			t.Fatalf("transfer failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("transfer did not complete")
	}
}

type slowIncomingWriter struct {
	delay  time.Duration
	writes chan struct{}
}

func (w *slowIncomingWriter) Write(p []byte) (int, error) {
	time.Sleep(w.delay)
	select {
	case w.writes <- struct{}{}:
	default:
	}
	return len(p), nil
}

func (w *slowIncomingWriter) Close() error { return nil }
