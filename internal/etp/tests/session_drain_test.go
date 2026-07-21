package tests

import (
	"bytes"
	"context"
	"errors"
	. "github.com/elum-utils/go-etp/internal/etp"
	"net"
	"testing"
	"time"
)

func TestGracefulDrainWaitsForActiveTransferAndCloseAck(t *testing.T) {
	left, right := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer left.Close()
	defer right.Close()

	writer := &controlledTransferWriter{entered: make(chan struct{}, 1), release: make(chan struct{})}
	clientConfig := DefaultClientConfig()
	clientConfig.Receive.TransferHandler = func(context.Context, IncomingTransferInfo) (IncomingTransferWriter, error) { return writer, nil }
	client := NewSessionWithConfig(NewStreamTransport(left), clientConfig)
	server := NewSessionWithConfig(NewStreamTransport(right), DefaultServerConfig())
	go func() { _ = client.Run(ctx) }()
	go func() { _ = server.Run(ctx) }()
	handshakeSessions(t, client, server)

	transferDone := make(chan error, 1)
	go func() {
		_, err := server.SendTransfer("active.bin", ContentFile, bytes.NewReader([]byte("data")), 4)
		transferDone <- err
	}()
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("transfer writer did not start")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- server.CloseWith(CloseMessage{ReasonCode: CloseServerShutdown, Flags: CloseFlagDrain | CloseFlagNoNewRequests | CloseFlagNoNewTransfers, DrainTimeoutMillis: 3000})
	}()
	select {
	case err := <-closeDone:
		t.Fatalf("drain closed before active transfer completed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(writer.release)
	select {
	case err := <-transferDone:
		if err != nil {
			t.Fatalf("transfer during drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("active transfer did not complete during drain")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("graceful close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close ack was not received after drain")
	}
	deadline := time.Now().Add(time.Second)
	for client.State() != SessionClosed && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.State() != SessionClosed || client.State() != SessionClosed {
		t.Fatalf("states after drain: server=%s client=%s", server.State(), client.State())
	}
}

func TestPeerDrainCloseAckFailureMarksSessionFailed(t *testing.T) {
	transport := newRecordingTransport(t)
	transport.err = errors.New("carrier write failed")
	session := NewSessionWithConfig(transport, DefaultServerConfig())
	closeFrame := NewFrame(FrameClose, SchemaClose, EncodeCloseMessage(CloseMessage{
		ReasonCode: CloseNormal,
		Flags:      CloseFlagDrain,
	}))
	if err := session.HandleFrame(t.Context(), closeFrame); err != nil {
		t.Fatalf("handle peer drain: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for session.State() != SessionFailed && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if session.State() != SessionFailed {
		t.Fatalf("state after CloseAck write failure = %s", session.State())
	}
}
