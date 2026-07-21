package tests

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	. "github.com/elum-utils/go-etp/internal/etp"
)

type disconnectAwareWriter struct {
	entered  chan struct{}
	canceled chan struct{}
	aborted  chan struct{}
	once     sync.Once
}

func (w *disconnectAwareWriter) Write([]byte) (int, error) {
	return 0, errors.New("WriteContext was not used")
}

func (w *disconnectAwareWriter) WriteContext(ctx context.Context, _ []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-ctx.Done()
	close(w.canceled)
	return 0, ctx.Err()
}

func (w *disconnectAwareWriter) Close() error { return nil }

func (w *disconnectAwareWriter) Abort() error {
	close(w.aborted)
	return nil
}

func TestDisconnectFailsActiveTransferAndCancelsStorageWrite(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	writer := &disconnectAwareWriter{
		entered:  make(chan struct{}),
		canceled: make(chan struct{}),
		aborted:  make(chan struct{}),
	}
	serverConfig := DefaultServerConfig()
	serverConfig.Receive.TransferHandler = func(context.Context, IncomingTransferInfo) (IncomingTransferWriter, error) {
		return writer, nil
	}
	client := NewSessionWithConfig(NewStreamTransport(left), DefaultClientConfig())
	server := NewSessionWithConfig(NewStreamTransport(right), serverConfig)

	runErrors := make(chan error, 2)
	go func() { runErrors <- client.Run(ctx) }()
	go func() { runErrors <- server.Run(ctx) }()
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
		t.Fatal("storage writer did not receive the first chunk")
	}

	if err := right.Close(); err != nil {
		t.Fatalf("close peer connection: %v", err)
	}
	select {
	case err := <-handle.Done():
		if err == nil {
			t.Fatal("active transfer succeeded after the connection was lost")
		}
	case <-time.After(time.Second):
		t.Fatal("active transfer remained blocked after disconnect")
	}
	select {
	case <-writer.canceled:
	case <-time.After(time.Second):
		t.Fatal("storage WriteContext was not canceled after disconnect")
	}
	select {
	case <-writer.aborted:
	case <-time.After(time.Second):
		t.Fatal("partial storage writer was not aborted after disconnect")
	}

	for i := 0; i < 2; i++ {
		select {
		case err := <-runErrors:
			if err == nil {
				t.Fatal("session Run returned nil after disconnect")
			}
		case <-time.After(time.Second):
			t.Fatal("session Run remained blocked after disconnect")
		}
	}
}
