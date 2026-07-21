package etp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	protocol "github.com/elum-utils/go-etp/internal/etp"
)

func TestAppRoutesInlineRequest(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	app := New(Config{})
	received := make(chan string, 1)
	if err := app.On("message.ping", func(ctx *Context) error {
		data, err := ctx.Bytes()
		if err != nil {
			return err
		}
		received <- string(data)
		return nil
	}); err != nil {
		t.Fatalf("on: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serverDone := make(chan error, 1)
	go func() {
		_, err := app.ServeTransport(ctx, "pipe", protocol.NewStreamTransport(right))
		if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
			serverDone <- nil
			return
		}
		serverDone <- err
	}()

	client := protocol.NewSessionWithConfig(protocol.NewStreamTransport(left), protocol.DefaultClientConfig())
	clientCtx, clientCancel := context.WithCancel(context.Background())
	defer clientCancel()
	clientDone := make(chan error, 1)
	go func() {
		clientDone <- client.Run(clientCtx)
	}()
	if err := client.SendHello(protocol.RoleClient); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	waitSessionEstablished(t, client)
	if _, err := client.Request(context.Background(), protocol.MessageOptions{Event: "message.ping", Data: []byte("hello")}); err != nil {
		t.Fatalf("request: %v", err)
	}

	select {
	case got := <-received:
		if got != "hello" {
			t.Fatalf("received = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for handler")
	}
	cancel()
	clientCancel()
	_ = left.Close()
	<-serverDone
	<-clientDone
}

func TestAppDispatchInlineDoesNotAllocate(t *testing.T) {
	app := New(Config{})
	if err := app.On("message.ping", func(*Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	app.Compile()
	peer := &Peer{app: app}
	frame := protocol.NewFrame(protocol.FrameRequest, protocol.SchemaEvent, nil)
	frame.Header.RequestID = 42
	message := protocol.EventMessageView{Event: []byte("message.ping"), Data: []byte("hello")}
	ctx := context.Background()

	if err := peer.handleRequest(ctx, frame, message); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if err := peer.handleRequest(ctx, frame, message); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("dispatch allocations = %v, want 0", allocs)
	}
}

func TestContextBodyViewSharesInlinePayload(t *testing.T) {
	payload := []byte("hello")
	ctx := Context{Body: NewBytesBody(payload)}
	view, ok := ctx.BodyView()
	if !ok || !bytes.Equal(view, payload) {
		t.Fatalf("view = %q, ok = %v", view, ok)
	}
	view[0] = 'H'
	if payload[0] != 'H' {
		t.Fatalf("body view unexpectedly copied payload")
	}
}

func TestAppRoutesTransferRequest(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	app := New(Config{MaxMemoryBody: 4})
	received := make(chan string, 1)
	if err := app.On("attach.upload", func(ctx *Context) error {
		data, err := ctx.Bytes()
		if err == ErrBodyTooLargeForBytes {
			reader, openErr := ctx.Body.Open()
			if openErr != nil {
				return openErr
			}
			defer reader.Close()
			data, err = io.ReadAll(reader)
		}
		if err != nil {
			return err
		}
		received <- string(data)
		return nil
	}); err != nil {
		t.Fatalf("on: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serverDone := make(chan error, 1)
	go func() {
		_, err := app.ServeTransport(ctx, "pipe", protocol.NewStreamTransport(right))
		if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
			serverDone <- nil
			return
		}
		serverDone <- err
	}()

	clientConfig := protocol.DefaultClientConfig()
	clientConfig.Payload.MaxInlineBodyBytes = 8
	client := protocol.NewSessionWithConfig(protocol.NewStreamTransport(left), clientConfig)
	clientCtx, clientCancel := context.WithCancel(context.Background())
	defer clientCancel()
	clientDone := make(chan error, 1)
	go func() {
		err := client.Run(clientCtx)
		if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) || clientCtx.Err() != nil {
			clientDone <- nil
			return
		}
		clientDone <- err
	}()
	if err := client.SendHello(protocol.RoleClient); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	waitSessionEstablished(t, client)

	body := bytes.Repeat([]byte("x"), 32)
	handle, err := client.Request(context.Background(), protocol.MessageOptions{Event: "attach.upload", Data: body, Field: "file", Name: "x.bin"})
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if handle.TransferID == 0 {
		t.Fatalf("expected transfer path")
	}
	if err := <-handle.Done(); err != nil {
		t.Fatalf("transfer done: %v", err)
	}

	select {
	case got := <-received:
		if got != string(body) {
			t.Fatalf("received = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for transfer handler")
	}
	cancel()
	clientCancel()
	_ = left.Close()
	<-serverDone
	<-clientDone
}

func waitSessionEstablished(t *testing.T, session *protocol.Session) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for session.State() != protocol.SessionEstablished {
		if state := session.State(); state == protocol.SessionFailed || time.Now().After(deadline) {
			t.Fatalf("session did not become established: %s", state)
		}
		time.Sleep(time.Millisecond)
	}
}
