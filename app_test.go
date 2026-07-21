package etp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
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

func TestAppLifecycleAuthConnectDisconnect(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	app := New(Config{})
	authenticated := make(chan struct{}, 1)
	connected := make(chan SessionIdentity, 1)
	disconnected := make(chan error, 1)
	if err := app.OnAuth(func(_ context.Context, request AuthRequest) (AuthResult, error) {
		if string(request.Payload) != "token" {
			return AuthResult{OK: false}, nil
		}
		authenticated <- struct{}{}
		return AuthResult{OK: true, UserID: "user-42"}, nil
	}); err != nil {
		t.Fatalf("on auth: %v", err)
	}
	if err := app.OnConnect(func(_ context.Context, peer *Peer) error {
		connected <- peer.Identity()
		return nil
	}); err != nil {
		t.Fatalf("on connect: %v", err)
	}
	if err := app.OnDisconnect(func(_ context.Context, _ *Peer, cause error) {
		disconnected <- cause
	}); err != nil {
		t.Fatalf("on disconnect: %v", err)
	}

	serverCtx, stopServer := context.WithCancel(context.Background())
	defer stopServer()
	serverDone := make(chan error, 1)
	go func() {
		_, err := app.ServeTransport(serverCtx, "pipe", protocol.NewStreamTransport(right))
		serverDone <- err
	}()

	client := protocol.NewSessionWithConfig(protocol.NewStreamTransport(left), protocol.DefaultClientConfig())
	clientCtx, stopClient := context.WithCancel(context.Background())
	defer stopClient()
	clientDone := make(chan error, 1)
	go func() { clientDone <- client.Run(clientCtx) }()

	if err := client.Authenticate(protocol.AuthRequest{Payload: []byte("token")}); err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	select {
	case <-authenticated:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for auth handler")
	}
	waitSessionState(t, client, "AUTH_ACCEPTED")
	if err := client.SendHello(protocol.RoleClient); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	select {
	case identity := <-connected:
		if identity.UserID != "user-42" {
			t.Fatalf("connected identity = %+v", identity)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for connect handler")
	}

	stopServer()
	stopClient()
	_ = left.Close()
	_ = right.Close()
	<-serverDone
	<-clientDone
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for disconnect handler")
	}
}

func TestAppOnNotFound(t *testing.T) {
	app := New(Config{})
	want := errors.New("unknown event")
	if err := app.OnNotFound(func(ctx *Context) error {
		if ctx.Event != "unknown.event" {
			t.Fatalf("event = %q", ctx.Event)
		}
		return want
	}); err != nil {
		t.Fatalf("on not found: %v", err)
	}
	app.Compile()

	peer := &Peer{app: app}
	frame := protocol.NewFrame(protocol.FrameRequest, protocol.SchemaEvent, nil)
	err := peer.handleRequest(context.Background(), frame, protocol.EventMessageView{Event: []byte("unknown.event")})
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func TestAppOnProtocolEventAuthReject(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	app := New(Config{})
	if err := app.OnAuth(func(context.Context, AuthRequest) (AuthResult, error) {
		return AuthResult{OK: false, Reason: "bad token"}, nil
	}); err != nil {
		t.Fatalf("on auth: %v", err)
	}
	protocolEvents := make(chan ProtocolEvent, 1)
	if err := app.OnProtocolEvent(func(_ context.Context, _ *Peer, event ProtocolEvent) {
		if event.Code == EventAuthRejected {
			protocolEvents <- event
		}
	}); err != nil {
		t.Fatalf("on protocol event: %v", err)
	}

	serverCtx, stopServer := context.WithCancel(context.Background())
	defer stopServer()
	serverDone := make(chan error, 1)
	go func() {
		_, err := app.ServeTransport(serverCtx, "pipe", protocol.NewStreamTransport(right))
		serverDone <- err
	}()

	client := protocol.NewSessionWithConfig(protocol.NewStreamTransport(left), protocol.DefaultClientConfig())
	clientCtx, stopClient := context.WithCancel(context.Background())
	defer stopClient()
	clientDone := make(chan error, 1)
	go func() { clientDone <- client.Run(clientCtx) }()
	if err := client.Authenticate(protocol.AuthRequest{Payload: []byte("bad-token")}); err != nil {
		t.Fatalf("authenticate: %v", err)
	}

	select {
	case event := <-protocolEvents:
		if event.Message != "bad token" {
			t.Fatalf("event message = %q", event.Message)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for auth rejected event")
	}

	stopServer()
	stopClient()
	_ = left.Close()
	_ = right.Close()
	<-serverDone
	<-clientDone
}

func TestAppOnError(t *testing.T) {
	app := New(Config{})
	want := errors.New("handler failed")
	reported := make(chan error, 1)
	if err := app.OnError(func(_ *Context, err error) {
		reported <- err
	}); err != nil {
		t.Fatalf("on error: %v", err)
	}
	if err := app.On("message.fail", func(*Context) error { return want }); err != nil {
		t.Fatalf("on: %v", err)
	}
	app.Compile()

	peer := &Peer{app: app}
	frame := protocol.NewFrame(protocol.FrameRequest, protocol.SchemaEvent, nil)
	err := peer.handleRequest(context.Background(), frame, protocol.EventMessageView{Event: []byte("message.fail")})
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	select {
	case got := <-reported:
		if !errors.Is(got, want) {
			t.Fatalf("reported error = %v, want %v", got, want)
		}
	default:
		t.Fatal("error handler was not called")
	}
}

func TestAppLifecycleRegistrationAfterCompile(t *testing.T) {
	app := New(Config{})
	app.Compile()
	if err := app.OnAuth(func(context.Context, AuthRequest) (AuthResult, error) { return AuthResult{}, nil }); !errors.Is(err, ErrRouterCompiled) {
		t.Fatalf("on auth error = %v", err)
	}
	if err := app.OnConnect(func(context.Context, *Peer) error { return nil }); !errors.Is(err, ErrRouterCompiled) {
		t.Fatalf("on connect error = %v", err)
	}
	if err := app.OnDisconnect(func(context.Context, *Peer, error) {}); !errors.Is(err, ErrRouterCompiled) {
		t.Fatalf("on disconnect error = %v", err)
	}
	if err := app.OnNotFound(func(*Context) error { return nil }); !errors.Is(err, ErrRouterCompiled) {
		t.Fatalf("on not found error = %v", err)
	}
	if err := app.OnError(func(*Context, error) {}); !errors.Is(err, ErrRouterCompiled) {
		t.Fatalf("on error error = %v", err)
	}
	if err := app.OnProtocolEvent(func(context.Context, *Peer, ProtocolEvent) {}); !errors.Is(err, ErrRouterCompiled) {
		t.Fatalf("on protocol event error = %v", err)
	}
	if err := app.OnProgress(func(context.Context, *Peer, Progress) {}); !errors.Is(err, ErrRouterCompiled) {
		t.Fatalf("on progress error = %v", err)
	}
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
	var progressed atomic.Bool
	if err := app.OnProgress(func(_ context.Context, _ *Peer, progress Progress) {
		if progress.TransferID != 0 {
			progressed.Store(true)
		}
	}); err != nil {
		t.Fatalf("on progress: %v", err)
	}
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
	if !progressed.Load() {
		t.Fatal("progress handler was not called")
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

func waitSessionState(t *testing.T, session *protocol.Session, want string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for session.State().String() != want {
		if state := session.State(); state == protocol.SessionFailed || time.Now().After(deadline) {
			t.Fatalf("session state = %s, want %s", state, want)
		}
		time.Sleep(time.Millisecond)
	}
}
