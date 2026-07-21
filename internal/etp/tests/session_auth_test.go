package tests

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	. "github.com/elum-utils/go-etp/internal/etp"
)

func TestSessionAuthAcceptsValidRequest(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultSessionConfig(RoleServer)
	config.Auth.Required = true
	config.Auth.Handler = func(ctx context.Context, req AuthRequest) (AuthResult, error) {
		if req.Method != AuthMethodBearer || !bytes.Equal(req.Payload, []byte("ok-token")) {
			return AuthResult{OK: false, RejectCode: AuthRejectUnauthorized, Reason: "bad token"}, nil
		}
		return AuthResult{
			OK:         true,
			UserID:     "user-42",
			Attributes: []AuthAttribute{{Key: "scope", Value: "files"}},
		}, nil
	}
	session := NewSessionWithConfig(transport, config)
	frame := NewFrame(FrameAuth, SchemaAuth, EncodeAuthRequest(AuthRequest{
		Method:       AuthMethodBearer,
		AuthSchemaID: SchemaAuth,
		Payload:      []byte("ok-token"),
	}))

	if err := session.HandleAuth(context.Background(), frame); err != nil {
		t.Fatalf("handle auth: %v", err)
	}
	if session.State() != SessionAuthAccepted {
		t.Fatalf("state = %s", session.State())
	}
	if session.Identity().UserID != "user-42" || session.GetAttribute("scope") != "files" {
		t.Fatalf("identity = %+v scope=%q", session.Identity(), session.GetAttribute("scope"))
	}
	sent := requireFrameType(t, transport.frames(), FrameAuthAccept)
	accept, err := DecodeAuthAccept(sent.Payload)
	if err != nil {
		t.Fatalf("decode auth accept: %v", err)
	}
	if accept.UserID != "user-42" {
		t.Fatalf("accept user = %q", accept.UserID)
	}
}

func TestSessionAuthRejectsInvalidCredentials(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultSessionConfig(RoleServer)
	config.Auth.Required = true
	config.Auth.Handler = func(ctx context.Context, req AuthRequest) (AuthResult, error) {
		return AuthResult{OK: false, RejectCode: AuthRejectUnauthorized, Reason: "bad token"}, nil
	}
	session := NewSessionWithConfig(transport, config)
	frame := NewFrame(FrameAuth, SchemaAuth, EncodeAuthRequest(AuthRequest{
		Method:  AuthMethodBearer,
		Payload: []byte("bad"),
	}))

	if err := session.HandleAuth(context.Background(), frame); err != nil {
		t.Fatalf("handle auth reject: %v", err)
	}
	if session.State() != SessionAuthRejected {
		t.Fatalf("state = %s", session.State())
	}
	sent := requireFrameType(t, transport.frames(), FrameAuthReject)
	reject, err := DecodeAuthReject(sent.Payload)
	if err != nil {
		t.Fatalf("decode auth reject: %v", err)
	}
	if reject.StatusCode != AuthRejectUnauthorized || reject.Message != "bad token" {
		t.Fatalf("reject = %+v", reject)
	}
}

func TestSessionAuthRejectsHandlerError(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultSessionConfig(RoleServer)
	config.Auth.Required = true
	wantErr := errors.New("storage down")
	config.Auth.Handler = func(ctx context.Context, req AuthRequest) (AuthResult, error) {
		return AuthResult{}, wantErr
	}
	session := NewSessionWithConfig(transport, config)
	frame := NewFrame(FrameAuth, SchemaAuth, EncodeAuthRequest(AuthRequest{Payload: []byte("token")}))

	if err := session.HandleAuth(context.Background(), frame); !errors.Is(err, wantErr) {
		t.Fatalf("error = %v", err)
	}
	if session.State() != SessionAuthRejected {
		t.Fatalf("state = %s", session.State())
	}
	rejectFrame := requireFrameType(t, transport.frames(), FrameAuthReject)
	reject, decodeErr := DecodeAuthReject(rejectFrame.Payload)
	if decodeErr != nil || reject.Message != "authentication failed" {
		t.Fatalf("auth handler error leaked to peer: %+v err=%v", reject, decodeErr)
	}
}

func TestSessionAuthRejectsHandlerPanic(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultServerConfig()
	config.Auth.Required = true
	config.Auth.Handler = func(context.Context, AuthRequest) (AuthResult, error) {
		panic("auth backend failed")
	}
	session := NewSessionWithConfig(transport, config)
	frame := NewFrame(FrameAuth, SchemaAuth, EncodeAuthRequest(AuthRequest{Payload: []byte("token")}))
	if err := session.HandleAuth(context.Background(), frame); err == nil {
		t.Fatal("auth handler panic escaped as success")
	}
	if session.State() != SessionAuthRejected {
		t.Fatalf("state = %s", session.State())
	}
	reject := requireFrameType(t, transport.frames(), FrameAuthReject)
	message, err := DecodeAuthReject(reject.Payload)
	if err != nil || message.StatusCode != AuthRejectUnauthorized {
		t.Fatalf("auth panic reject = %+v err=%v", message, err)
	}
	if message.Message != "authentication failed" {
		t.Fatalf("auth panic details leaked to peer: %q", message.Message)
	}
}

func TestSessionAuthRejectsMalformedOrOversizedPayload(t *testing.T) {
	t.Run("malformed", func(t *testing.T) {
		transport := newRecordingTransport(t)
		config := DefaultSessionConfig(RoleServer)
		config.Auth.Required = true
		session := NewSessionWithConfig(transport, config)
		err := session.HandleAuth(context.Background(), NewFrame(FrameAuth, SchemaAuth, []byte{1, 2, 3}))
		if err == nil {
			t.Fatalf("expected malformed auth error")
		}
		requireFrameType(t, transport.frames(), FrameAuthReject)
	})

	t.Run("oversized", func(t *testing.T) {
		transport := newRecordingTransport(t)
		config := DefaultSessionConfig(RoleServer)
		config.Auth.Required = true
		config.Auth.MaxPayloadBytes = 4
		called := false
		config.Auth.Handler = func(ctx context.Context, req AuthRequest) (AuthResult, error) {
			called = true
			return AuthResult{OK: true}, nil
		}
		session := NewSessionWithConfig(transport, config)
		err := session.HandleAuth(context.Background(), NewFrame(FrameAuth, SchemaAuth, EncodeAuthRequest(AuthRequest{Payload: []byte("too-large")})))
		if err != nil {
			t.Fatalf("oversized auth reject send: %v", err)
		}
		if called {
			t.Fatalf("auth handler was called for oversized payload")
		}
		sent := requireFrameType(t, transport.frames(), FrameAuthReject)
		reject, err := DecodeAuthReject(sent.Payload)
		if err != nil {
			t.Fatalf("decode auth reject: %v", err)
		}
		if reject.StatusCode != AuthRejectTooLarge {
			t.Fatalf("reject status = %d", reject.StatusCode)
		}
	})
}

func TestSessionClientHandlesAuthResultFrames(t *testing.T) {
	t.Run("accept", func(t *testing.T) {
		session := NewSession(newRecordingTransport(t))
		frame := NewFrame(FrameAuthAccept, SchemaAuthResult, EncodeAuthAccept(AuthAccept{UserID: "client-user"}))
		if err := session.HandleAuthAccept(frame); err != nil {
			t.Fatalf("handle accept: %v", err)
		}
		if session.State() != SessionAuthAccepted || session.Identity().UserID != "client-user" {
			t.Fatalf("state=%s identity=%+v", session.State(), session.Identity())
		}
	})

	t.Run("reject", func(t *testing.T) {
		session := NewSession(newRecordingTransport(t))
		frame := NewFrame(FrameAuthReject, SchemaAuthResult, EncodeAuthReject(AuthReject{StatusCode: AuthRejectUnauthorized, Message: "no"}))
		if err := session.HandleAuthReject(frame); err == nil {
			t.Fatalf("expected reject error")
		}
		if session.State() != SessionAuthRejected {
			t.Fatalf("state = %s", session.State())
		}
	})
}

func TestSessionAuthRequiredBlocksEarlyApplicationFrames(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultSessionConfig(RoleClient)
	config.Auth.Required = true
	session := NewSessionWithConfig(transport, config)

	if err := session.SendText("too early"); err == nil {
		t.Fatalf("expected auth required error")
	}
	if len(transport.frames()) != 0 {
		t.Fatalf("unexpected frame sent before auth")
	}

	if err := session.HandleAuthAccept(NewFrame(FrameAuthAccept, SchemaAuthResult, EncodeAuthAccept(AuthAccept{UserID: "ok"}))); err != nil {
		t.Fatalf("handle accept: %v", err)
	}
	if err := session.SendHello(RoleClient); err != nil {
		t.Fatalf("send hello after auth: %v", err)
	}
	helloAck := NewFrame(FrameHelloAck, SchemaHello, EncodeHelloMessage(validHello(RoleServer)))
	if err := session.HandleFrame(context.Background(), helloAck); err != nil {
		t.Fatalf("handle hello ack: %v", err)
	}
	if err := session.SendText("after auth"); err != nil {
		t.Fatalf("send text after auth: %v", err)
	}
	requireFrameType(t, transport.frames(), FrameHello)
	requireFrameType(t, transport.frames(), FrameData)
}

func TestSessionAuthTimeoutEmitsEvent(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultSessionConfig(RoleServer)
	config.Auth.Required = true
	config.Auth.Timeout = 10 * time.Millisecond
	session := NewSessionWithConfig(transport, config)
	events := make(chan ProtocolEvent, 1)
	session.OnProtocolEvent(func(e ProtocolEvent) {
		events <- e
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session.StartAuthTimeout(ctx)

	select {
	case event := <-events:
		if event.Code != EventAuthTimeout {
			t.Fatalf("event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatalf("auth timeout event was not emitted")
	}
	if session.State() != SessionAuthRejected {
		t.Fatalf("state = %s", session.State())
	}
	if !transport.isClosed() {
		t.Fatalf("transport was not closed after auth timeout")
	}
}

func TestSessionAuthTimeoutDoesNotRejectAfterAuthAndHello(t *testing.T) {
	transport := newRecordingTransport(t)
	config := DefaultSessionConfig(RoleClient)
	config.Auth.Required = true
	config.Auth.Timeout = 20 * time.Millisecond
	session := NewSessionWithConfig(transport, config)
	events := make(chan ProtocolEvent, 1)
	session.OnProtocolEvent(func(e ProtocolEvent) {
		events <- e
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session.StartAuthTimeout(ctx)

	if err := session.HandleAuthAccept(NewFrame(FrameAuthAccept, SchemaAuthResult, EncodeAuthAccept(AuthAccept{UserID: "ok"}))); err != nil {
		t.Fatalf("handle auth accept: %v", err)
	}
	if err := session.SendHello(RoleClient); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if session.State() != SessionHelloSent {
		t.Fatalf("state = %s", session.State())
	}
	if transport.isClosed() {
		t.Fatalf("transport was closed after successful auth")
	}
	select {
	case event := <-events:
		if event.Code == EventAuthTimeout {
			t.Fatalf("unexpected auth timeout event after auth success")
		}
	default:
	}
}

func TestSessionAuthRequiredRejectsInboundApplicationFrame(t *testing.T) {
	config := DefaultSessionConfig(RoleServer)
	config.Auth.Required = true
	session := NewSessionWithConfig(newRecordingTransport(t), config)
	events := make(chan ProtocolEvent, 1)
	session.OnProtocolEvent(func(e ProtocolEvent) {
		events <- e
	})

	frame := NewFrame(FrameHello, SchemaHello, EncodeHello(RoleClient))
	if err := session.EnsureAuthenticatedFor(frame); err == nil {
		t.Fatalf("expected inbound hello before auth to be rejected")
	}

	select {
	case event := <-events:
		if event.Code != EventAuthRequired || event.FrameType != FrameHello {
			t.Fatalf("event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatalf("auth required event was not emitted")
	}
}
