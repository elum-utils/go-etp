package tests

import (
	"bytes"
	"testing"

	. "github.com/elum-utils/go-etp"
)

func TestAuthRequestPayloadRoundTrip(t *testing.T) {
	token := []byte("token-123")
	req := AuthRequest{
		Method:       AuthMethodBearer,
		Flags:        7,
		AuthSchemaID: 99,
		Payload:      token,
	}
	buf := make([]byte, 0, AuthRequestPayloadSize(req))
	encoded, err := EncodeAuthRequestInto(buf, req)
	if err != nil {
		t.Fatalf("encode auth request: %v", err)
	}
	decoded, err := DecodeAuthRequestView(encoded)
	if err != nil {
		t.Fatalf("decode auth request: %v", err)
	}
	if decoded.Method != req.Method || decoded.Flags != req.Flags || decoded.AuthSchemaID != req.AuthSchemaID {
		t.Fatalf("decoded metadata = %+v", decoded)
	}
	if !bytes.Equal(decoded.Payload, token) {
		t.Fatalf("decoded payload = %q", decoded.Payload)
	}
	if len(decoded.Payload) > 0 && &decoded.Payload[0] != &encoded[12] {
		t.Fatalf("auth request decode copied payload")
	}
}

func TestAuthRequestPayloadRejectsInvalidData(t *testing.T) {
	if _, err := DecodeAuthRequestView([]byte{1, 2, 3}); err == nil {
		t.Fatalf("expected short auth request error")
	}
	encoded := EncodeAuthRequest(AuthRequest{Method: AuthMethodBearer, Payload: []byte("x")})
	encoded[11] = 2
	if _, err := DecodeAuthRequestView(encoded); err == nil {
		t.Fatalf("expected invalid auth request length error")
	}
}

func TestAuthResultPayloadRoundTrip(t *testing.T) {
	accept, err := DecodeAuthAccept(EncodeAuthAccept(AuthAccept{UserID: "user-1"}))
	if err != nil {
		t.Fatalf("decode auth accept: %v", err)
	}
	if accept.UserID != "user-1" {
		t.Fatalf("user id = %q", accept.UserID)
	}
	acceptEncoded := EncodeAuthAccept(AuthAccept{UserID: "user-1"})
	acceptView, err := DecodeAuthAcceptView(acceptEncoded)
	if err != nil {
		t.Fatalf("decode auth accept view: %v", err)
	}
	if len(acceptView.UserID) > 0 && &acceptView.UserID[0] != &acceptEncoded[4] {
		t.Fatalf("auth accept view copied user id")
	}

	reject, err := DecodeAuthReject(EncodeAuthReject(AuthReject{
		StatusCode: AuthRejectUnauthorized,
		ReasonCode: AuthRejectForbidden,
		Message:    "nope",
	}))
	if err != nil {
		t.Fatalf("decode auth reject: %v", err)
	}
	if reject.StatusCode != AuthRejectUnauthorized || reject.ReasonCode != AuthRejectForbidden || reject.Message != "nope" {
		t.Fatalf("reject = %+v", reject)
	}
	rejectEncoded := EncodeAuthReject(AuthReject{StatusCode: AuthRejectUnauthorized, Message: "nope"})
	rejectView, err := DecodeAuthRejectView(rejectEncoded)
	if err != nil {
		t.Fatalf("decode auth reject view: %v", err)
	}
	if len(rejectView.Message) > 0 && &rejectView.Message[0] != &rejectEncoded[8] {
		t.Fatalf("auth reject view copied message")
	}
}

func TestAuthResultPayloadRejectsInvalidData(t *testing.T) {
	if _, err := DecodeAuthAccept([]byte{1}); err == nil {
		t.Fatalf("expected short auth accept error")
	}
	if _, err := DecodeAuthReject([]byte{1}); err == nil {
		t.Fatalf("expected short auth reject error")
	}
	accept := EncodeAuthAccept(AuthAccept{UserID: "u"})
	accept[3] = 2
	if _, err := DecodeAuthAccept(accept); err == nil {
		t.Fatalf("expected invalid auth accept length error")
	}
	reject := EncodeAuthReject(AuthReject{StatusCode: AuthRejectUnauthorized, Message: "x"})
	reject[7] = 2
	if _, err := DecodeAuthReject(reject); err == nil {
		t.Fatalf("expected invalid auth reject length error")
	}
}
