package benchmarks

import (
	"testing"

	. "github.com/elum-utils/go-etp"
)

func BenchmarkAuthRequestEncodeIntoDecodeView(b *testing.B) {
	req := AuthRequest{
		Method:       AuthMethodBearer,
		AuthSchemaID: SchemaAuth,
		Payload:      []byte("bearer-token"),
	}
	buf := make([]byte, 0, AuthRequestPayloadSize(req))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, err := EncodeAuthRequestInto(buf[:0], req)
		if err != nil {
			b.Fatal(err)
		}
		decoded, err := DecodeAuthRequestView(encoded)
		if err != nil {
			b.Fatal(err)
		}
		if decoded.Method != AuthMethodBearer || len(decoded.Payload) == 0 {
			b.Fatal(decoded)
		}
	}
}

func BenchmarkAuthAcceptEncodeDecode(b *testing.B) {
	userID := []byte("user-42")
	buf := make([]byte, 0, AuthAcceptPayloadSize(userID))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, err := EncodeAuthAcceptInto(buf[:0], userID)
		if err != nil {
			b.Fatal(err)
		}
		decoded, err := DecodeAuthAcceptView(encoded)
		if err != nil {
			b.Fatal(err)
		}
		if len(decoded.UserID) == 0 {
			b.Fatal(decoded)
		}
	}
}

func BenchmarkAuthRejectEncodeDecode(b *testing.B) {
	message := []byte("unauthorized")
	buf := make([]byte, 0, AuthRejectPayloadSize(message))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, err := EncodeAuthRejectInto(buf[:0], AuthRejectUnauthorized, AuthRejectUnauthorized, message)
		if err != nil {
			b.Fatal(err)
		}
		decoded, err := DecodeAuthRejectView(encoded)
		if err != nil {
			b.Fatal(err)
		}
		if decoded.StatusCode != AuthRejectUnauthorized {
			b.Fatal(decoded)
		}
	}
}
