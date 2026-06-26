package benchmarks

import (
	"testing"

	. "github.com/elum-utils/go-etp"
)

func BenchmarkErrorMessageEncodeIntoDecodeView(b *testing.B) {
	message := []byte("bad request")
	buf := make([]byte, 0, ErrorMessagePayloadSize(message))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, err := EncodeErrorMessageInto(buf[:0], ErrorInvalidRequest, FrameRequest, SchemaEvent, 42, 0, message)
		if err != nil {
			b.Fatal(err)
		}
		decoded, err := DecodeErrorMessageView(encoded)
		if err != nil {
			b.Fatal(err)
		}
		if decoded.Code != ErrorInvalidRequest {
			b.Fatal(decoded)
		}
	}
}

func BenchmarkGoAwayEncodeIntoDecodeView(b *testing.B) {
	message := []byte("restart")
	buf := make([]byte, 0, GoAwayPayloadSize(message))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, err := EncodeGoAwayInto(buf[:0], CloseServerShutdown, CloseFlagDrain, 1000, 42, 77, message)
		if err != nil {
			b.Fatal(err)
		}
		decoded, err := DecodeGoAwayView(encoded)
		if err != nil {
			b.Fatal(err)
		}
		if decoded.ReasonCode != CloseServerShutdown {
			b.Fatal(decoded)
		}
	}
}
