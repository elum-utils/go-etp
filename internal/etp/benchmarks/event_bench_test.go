package benchmarks

import (
	"testing"

	. "github.com/elum-utils/go-etp/internal/etp"
)

func BenchmarkEventMessageEncodeIntoDecodeView(b *testing.B) {
	event := []byte("message.get")
	data := []byte(`{"id":42}`)
	buf := make([]byte, 0, EventMessagePayloadSize(event, data, nil))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, err := EncodeEventMessageInto(buf[:0], event, data)
		if err != nil {
			b.Fatal(err)
		}
		decoded, err := DecodeEventMessageView(encoded)
		if err != nil {
			b.Fatal(err)
		}
		if len(decoded.Event) == 0 {
			b.Fatal(decoded)
		}
	}
}
