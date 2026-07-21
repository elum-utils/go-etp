package benchmarks

import (
	"testing"

	. "github.com/elum-utils/go-etp/internal/etp"
)

func BenchmarkHelloEncodeDecode(b *testing.B) {
	hello := Hello{
		Role:              "client",
		Capabilities:      DefaultCapabilities,
		MaxFrameBytes:     MaxFrameBytes,
		MaxChunkSize:      64 * 1024,
		MaxTransferBytes:  512 << 20,
		MaxInFlightChunks: 16,
		HeartbeatMillis:   10_000,
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		payload := EncodeHelloMessage(hello)
		decoded, err := DecodeHelloMessage(payload)
		if err != nil {
			b.Fatal(err)
		}
		if decoded.Role == "" {
			b.Fatal("empty role")
		}
	}
}

func BenchmarkTransferBeginEncodeDecode(b *testing.B) {
	begin := TransferBegin{
		TotalSize:   100 << 20,
		ChunkSize:   16 * 1024,
		ChunkCount:  6400,
		ContentType: ContentFile,
		Flags:       TransferFlagChecksumSHA256,
		Name:        "video.mp4",
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		payload := EncodeTransferBegin(begin)
		decoded, err := DecodeTransferBegin(payload)
		if err != nil {
			b.Fatal(err)
		}
		if decoded.Name == "" {
			b.Fatal("empty name")
		}
	}
}

func BenchmarkAckEncodeDecode(b *testing.B) {
	ack := Ack{TransferID: 1, ChunkFrom: 0, ChunkTo: 63, ReceivedBytes: 1 << 20}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		payload := EncodeAck(ack)
		decoded, err := DecodeAck(payload)
		if err != nil {
			b.Fatal(err)
		}
		if decoded.ReceivedBytes == 0 {
			b.Fatal("empty ack")
		}
	}
}
