package benchmarks

import (
	"testing"

	. "github.com/elum-utils/go-etp/internal/etp"
)

func BenchmarkFrameEncodeSmallPayload(b *testing.B) {
	payload := []byte("hello")
	frame := NewFrame(FrameData, SchemaTextMessage, payload)
	frame.Header.TransferID = 42
	frame.Header.ChunkID = 7

	b.ReportAllocs()
	b.SetBytes(int64(HeaderSize + len(payload)))
	for i := 0; i < b.N; i++ {
		out, err := EncodeFrame(frame)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("empty frame")
		}
	}
}

func BenchmarkFrameEncodeIntoSmallPayload(b *testing.B) {
	payload := []byte("hello")
	frame := NewFrame(FrameData, SchemaTextMessage, payload)
	buf := make([]byte, 0, EncodedFrameSize(frame))

	b.ReportAllocs()
	b.SetBytes(int64(HeaderSize + len(payload)))
	for i := 0; i < b.N; i++ {
		out, err := EncodeFrameInto(buf[:0], frame)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("empty frame")
		}
	}
}

func BenchmarkFrameEncodeChunkPayload(b *testing.B) {
	payload := make([]byte, 16*1024)
	frame := NewFrame(FrameData, 0, payload)
	frame.Header.TransferID = 42
	frame.Header.ChunkID = 7

	b.ReportAllocs()
	b.SetBytes(int64(HeaderSize + len(payload)))
	for i := 0; i < b.N; i++ {
		out, err := EncodeFrame(frame)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("empty frame")
		}
	}
}

func BenchmarkFrameEncodeIntoChunkPayload(b *testing.B) {
	payload := make([]byte, 16*1024)
	frame := NewFrame(FrameData, 0, payload)
	frame.Header.TransferID = 42
	frame.Header.ChunkID = 7
	buf := make([]byte, 0, EncodedFrameSize(frame))

	b.ReportAllocs()
	b.SetBytes(int64(HeaderSize + len(payload)))
	for i := 0; i < b.N; i++ {
		out, err := EncodeFrameInto(buf[:0], frame)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("empty frame")
		}
	}
}

func BenchmarkFrameDecodeSmallPayload(b *testing.B) {
	encoded, err := EncodeFrame(NewFrame(FrameData, SchemaTextMessage, []byte("hello")))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for i := 0; i < b.N; i++ {
		frame, err := DecodeFrame(encoded)
		if err != nil {
			b.Fatal(err)
		}
		if len(frame.Payload) == 0 {
			b.Fatal("empty payload")
		}
	}
}

func BenchmarkFrameDecodeViewSmallPayload(b *testing.B) {
	encoded, err := EncodeFrame(NewFrame(FrameData, SchemaTextMessage, []byte("hello")))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for i := 0; i < b.N; i++ {
		frame, err := DecodeFrameView(encoded)
		if err != nil {
			b.Fatal(err)
		}
		if len(frame.Payload) == 0 {
			b.Fatal("empty payload")
		}
	}
}

func BenchmarkFrameDecodeChunkPayload(b *testing.B) {
	encoded, err := EncodeFrame(NewFrame(FrameData, 0, make([]byte, 16*1024)))
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for i := 0; i < b.N; i++ {
		frame, err := DecodeFrame(encoded)
		if err != nil {
			b.Fatal(err)
		}
		if len(frame.Payload) == 0 {
			b.Fatal("empty payload")
		}
	}
}
