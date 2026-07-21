package gorilla

import (
	"bytes"
	"testing"

	etp "github.com/elum-utils/go-etp"
)

func BenchmarkReadETPFrameLease1KiB(b *testing.B) {
	frame, err := etp.EncodeFrame(etp.NewFrame(etp.FrameRequest, etp.SchemaEvent, make([]byte, 1024)))
	if err != nil {
		b.Fatal(err)
	}
	reader := bytes.NewReader(frame)

	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for b.Loop() {
		reader.Reset(frame)
		lease, err := readETPFrameLease(reader, false)
		if err != nil {
			b.Fatal(err)
		}
		lease.Release()
	}
}
