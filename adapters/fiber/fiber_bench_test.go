package fiber

import (
	"bytes"
	"fmt"
	"testing"

	etp "github.com/elum-utils/go-etp"
)

func BenchmarkReadETPFrameLease(b *testing.B) {
	for _, size := range []int{1 << 10, 8 << 10, 16 << 10, 32 << 10, 64 << 10} {
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) {
			frame, err := etp.EncodeFrame(etp.NewFrame(etp.FrameRequest, etp.SchemaEvent, make([]byte, size)))
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
		})
	}
}

func BenchmarkReadETPFrameLeaseStrict(b *testing.B) {
	frame, err := etp.EncodeFrame(etp.NewFrame(etp.FrameRequest, etp.SchemaEvent, make([]byte, 8<<10)))
	if err != nil {
		b.Fatal(err)
	}
	reader := bytes.NewReader(frame)

	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for b.Loop() {
		reader.Reset(frame)
		lease, err := readETPFrameLease(reader, true)
		if err != nil {
			b.Fatal(err)
		}
		lease.Release()
	}
}
