package benchmarks

import (
	"encoding/binary"
	"io"
	"net"
	"testing"

	. "github.com/elum-utils/go-etp/internal/etp"
)

func BenchmarkRawPipeFraming(b *testing.B) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	payload := make([]byte, 16*1024)
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(payload)))
	done := make(chan struct{})
	go func() {
		defer close(done)
		var readPrefix [4]byte
		frame := make([]byte, len(payload))
		for i := 0; i < b.N; i++ {
			if _, err := io.ReadFull(right, readPrefix[:1]); err != nil {
				b.Errorf("read first length byte: %v", err)
				return
			}
			if _, err := io.ReadFull(right, readPrefix[1:]); err != nil {
				b.Errorf("read remaining length bytes: %v", err)
				return
			}
			if _, err := io.ReadFull(right, frame); err != nil {
				b.Errorf("read frame: %v", err)
				return
			}
		}
	}()

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := left.Write(prefix[:]); err != nil {
			b.Fatal(err)
		}
		if _, err := left.Write(payload); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	<-done
}

func BenchmarkStreamTransportSendReadFrame(b *testing.B) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	sender := NewStreamTransportForStream(left, SlowlorisConfig{DisableDeadlines: true})
	receiver := NewStreamTransportForStream(right, SlowlorisConfig{DisableDeadlines: true})
	payload := make([]byte, 16*1024)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < b.N; i++ {
			frame, err := receiver.ReadFrame()
			if err != nil {
				b.Errorf("read frame: %v", err)
				return
			}
			if len(frame) != len(payload) {
				b.Errorf("frame size = %d", len(frame))
				return
			}
		}
	}()

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sender.SendFrame(payload); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	<-done
}

func BenchmarkStreamTransportSendReadFrameInto(b *testing.B) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()

	sender := NewStreamTransportForStream(left, SlowlorisConfig{DisableDeadlines: true})
	receiver := NewStreamTransportForStream(right, SlowlorisConfig{DisableDeadlines: true})
	payload := make([]byte, 16*1024)

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 0, len(payload))
		for i := 0; i < b.N; i++ {
			frame, err := receiver.ReadFrameInto(buf)
			if err != nil {
				b.Errorf("read frame: %v", err)
				return
			}
			buf = frame[:0]
			if len(frame) != len(payload) {
				b.Errorf("frame size = %d", len(frame))
				return
			}
		}
	}()

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sender.SendFrame(payload); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	<-done
}
