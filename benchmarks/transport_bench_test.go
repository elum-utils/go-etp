package benchmarks

import (
	"net"
	"testing"

	. "github.com/elum-utils/go-etp"
)

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
