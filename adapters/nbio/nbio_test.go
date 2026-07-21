package nbio

import (
	"errors"
	"sync"
	"testing"

	etp "github.com/elum-utils/go-etp"
)

func newQueueTransport(queueSize int) *Transport {
	t := &Transport{frames: make(chan *queuedFrame, queueSize)}
	t.maxFrame.Store(etp.MaxFrameBytes)
	return t
}

func TestTransportEnqueueLease(t *testing.T) {
	transport := newQueueTransport(1)
	transport.enqueue([]byte("frame"))
	lease, err := transport.ReadFrameLease()
	if err != nil {
		t.Fatal(err)
	}
	if string(lease.Data) != "frame" {
		t.Fatalf("frame = %q", lease.Data)
	}
	lease.Release()
}

func TestTransportClosesWhenQueueIsFull(t *testing.T) {
	transport := newQueueTransport(1)
	transport.enqueue([]byte("first"))
	transport.enqueue([]byte("second"))

	lease, err := transport.ReadFrameLease()
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
	if _, err := transport.ReadFrameLease(); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("close error = %v", err)
	}
}

func TestTransportRejectsOversizedFrame(t *testing.T) {
	transport := newQueueTransport(1)
	transport.SetMaxFrameBytes(etp.HeaderSize)
	transport.enqueue(make([]byte, etp.HeaderSize+1))
	if _, err := transport.ReadFrameLease(); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("close error = %v", err)
	}
}

func TestTransportConcurrentCloseAndEnqueue(t *testing.T) {
	transport := newQueueTransport(64)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				transport.enqueue([]byte("frame"))
			}
		}()
	}
	transport.close(ErrClosed)
	wg.Wait()
	for frame := range transport.frames {
		frame.ReleaseFrameLease(nil)
	}
}

func BenchmarkTransportEnqueueReadLease1KiB(b *testing.B) {
	transport := newQueueTransport(1)
	data := make([]byte, 1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		transport.enqueue(data)
		lease, err := transport.ReadFrameLease()
		if err != nil {
			b.Fatal(err)
		}
		lease.Release()
	}
}
