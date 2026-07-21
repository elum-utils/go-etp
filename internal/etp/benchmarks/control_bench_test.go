package benchmarks

import (
	"context"
	"testing"

	. "github.com/elum-utils/go-etp/internal/etp"
)

type benchResumeStore struct {
	decision TransferResumeDecision
}

func (s benchResumeStore) ResumeIncoming(context.Context, TransferResumeView) (TransferResumeDecision, error) {
	return s.decision, nil
}

func BenchmarkFrameSchedulerPushPop(b *testing.B) {
	scheduler := NewFrameScheduler()
	frame := NewFrame(FrameData, 0, []byte("payload"))
	frame.Header.Priority = PriorityHigh

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = scheduler.PushPop(frame)
	}
}

func BenchmarkSessionHandleWindow(b *testing.B) {
	session := NewSession(benchTransport{})
	done := make(chan struct{})
	defer close(done)
	session.StartTransfer(context.Background(), TransferOptions{
		Name:        "blocked.bin",
		ContentType: ContentFile,
		Reader:      benchGateReader{done: done},
		TotalSize:   1,
	})
	frame := NewFrame(FrameWindow, SchemaWindow, EncodeWindow(Window{
		TransferID:   7001,
		WindowBytes:  1024,
		WindowChunks: 1,
		Flags:        WindowFlagTransfer,
	}))

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := session.HandleWindow(frame); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSessionHandleFrameRateLimit(b *testing.B) {
	config := DefaultSessionConfig(RoleServer)
	config.RateLimit.MaxFramesPerSecond = b.N + 1
	config.RateLimit.MaxBytesPerSecond = ^uint64(0)
	session := NewSessionWithConfig(benchTransport{}, config)
	frame := NewFrame(FramePong, 0, nil)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := session.HandleFrame(context.Background(), frame); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSessionHandleInvalidFrame(b *testing.B) {
	session := NewSessionWithConfig(benchTransport{}, DefaultSessionConfig(RoleServer))
	frame := NewFrame(FrameRequest, SchemaEvent, EncodeEventMessage(EventMessage{Event: "bad"}))

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = session.HandleFrame(context.Background(), frame)
	}
}

func BenchmarkSessionTransferResumeDuplicateRejected(b *testing.B) {
	config := DefaultSessionConfig(RoleServer)
	config.Capabilities |= CapabilityTransferResume
	config.RateLimit.MaxFramesPerSecond = b.N + 1
	config.RateLimit.MaxBytesPerSecond = ^uint64(0)
	config.Resume.Store = benchResumeStore{decision: TransferResumeDecision{
		Accepted:      true,
		Meta:          TransferBegin{TotalSize: 4, ChunkSize: 4, ChunkCount: 1},
		Writer:        discardIncomingWriterForBench{},
		ReceivedBytes: 0,
		NextChunk:     0,
	}}
	session := NewSessionWithConfig(benchResumeTransport{}, config)
	frame := NewFrame(FrameTransferResume, SchemaTransferState, EncodeTransferResume(TransferResume{
		TransferID: 1,
	}))
	frame.Header.TransferID = 1
	if err := session.HandleFrame(context.Background(), frame); err != nil {
		b.Fatalf("seed accepted resume: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := session.HandleFrame(context.Background(), frame); err != nil {
			b.Fatal(err)
		}
	}
}

type benchResumeTransport struct{ benchTransport }

func (benchResumeTransport) NegotiatedCapabilities() uint64 {
	return DefaultCapabilities | CapabilityTransferResume
}

type benchGateReader struct {
	done <-chan struct{}
}

func (r benchGateReader) Read([]byte) (int, error) {
	<-r.done
	return 0, context.Canceled
}

type discardIncomingWriterForBench struct{}

func (discardIncomingWriterForBench) Write(p []byte) (int, error) { return len(p), nil }
func (discardIncomingWriterForBench) Close() error                { return nil }
