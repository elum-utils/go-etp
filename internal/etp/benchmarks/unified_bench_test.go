package benchmarks

import (
	"context"
	"testing"
	"time"

	. "github.com/elum-utils/go-etp/internal/etp"
)

func BenchmarkSessionUnifiedSmallRequest(b *testing.B) {
	config := DefaultSessionConfig(RoleClient)
	config.Payload.MaxInlineBodyBytes = 1024
	session := NewSessionWithConfig(benchTransport{}, config)
	opts := MessageOptions{
		Event: "attach.upload",
		Data:  []byte("hello"),
		Fields: []TransferField{
			{Key: "dialog", Value: "dialog-1"},
		},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := session.Request(context.Background(), opts); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSessionUnifiedLargeRequest32KiB(b *testing.B) {
	transport := &benchAckTransport{}
	config := DefaultSessionConfig(RoleClient)
	config.Payload.MaxInlineBodyBytes = 1024
	config.FlowControl.MaxConcurrentTransfers = 1
	config.FlowControl.MaxInFlightChunks = 64
	config.FlowControl.MaxInFlightBytes = 1 << 20
	config.FlowControl.MaxSendBufferBytes = 1 << 20
	config.FlowControl.MaxTransferBytes = 1 << 20
	config.FlowControl.MaxChunkSize = 16 * 1024
	config.FlowControl.AckTimeout = time.Second
	config.FlowControl.RetryLimit = 1
	config.RateLimit.MaxFramesPerSecond = int(^uint(0) >> 1)
	config.RateLimit.MaxBytesPerSecond = ^uint64(0)
	session := NewSessionWithConfig(transport, config)
	transport.session = session
	opts := MessageOptions{
		Event: "message.send",
		Data:  make([]byte, 32*1024),
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(opts.Data)))
	for i := 0; i < b.N; i++ {
		handle, err := session.Request(context.Background(), opts)
		if err != nil {
			b.Fatal(err)
		}
		if err := <-handle.Done(); err != nil {
			b.Fatal(err)
		}
	}
}
