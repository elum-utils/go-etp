package etp

import (
	"bytes"
	"context"
	"testing"

	protocol "github.com/elum-utils/go-etp/internal/etp"
)

func BenchmarkAppDispatchInline(b *testing.B) {
	app := New(Config{})
	if err := app.On("message.ping", func(*Context) error { return nil }); err != nil {
		b.Fatal(err)
	}
	app.Compile()
	peer := &Peer{app: app}
	frame := protocol.NewFrame(protocol.FrameRequest, protocol.SchemaEvent, nil)
	frame.Header.RequestID = 42
	message := protocol.EventMessageView{
		Event: []byte("message.ping"),
		Data:  []byte("hello"),
	}
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		if err := peer.handleRequest(ctx, frame, message); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppDispatchTransferMemory64KiB(b *testing.B) {
	app := New(Config{MaxMemoryBody: 128 << 10})
	if err := app.On("attach.upload", func(*Context) error { return nil }); err != nil {
		b.Fatal(err)
	}
	app.Compile()
	peer := &Peer{app: app}
	info := protocol.IncomingTransferInfo{
		TransferID: 1,
		RequestID:  42,
		Meta: protocol.TransferBegin{
			Event:     "attach.upload",
			TotalSize: 64 << 10,
		},
	}
	body := bytes.Repeat([]byte("x"), 64<<10)
	ctx := context.Background()

	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	for b.Loop() {
		writer, err := peer.handleTransfer(ctx, info)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := writer.Write(body); err != nil {
			b.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
