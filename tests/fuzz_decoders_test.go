package tests

import (
	. "github.com/elum-utils/go-etp"
	"testing"
)

func FuzzDecodeFrameView(f *testing.F) {
	encoded, _ := EncodeFrame(NewFrame(FrameData, SchemaTextMessage, EncodeTextMessage("hello")))
	f.Add(encoded)
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeFrameView(data)
	})
}

func FuzzDecodeEventMessageView(f *testing.F) {
	encoded := EncodeEventMessage(EventMessage{Event: "message.get", Data: []byte(`{"id":42}`)})
	f.Add(encoded)
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeEventMessageView(data)
	})
}

func FuzzDecodeTransferBegin(f *testing.F) {
	f.Add(EncodeTransferBegin(TransferBegin{Name: "file.txt", TotalSize: 10, ChunkSize: 10, ChunkCount: 1}))
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeTransferBegin(data)
	})
}

func FuzzDecodeControlPayloads(f *testing.F) {
	f.Add(EncodeAck(Ack{TransferID: 1, ChunkFrom: 0, ChunkTo: 0, ReceivedBytes: 1}))
	f.Add(EncodeNack(Nack{TransferID: 1, ReasonCode: NackMissingChunk}))
	f.Add(EncodeCancel(Cancel{TransferID: 1, ReasonCode: CancelUser}))
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeAck(data)
		_, _ = DecodeNack(data)
		_, _ = DecodeCancel(data)
		_, _, _ = DecodeCancelAck(data)
	})
}
