package tests

import (
	. "github.com/elum-utils/go-etp/internal/etp"
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

func FuzzDecodeSessionPayloads(f *testing.F) {
	f.Add(EncodeHelloMessage(validHello(RoleClient)))
	f.Add(EncodeErrorMessage(ErrorMessage{Code: ErrorInternal, Message: "error"}))
	f.Add(EncodeGoAway(GoAway{ReasonCode: CloseServerShutdown, Flags: CloseFlagDrain, Message: "drain"}))
	f.Add(EncodeCloseMessage(CloseMessage{ReasonCode: CloseNormal, Flags: CloseFlagImmediate}))
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeHelloMessage(data)
		_, _ = DecodeTextMessage(data)
		_, _ = DecodeErrorMessageView(data)
		_, _ = DecodeGoAwayView(data)
		_, _ = DecodeCloseMessage(data)
	})
}

func FuzzDecodeAuthPayloads(f *testing.F) {
	f.Add(EncodeAuthRequest(AuthRequest{Method: AuthMethodBearer, Payload: []byte("token")}))
	f.Add(EncodeAuthAccept(AuthAccept{UserID: "user"}))
	f.Add(EncodeAuthReject(AuthReject{StatusCode: AuthRejectUnauthorized, ReasonCode: AuthRejectUnauthorized, Message: "unauthorized"}))
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeAuthRequestView(data)
		_, _ = DecodeAuthAcceptView(data)
		_, _ = DecodeAuthRejectView(data)
	})
}

func FuzzDecodeTransferControlPayloads(f *testing.F) {
	f.Add(EncodeWindow(Window{TransferID: 1, WindowBytes: 1024, WindowChunks: 1, Flags: WindowFlagTransfer}))
	f.Add(EncodeTransferResume(TransferResume{TransferID: 1, ReceivedBytes: 4, NextChunk: 1, Token: []byte("token")}))
	f.Add(EncodeTransferStateMessage(TransferStateMessage{TransferID: 1, ReceivedBytes: 4, NextChunk: 1, Flags: TransferStateFlagCompleted}))
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeWindow(data)
		_, _ = DecodeTransferResumeView(data)
		_, _ = DecodeTransferStateMessage(data)
	})
}
