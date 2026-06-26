package tests

import (
	"encoding/hex"
	. "github.com/elum-utils/go-etp"
	"testing"
)

func TestGoldenEventMessageEncoding(t *testing.T) {
	encoded, err := EncodeEventMessageInto(nil, []byte("message.get"), []byte(`{"id":42}`))
	if err != nil {
		t.Fatalf("encode event: %v", err)
	}
	const wantHex = "0000000b6d6573736167652e676574000000097b226964223a34327d"
	if got := hex.EncodeToString(encoded); got != wantHex {
		t.Fatalf("event hex = %s want %s", got, wantHex)
	}
}

func TestGoldenAckEncoding(t *testing.T) {
	encoded := EncodeAck(Ack{TransferID: 7, ChunkFrom: 1, ChunkTo: 3, ReceivedBytes: 4096})
	const wantHex = "000000000000000700000001000000030000000000001000"
	if got := hex.EncodeToString(encoded); got != wantHex {
		t.Fatalf("ack hex = %s want %s", got, wantHex)
	}
}

func TestGoldenWindowEncoding(t *testing.T) {
	encoded := EncodeWindow(Window{TransferID: 7, WindowBytes: 65536, WindowChunks: 4, Flags: WindowFlagTransfer})
	const wantHex = "000000000000000700000000000100000000000400020000"
	if got := hex.EncodeToString(encoded); got != wantHex {
		t.Fatalf("window hex = %s want %s", got, wantHex)
	}
}
