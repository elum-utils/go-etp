package tests

import (
	. "github.com/elum-utils/go-etp"
	"testing"
)

func TestAckNackCancelPayloadRoundTrips(t *testing.T) {
	ack := Ack{TransferID: 55, ChunkFrom: 1, ChunkTo: 3, ReceivedBytes: 4096}
	decodedAck, err := DecodeAck(EncodeAck(ack))
	if err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if decodedAck != ack {
		t.Fatalf("ack mismatch: %+v", decodedAck)
	}

	nack := Nack{TransferID: 55, ChunkFrom: 2, ChunkTo: 4, ReasonCode: NackMissingChunk}
	decodedNack, err := DecodeNack(EncodeNack(nack))
	if err != nil {
		t.Fatalf("decode nack: %v", err)
	}
	if decodedNack != nack {
		t.Fatalf("nack mismatch: %+v", decodedNack)
	}

	cancel := Cancel{TransferID: 55, ReasonCode: CancelUser, Flags: CancelDeletePartial}
	decodedCancel, err := DecodeCancel(EncodeCancel(cancel))
	if err != nil {
		t.Fatalf("decode cancel: %v", err)
	}
	if decodedCancel != cancel {
		t.Fatalf("cancel mismatch: %+v", decodedCancel)
	}

	transferID, status, err := DecodeCancelAck(EncodeCancelAck(55, CancelAckOK))
	if err != nil {
		t.Fatalf("decode cancel ack: %v", err)
	}
	if transferID != 55 || status != CancelAckOK {
		t.Fatalf("cancel ack mismatch: transfer=%d status=%d", transferID, status)
	}
}

func TestControlPayloadsRejectInvalidData(t *testing.T) {
	if _, err := DecodeAck([]byte{1}); err == nil {
		t.Fatalf("expected ack error")
	}
	if _, err := DecodeNack([]byte{1}); err == nil {
		t.Fatalf("expected nack error")
	}
	if _, err := DecodeCancel([]byte{1}); err == nil {
		t.Fatalf("expected cancel error")
	}
	if _, _, err := DecodeCancelAck([]byte{1}); err == nil {
		t.Fatalf("expected cancel ack error")
	}
}
