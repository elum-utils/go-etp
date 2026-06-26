package tests

import (
	. "github.com/elum-utils/go-etp"
	"reflect"
	"testing"
)

func TestTransferBeginPayloadRoundTrip(t *testing.T) {
	begin := TransferBegin{
		TotalSize:   1234,
		ChunkSize:   4096,
		ChunkCount:  1,
		ContentType: ContentFile,
		Flags:       TransferFlagChecksumSHA256,
		Name:        "file.bin",
		Event:       "attach.upload",
		Field:       "files",
		Index:       2,
		Parts: []TransferPart{
			{Field: "files", Index: 0, Name: "one.bin", TotalSize: 10, ContentType: ContentFile},
			{Field: "files", Index: 1, Name: "two.bin", TotalSize: 20, ContentType: ContentMedia},
		},
		Fields: []TransferField{
			{Key: "messageID", Value: "m-1"},
			{Key: "caption", Value: "hello"},
		},
	}
	begin.Checksum[0] = 7

	decoded, err := DecodeTransferBegin(EncodeTransferBegin(begin))
	if err != nil {
		t.Fatalf("decode transfer begin: %v", err)
	}
	if !reflect.DeepEqual(decoded, begin) {
		t.Fatalf("transfer begin mismatch: %+v", decoded)
	}
}

func TestTransferBeginPayloadRejectsInvalidData(t *testing.T) {
	if _, err := DecodeTransferBegin([]byte{1, 2, 3}); err == nil {
		t.Fatalf("expected short transfer begin error")
	}

	payload := EncodeTransferBegin(TransferBegin{Name: "file.bin"})
	payload[56] = 0xff
	payload[57] = 0xff
	payload[58] = 0xff
	payload[59] = 0xff
	if _, err := DecodeTransferBegin(payload); err == nil {
		t.Fatalf("expected invalid name length error")
	}
}
