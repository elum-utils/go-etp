package tests

import (
	"encoding/binary"
	"testing"

	. "github.com/elum-utils/go-etp/internal/etp"
)

func TestPayloadDecodersRejectOverflowingLengths(t *testing.T) {
	maxLength := ^uint32(0)
	tests := []struct {
		name   string
		decode func([]byte) error
		data   []byte
	}{
		{name: "hello role", data: lengthPayload(40, 36, maxLength), decode: decodeHelloError},
		{name: "text", data: lengthPayload(4, 0, maxLength), decode: decodeTextError},
		{name: "event name", data: lengthPayload(8, 0, maxLength), decode: decodeEventError},
		{name: "error message", data: lengthPayload(32, 28, maxLength), decode: decodeErrorMessageError},
		{name: "goaway message", data: lengthPayload(32, 28, maxLength), decode: decodeGoAwayError},
		{name: "resume token", data: lengthPayload(24, 20, maxLength), decode: decodeResumeError},
		{name: "auth request", data: lengthPayload(12, 8, maxLength), decode: decodeAuthRequestError},
		{name: "auth accept", data: lengthPayload(4, 0, maxLength), decode: decodeAuthAcceptError},
		{name: "auth reject", data: lengthPayload(8, 4, maxLength), decode: decodeAuthRejectError},
		{name: "transfer name", data: lengthPayload(72, 56, maxLength), decode: decodeTransferBeginError},
		{name: "transfer part field", data: transferPartWithLength(maxLength), decode: decodeTransferBeginError},
		{name: "event field key", data: eventFieldWithLength(maxLength), decode: decodeEventError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.decode(tt.data); err == nil {
				t.Fatal("decoder accepted overflowing length")
			}
		})
	}
}

func lengthPayload(size int, offset int, length uint32) []byte {
	payload := make([]byte, size)
	binary.BigEndian.PutUint32(payload[offset:offset+4], length)
	return payload
}

func transferPartWithLength(length uint32) []byte {
	payload := make([]byte, 80)
	binary.BigEndian.PutUint32(payload[72:76], 1)
	binary.BigEndian.PutUint32(payload[76:80], length)
	return payload
}

func eventFieldWithLength(length uint32) []byte {
	payload := make([]byte, 16)
	binary.BigEndian.PutUint32(payload[8:12], 1)
	binary.BigEndian.PutUint32(payload[12:16], length)
	return payload
}

func decodeHelloError(data []byte) error {
	_, err := DecodeHelloMessage(data)
	return err
}

func decodeTextError(data []byte) error {
	_, err := DecodeTextMessage(data)
	return err
}

func decodeEventError(data []byte) error {
	_, err := DecodeEventMessageView(data)
	return err
}

func decodeErrorMessageError(data []byte) error {
	_, err := DecodeErrorMessageView(data)
	return err
}

func decodeGoAwayError(data []byte) error {
	_, err := DecodeGoAwayView(data)
	return err
}

func decodeResumeError(data []byte) error {
	_, err := DecodeTransferResumeView(data)
	return err
}

func decodeAuthRequestError(data []byte) error {
	_, err := DecodeAuthRequestView(data)
	return err
}

func decodeAuthAcceptError(data []byte) error {
	_, err := DecodeAuthAcceptView(data)
	return err
}

func decodeAuthRejectError(data []byte) error {
	_, err := DecodeAuthRejectView(data)
	return err
}

func decodeTransferBeginError(data []byte) error {
	_, err := DecodeTransferBegin(data)
	return err
}
