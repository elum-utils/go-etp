package tests

import (
	"testing"

	. "github.com/elum-utils/go-etp/internal/etp"
)

func TestErrorGoAwayClosePayloadRoundTrips(t *testing.T) {
	errPayload := EncodeErrorMessage(ErrorMessage{
		Code:       ErrorInvalidRequest,
		FrameType:  FrameRequest,
		SchemaID:   SchemaEvent,
		RequestID:  42,
		TransferID: 7,
		Message:    "bad request",
	})
	errMsg, err := DecodeErrorMessage(errPayload)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if errMsg.Code != ErrorInvalidRequest || errMsg.RequestID != 42 || errMsg.Message != "bad request" {
		t.Fatalf("error msg = %+v", errMsg)
	}
	errView, err := DecodeErrorMessageView(errPayload)
	if err != nil {
		t.Fatalf("decode error view: %v", err)
	}
	if len(errView.Message) > 0 && &errView.Message[0] != &errPayload[32] {
		t.Fatalf("error view copied message")
	}

	goAwayPayload := EncodeGoAway(GoAway{
		ReasonCode:             CloseServerShutdown,
		Flags:                  CloseFlagDrain,
		DrainTimeoutMillis:     1000,
		LastAcceptedRequestID:  10,
		LastAcceptedTransferID: 20,
		Message:                "restart",
	})
	goAway, err := DecodeGoAway(goAwayPayload)
	if err != nil {
		t.Fatalf("decode goaway: %v", err)
	}
	if goAway.ReasonCode != CloseServerShutdown || goAway.Flags != CloseFlagDrain || goAway.Message != "restart" {
		t.Fatalf("goaway = %+v", goAway)
	}

	closePayload := EncodeCloseMessage(CloseMessage{ReasonCode: CloseNormal, Flags: CloseFlagImmediate, DrainTimeoutMillis: 123})
	closeMsg, err := DecodeCloseMessage(closePayload)
	if err != nil {
		t.Fatalf("decode close: %v", err)
	}
	if closeMsg.ReasonCode != CloseNormal || closeMsg.Flags != CloseFlagImmediate || closeMsg.DrainTimeoutMillis != 123 {
		t.Fatalf("close = %+v", closeMsg)
	}
}

func TestErrorGoAwayClosePayloadRejectsInvalidData(t *testing.T) {
	if _, err := DecodeErrorMessageView([]byte{1}); err == nil {
		t.Fatalf("expected short error payload")
	}
	payload := EncodeErrorMessage(ErrorMessage{Message: "x"})
	payload[31] = 2
	if _, err := DecodeErrorMessageView(payload); err == nil {
		t.Fatalf("expected invalid error message length")
	}
	if _, err := DecodeGoAwayView([]byte{1}); err == nil {
		t.Fatalf("expected short goaway payload")
	}
	payload = EncodeGoAway(GoAway{Message: "x"})
	payload[31] = 2
	if _, err := DecodeGoAwayView(payload); err == nil {
		t.Fatalf("expected invalid goaway message length")
	}
	payload = EncodeGoAway(GoAway{Flags: CloseFlagImmediate | CloseFlagDrain})
	if _, err := DecodeGoAwayView(payload); err == nil {
		t.Fatalf("expected mutually exclusive goaway flags")
	}
	if _, err := DecodeCloseMessage([]byte{1}); err == nil {
		t.Fatalf("expected short close payload")
	}
}

func TestPayloadEncodersClearReservedBytesInReusedBuffers(t *testing.T) {
	dirty := func(size int) []byte {
		buf := make([]byte, size)
		for i := range buf {
			buf[i] = 0xff
		}
		return buf[:0]
	}

	errorPayload, err := EncodeErrorMessageInto(dirty(64), ErrorInternal, FrameRequest, SchemaEvent, 1, 2, []byte("x"))
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	if _, err := DecodeErrorMessageView(errorPayload); err != nil {
		t.Fatalf("decode error encoded into dirty buffer: %v", err)
	}

	goAwayPayload, err := EncodeGoAwayInto(dirty(64), CloseServerShutdown, CloseFlagDrain, 100, 1, 2, []byte("x"))
	if err != nil {
		t.Fatalf("encode goaway: %v", err)
	}
	if _, err := DecodeGoAwayView(goAwayPayload); err != nil {
		t.Fatalf("decode goaway encoded into dirty buffer: %v", err)
	}

	windowPayload := EncodeWindowInto(dirty(24), Window{
		TransferID:   1,
		WindowBytes:  1024,
		WindowChunks: 4,
		Flags:        WindowFlagTransfer,
	})
	if _, err := DecodeWindow(windowPayload); err != nil {
		t.Fatalf("decode window encoded into dirty buffer: %v", err)
	}
}
