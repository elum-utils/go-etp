package tests

import (
	"bytes"
	"testing"
	"time"

	. "github.com/elum-utils/go-etp/internal/etp"
)

func TestProtocolEventCallbackPanicDoesNotEscapeSession(t *testing.T) {
	session := NewSession(newRecordingTransport(t))
	session.OnProtocolEvent(func(ProtocolEvent) { panic("event callback failed") })
	unknown := NewFrame(255, 0, nil)
	if err := session.HandleFrame(t.Context(), unknown); err == nil {
		t.Fatal("unknown frame was accepted")
	}
}

func TestProgressCallbackPanicDoesNotFailTransfer(t *testing.T) {
	transport := newRecordingTransport(t)
	session := NewSession(transport)
	session.OnProgress(func(Progress) { panic("progress callback failed") })
	installTransferAcker(t, transport, session)
	handle := session.StartTransfer(t.Context(), TransferOptions{
		Reader:    bytes.NewReader([]byte("data")),
		TotalSize: 4,
		ChunkSize: 4,
	})
	select {
	case err := <-handle.Done():
		if err != nil {
			t.Fatalf("progress callback panic failed transfer: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("transfer did not finish after progress callback panic")
	}
}
