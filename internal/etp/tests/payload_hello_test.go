package tests

import (
	. "github.com/elum-utils/go-etp/internal/etp"
	"testing"
)

func TestHelloPayloadRoundTrip(t *testing.T) {
	hello := Hello{
		Role:              "client",
		Capabilities:      DefaultCapabilities,
		MaxFrameBytes:     MaxFrameBytes,
		MaxChunkSize:      64 * 1024,
		MaxTransferBytes:  512 << 20,
		MaxInFlightChunks: 16,
		HeartbeatMillis:   10_000,
	}
	decoded, err := DecodeHelloMessage(EncodeHelloMessage(hello))
	if err != nil {
		t.Fatalf("decode hello: %v", err)
	}
	if decoded != hello {
		t.Fatalf("hello mismatch: %+v", decoded)
	}
}

func TestHelloPayloadRejectsInvalidData(t *testing.T) {
	if _, err := DecodeHelloMessage([]byte{1, 2, 3}); err == nil {
		t.Fatalf("expected short hello error")
	}

	payload := EncodeHelloMessage(Hello{Role: RoleClient, Capabilities: DefaultCapabilities})
	payload[36] = 0xff
	payload[37] = 0xff
	payload[38] = 0xff
	payload[39] = 0xff
	if _, err := DecodeHelloMessage(payload); err == nil {
		t.Fatalf("expected invalid role length error")
	}
}
