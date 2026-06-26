package tests

import (
	. "github.com/elum-utils/go-etp"
	"testing"
)

func TestStateStringValues(t *testing.T) {
	if SessionEstablished.String() != "ESTABLISHED" {
		t.Fatalf("session state string mismatch")
	}
	if SessionState(255).String() != "UNKNOWN" {
		t.Fatalf("unknown session state string mismatch")
	}
	if TransferCanceling.String() != "CANCELING" {
		t.Fatalf("transfer state string mismatch")
	}
	if TransferState(255).String() != "UNKNOWN" {
		t.Fatalf("unknown transfer state string mismatch")
	}
}
