package tests

import (
	"testing"

	. "github.com/elum-utils/go-etp/internal/etp"
)

type leaseReleaser struct {
	calls int
	data  []byte
}

func (r *leaseReleaser) ReleaseFrameLease(data []byte) {
	r.calls++
	r.data = data
}

func TestFrameLeaseReleaserRunsAfterLastReference(t *testing.T) {
	data := []byte("frame")
	owner := new(leaseReleaser)
	lease := InitFrameLease(new(FrameLease), data, owner)
	lease.Retain()
	lease.Release()
	if owner.calls != 0 {
		t.Fatalf("releaser called before last reference")
	}
	lease.Release()
	if owner.calls != 1 || string(owner.data) != "frame" {
		t.Fatalf("releaser calls/data = %d/%q", owner.calls, owner.data)
	}
}

func TestFrameLeaseRejectsActiveReinitialization(t *testing.T) {
	lease := InitFrameLease(new(FrameLease), []byte("first"), new(leaseReleaser))
	defer lease.Release()
	defer func() {
		if recover() == nil {
			t.Fatalf("expected active lease panic")
		}
	}()
	InitFrameLease(lease, []byte("second"), new(leaseReleaser))
}
