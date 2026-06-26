package tests

import (
	. "github.com/elum-utils/go-etp"
	"testing"
)

func TestFrameSchedulerPopsByPriority(t *testing.T) {
	scheduler := NewFrameScheduler()
	low := NewFrame(FrameData, 0, nil)
	low.Header.Priority = PriorityLow
	critical := NewFrame(FramePing, 0, nil)
	critical.Header.Priority = PriorityCritical
	normal := NewFrame(FrameRequest, SchemaEvent, nil)
	normal.Header.Priority = PriorityNormal

	scheduler.Push(low)
	scheduler.Push(normal)
	scheduler.Push(critical)

	frame, ok := scheduler.Pop()
	if !ok || frame.Header.FrameType != FramePing {
		t.Fatalf("first frame = %+v ok=%t", frame.Header, ok)
	}
	frame, ok = scheduler.Pop()
	if !ok || frame.Header.FrameType != FrameRequest {
		t.Fatalf("second frame = %+v ok=%t", frame.Header, ok)
	}
	frame, ok = scheduler.Pop()
	if !ok || frame.Header.FrameType != FrameData {
		t.Fatalf("third frame = %+v ok=%t", frame.Header, ok)
	}
	if scheduler.Len() != 0 {
		t.Fatalf("scheduler len = %d", scheduler.Len())
	}
}
