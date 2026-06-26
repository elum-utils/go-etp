package etp

import "sync"

type FrameScheduler struct {
	mu     sync.Mutex
	queues [5][]Frame
}

func NewFrameScheduler() *FrameScheduler {
	s := &FrameScheduler{}
	for i := range s.queues {
		s.queues[i] = make([]Frame, 0, 64)
	}
	return s
}

func (s *FrameScheduler) Push(frame Frame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pushLocked(frame)
}

func (s *FrameScheduler) pushLocked(frame Frame) {
	priority := frame.Header.Priority
	if priority > PriorityIdle {
		priority = PriorityIdle
	}
	s.queues[priority] = append(s.queues[priority], frame)
}

func (s *FrameScheduler) Pop() (Frame, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.popLocked()
}

func (s *FrameScheduler) popLocked() (Frame, bool) {
	for priority := PriorityCritical; priority <= PriorityIdle; priority++ {
		queue := s.queues[priority]
		if len(queue) == 0 {
			continue
		}
		frame := queue[0]
		copy(queue, queue[1:])
		var zero Frame
		queue[len(queue)-1] = zero
		s.queues[priority] = queue[:len(queue)-1]
		return frame, true
	}
	return Frame{}, false
}

func (s *FrameScheduler) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lenLocked()
}

func (s *FrameScheduler) PushPop(frame Frame) Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pushLocked(frame)
	next, ok := s.popLocked()
	if !ok {
		return frame
	}
	return next
}

func (s *FrameScheduler) lenLocked() int {
	total := 0
	for _, queue := range s.queues {
		total += len(queue)
	}
	return total
}
