package etp

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrSendQueueFull = errors.New("send queue full")
	ErrSessionClosed = errors.New("session closed")
)

type writeDeadlineTransport interface {
	SetWriteDeadline(time.Time) error
}

type channelFrameTransport interface {
	SendFrameOnChannel(channelID uint16, frame []byte) error
}

type sendItem struct {
	priority uint8
	channel  uint16
	data     []byte
	bufp     *[]byte
	done     chan error
}

type sendItemQueue struct {
	items []sendItem
	head  int
	size  int
}

func (q *sendItemQueue) push(item sendItem) {
	if q.size == len(q.items) {
		capacity := len(q.items) * 2
		if capacity == 0 {
			capacity = 64
		}
		items := make([]sendItem, capacity)
		for i := 0; i < q.size; i++ {
			items[i] = q.items[(q.head+i)%len(q.items)]
		}
		q.items = items
		q.head = 0
	}
	q.items[(q.head+q.size)%len(q.items)] = item
	q.size++
}

func (q *sendItemQueue) pop() (sendItem, bool) {
	if q.size == 0 {
		return sendItem{}, false
	}
	item := q.items[q.head]
	q.items[q.head] = sendItem{}
	q.head = (q.head + 1) % len(q.items)
	q.size--
	if q.size == 0 {
		q.head = 0
	}
	return item, true
}

type sessionWriter struct {
	t       FrameTransport
	cfg     SendQueueConfig
	onError func(error)
	mu      sync.Mutex
	cond    *sync.Cond

	queue        [5]sendItemQueue
	queuedItems  int
	queuedBytes  uint64
	inFlight     bool
	flushWaiters int
	queueWaiters int
	controlOnly  bool
	closed       bool
	err          error
	scheduleAt   uint8
}

var writerPrioritySchedule = [...]uint8{
	PriorityCritical, PriorityCritical, PriorityCritical, PriorityCritical,
	PriorityCritical, PriorityCritical, PriorityCritical, PriorityCritical,
	PriorityHigh, PriorityHigh, PriorityHigh, PriorityHigh,
	PriorityNormal, PriorityNormal,
	PriorityLow,
	PriorityIdle,
}

func newSessionWriter(t FrameTransport, cfg SendQueueConfig, onError func(error)) *sessionWriter {
	w := &sessionWriter{t: t, cfg: cfg, onError: onError}
	w.cond = sync.NewCond(&w.mu)
	for i := range w.queue {
		w.queue[i].items = make([]sendItem, 64)
	}
	go w.run()
	return w
}

func (w *sessionWriter) enqueue(ctx context.Context, item sendItem) error {
	if item.priority > PriorityIdle {
		item.priority = PriorityIdle
	}
	size := uint64(len(item.data))
	deadline := time.Time{}
	if w.cfg.EnqueueTimeout > 0 {
		deadline = time.Now().Add(w.cfg.EnqueueTimeout)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.controlOnly && (item.priority != PriorityCritical || item.channel != ChannelControl) {
		w.releaseItem(item, ErrSessionClosed)
		return ErrSessionClosed
	}
	for !w.canEnqueue(size) {
		if err := w.closedErrLocked(); err != nil {
			w.releaseItem(item, err)
			return err
		}
		if err := ctx.Err(); err != nil {
			w.releaseItem(item, err)
			return err
		}
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				w.releaseItem(item, ErrSendQueueFull)
				return ErrSendQueueFull
			}
			timer := time.AfterFunc(remaining, func() {
				w.mu.Lock()
				w.cond.Broadcast()
				w.mu.Unlock()
			})
			w.queueWaiters++
			w.cond.Wait()
			w.queueWaiters--
			timer.Stop()
			continue
		}
		w.queueWaiters++
		w.cond.Wait()
		w.queueWaiters--
	}
	if err := w.closedErrLocked(); err != nil {
		w.releaseItem(item, err)
		return err
	}
	if w.controlOnly && (item.priority != PriorityCritical || item.channel != ChannelControl) {
		w.releaseItem(item, ErrSessionClosed)
		return ErrSessionClosed
	}
	w.queue[item.priority].push(item)
	w.queuedItems++
	w.queuedBytes += size
	w.cond.Signal()
	return nil
}

func (w *sessionWriter) beginClosing() {
	w.mu.Lock()
	w.controlOnly = true
	w.releaseQueuedLocked(ErrSessionClosed)
	w.cond.Broadcast()
	w.mu.Unlock()
}

func (w *sessionWriter) flush(ctx context.Context) error {
	stop := context.AfterFunc(ctx, func() {
		w.mu.Lock()
		w.cond.Broadcast()
		w.mu.Unlock()
	})
	defer stop()
	w.mu.Lock()
	w.flushWaiters++
	defer func() { w.flushWaiters-- }()
	defer w.mu.Unlock()
	for w.queuedItems > 0 || w.inFlight {
		if err := ctx.Err(); err != nil {
			return err
		}
		if w.err != nil {
			return w.err
		}
		w.cond.Wait()
	}
	return w.err
}

func (w *sessionWriter) close() {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		w.err = ErrSessionClosed
		w.releaseQueuedLocked(ErrSessionClosed)
	}
	w.cond.Broadcast()
	w.mu.Unlock()
}

func (w *sessionWriter) run() {
	for {
		w.mu.Lock()
		for w.queuedItems == 0 && !w.closed {
			w.cond.Wait()
		}
		if w.queuedItems == 0 && w.closed {
			w.mu.Unlock()
			return
		}
		item, ok := w.popLocked()
		if ok {
			w.inFlight = true
		}
		if w.queueWaiters > 0 {
			w.cond.Broadcast()
		}
		w.mu.Unlock()
		if !ok {
			continue
		}

		err := w.write(item)
		w.releaseItem(item, err)
		w.mu.Lock()
		w.inFlight = false
		if err != nil {
			if w.err == nil {
				w.err = err
			}
			w.closed = true
			w.releaseQueuedLocked(err)
			w.cond.Broadcast()
			w.mu.Unlock()
			if w.onError != nil {
				w.onError(err)
			}
			return
		}
		if w.flushWaiters > 0 {
			w.cond.Broadcast()
		}
		w.mu.Unlock()
	}
}

func (w *sessionWriter) write(item sendItem) error {
	if len(item.data) == 0 {
		return nil
	}
	if dt, ok := w.t.(writeDeadlineTransport); ok && w.cfg.WriteTimeout > 0 {
		if err := dt.SetWriteDeadline(time.Now().Add(w.cfg.WriteTimeout)); err != nil {
			return err
		}
		defer dt.SetWriteDeadline(time.Time{})
	}
	if mt, ok := w.t.(channelFrameTransport); ok {
		return mt.SendFrameOnChannel(item.channel, item.data)
	}
	return w.t.SendFrame(item.data)
}

func (w *sessionWriter) popLocked() (sendItem, bool) {
	for range writerPrioritySchedule {
		priority := writerPrioritySchedule[w.scheduleAt]
		w.scheduleAt = (w.scheduleAt + 1) % uint8(len(writerPrioritySchedule))
		item, ok := w.queue[priority].pop()
		if !ok {
			continue
		}
		w.queuedItems--
		if n := uint64(len(item.data)); w.queuedBytes >= n {
			w.queuedBytes -= n
		} else {
			w.queuedBytes = 0
		}
		return item, true
	}
	return sendItem{}, false
}

func (w *sessionWriter) canEnqueue(size uint64) bool {
	if w.cfg.MaxFrames > 0 && w.queuedItems >= w.cfg.MaxFrames {
		return false
	}
	if w.cfg.MaxBytes > 0 && w.queuedBytes+size > w.cfg.MaxBytes {
		return false
	}
	return true
}

func (w *sessionWriter) closedErrLocked() error {
	if w.err != nil {
		return w.err
	}
	if w.closed {
		return ErrSessionClosed
	}
	return nil
}

func (w *sessionWriter) releaseQueuedLocked(err error) {
	for priority := range w.queue {
		queue := &w.queue[priority]
		for queue.size > 0 {
			item, _ := queue.pop()
			w.releaseItem(item, err)
		}
	}
	w.queuedItems = 0
	w.queuedBytes = 0
}

func (w *sessionWriter) releaseItem(item sendItem, err error) {
	if item.bufp != nil {
		putFrameBuffer(item.bufp)
	}
	if item.done != nil {
		item.done <- err
	}
}
