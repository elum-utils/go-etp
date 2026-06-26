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

type sessionWriter struct {
	t    FrameTransport
	cfg  SendQueueConfig
	mu   sync.Mutex
	cond *sync.Cond

	queue       [5][]sendItem
	queuedItems int
	queuedBytes uint64
	closed      bool
	err         error
}

func newSessionWriter(t FrameTransport, cfg SendQueueConfig) *sessionWriter {
	w := &sessionWriter{t: t, cfg: cfg}
	w.cond = sync.NewCond(&w.mu)
	for i := range w.queue {
		w.queue[i] = make([]sendItem, 0, 64)
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
			w.cond.Wait()
			timer.Stop()
			continue
		}
		w.cond.Wait()
	}
	if err := w.closedErrLocked(); err != nil {
		w.releaseItem(item, err)
		return err
	}
	w.queue[item.priority] = append(w.queue[item.priority], item)
	w.queuedItems++
	w.queuedBytes += size
	w.cond.Signal()
	return nil
}

func (w *sessionWriter) flush(ctx context.Context) error {
	done := make(chan error, 1)
	if err := w.enqueue(ctx, sendItem{priority: PriorityIdle, done: done}); err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
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
		w.cond.Broadcast()
		w.mu.Unlock()
		if !ok {
			continue
		}

		err := w.write(item)
		w.releaseItem(item, err)
		if err != nil {
			w.mu.Lock()
			if w.err == nil {
				w.err = err
			}
			w.closed = true
			w.releaseQueuedLocked(err)
			w.cond.Broadcast()
			w.mu.Unlock()
			return
		}
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
	for priority := PriorityCritical; priority <= PriorityIdle; priority++ {
		queue := w.queue[priority]
		if len(queue) == 0 {
			continue
		}
		item := queue[0]
		copy(queue, queue[1:])
		queue[len(queue)-1] = sendItem{}
		w.queue[priority] = queue[:len(queue)-1]
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
		queue := w.queue[priority]
		for i := range queue {
			w.releaseItem(queue[i], err)
		}
		w.queue[priority] = w.queue[priority][:0]
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
