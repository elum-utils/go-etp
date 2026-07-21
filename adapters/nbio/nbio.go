package nbio

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	etp "github.com/elum-utils/go-etp"
	"github.com/lesismal/nbio/nbhttp/websocket"
)

var (
	ErrClosed        = errors.New("nbio websocket transport closed")
	ErrFrameTooLarge = errors.New("nbio websocket frame exceeds configured limit")
	ErrQueueFull     = errors.New("nbio websocket frame queue is full")
)

var framePool = sync.Pool{
	New: func() any {
		return &queuedFrame{data: make([]byte, 0, 4096)}
	},
}

type queuedFrame struct {
	data  []byte
	lease etp.FrameLease
}

func (f *queuedFrame) ReleaseFrameLease([]byte) {
	if cap(f.data) <= etp.MaxPooledFrameBytes {
		f.data = f.data[:0]
		framePool.Put(f)
	}
}

type Transport struct {
	conn      *websocket.Conn
	frames    chan *queuedFrame
	closeOnce sync.Once
	queueMu   sync.RWMutex
	closed    atomic.Bool
	err       error
	mu        sync.Mutex
	maxFrame  atomic.Int64
}

func NewTransport(conn *websocket.Conn, queueSize int) *Transport {
	if queueSize <= 0 {
		queueSize = 64
	}
	t := &Transport{
		conn:   conn,
		frames: make(chan *queuedFrame, queueSize),
	}
	t.maxFrame.Store(etp.MaxFrameBytes)
	conn.OnMessage(func(_ *websocket.Conn, messageType websocket.MessageType, data []byte) {
		if messageType != websocket.BinaryMessage {
			return
		}
		t.enqueue(data)
	})
	conn.OnClose(func(_ *websocket.Conn, err error) {
		t.close(err)
	})
	return t
}

func (t *Transport) SendFrame(frame []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.conn.WriteMessage(websocket.BinaryMessage, frame)
}

func (t *Transport) ReadFrame() ([]byte, error) {
	lease, err := t.ReadFrameLease()
	if err != nil {
		return nil, err
	}
	data := append([]byte(nil), lease.Data...)
	lease.Release()
	return data, nil
}

func (t *Transport) ReadFrameLease() (*etp.FrameLease, error) {
	frame, ok := <-t.frames
	if ok {
		return etp.InitFrameLease(&frame.lease, frame.data, frame), nil
	}
	if t.err != nil {
		return nil, t.err
	}
	return nil, ErrClosed
}

func (t *Transport) SetMaxFrameBytes(max uint32) {
	if max < etp.HeaderSize || max > etp.MaxFrameBytes {
		max = etp.MaxFrameBytes
	}
	t.maxFrame.Store(int64(max))
}

func (t *Transport) Close() error {
	t.close(ErrClosed)
	return t.conn.Close()
}

func (t *Transport) close(err error) {
	t.closeOnce.Do(func() {
		t.queueMu.Lock()
		t.closed.Store(true)
		t.err = err
		close(t.frames)
		t.queueMu.Unlock()
	})
}

func (t *Transport) enqueue(data []byte) {
	if int64(len(data)) > t.maxFrame.Load() {
		t.closeAndDisconnect(ErrFrameTooLarge)
		return
	}
	frame := framePool.Get().(*queuedFrame)
	if cap(frame.data) < len(data) {
		frame.data = make([]byte, len(data))
	} else {
		frame.data = frame.data[:len(data)]
	}
	copy(frame.data, data)

	t.queueMu.RLock()
	queued := false
	if !t.closed.Load() {
		select {
		case t.frames <- frame:
			queued = true
		default:
		}
	}
	t.queueMu.RUnlock()
	if queued {
		return
	}
	frame.ReleaseFrameLease(nil)
	if !t.closed.Load() {
		t.closeAndDisconnect(ErrQueueFull)
	}
}

func (t *Transport) closeAndDisconnect(err error) {
	t.close(err)
	if t.conn != nil {
		_ = t.conn.Close()
	}
}

type Adapter struct {
	Addr      string
	Path      string
	Server    *http.Server
	Upgrader  *websocket.Upgrader
	QueueSize int
}

func New(addr string) *Adapter {
	return &Adapter{Addr: addr, Path: "/etp"}
}

func (a *Adapter) Name() string { return "websocket/nbio" }

func (a *Adapter) Handler(app *etp.App) http.Handler {
	upgrader := a.Upgrader
	if upgrader == nil {
		upgrader = websocket.NewUpgrader()
	}
	if max := int(app.MaxFrameBytes()); upgrader.MessageLengthLimit <= 0 || upgrader.MessageLengthLimit > max {
		upgrader.MessageLengthLimit = max
	}
	upgrader.BlockingModHandleRead = false
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		wsTransport := NewTransport(conn, a.QueueSize)
		go conn.HandleRead(upgrader.BlockingModReadBufferSize)
		remote := r.RemoteAddr
		if host, _, splitErr := net.SplitHostPort(r.RemoteAddr); splitErr == nil {
			remote = host
		}
		_, _ = app.ServeTransportWithRemote(r.Context(), a.Name(), remote, wsTransport)
	})
}

func (a *Adapter) Serve(ctx context.Context, app *etp.App) error {
	path := a.Path
	if path == "" {
		path = "/etp"
	}
	mux := http.NewServeMux()
	mux.Handle(path, a.Handler(app))
	server := a.Server
	if server == nil {
		server = &http.Server{Addr: a.Addr}
	}
	server.Handler = mux
	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, io.EOF) || ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
