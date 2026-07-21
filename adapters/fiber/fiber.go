package fiber

import (
	"context"
	"errors"
	"io"
	"sync"

	etp "github.com/elum-utils/go-etp"
	"github.com/fasthttp/websocket"
	fiber "github.com/gofiber/fiber/v3"
)

const maxPooledFrameCap = 8 << 20

var readBufferPool = sync.Pool{
	New: func() any {
		return &readBuffer{data: make([]byte, 0, 4096)}
	},
}

type readBuffer struct {
	data  []byte
	lease etp.FrameLease
}

func (b *readBuffer) ReleaseFrameLease([]byte) {
	putReadBuffer(b)
}

type Transport struct {
	conn   *websocket.Conn
	mu     sync.Mutex
	strict bool
}

func NewTransport(conn *websocket.Conn) *Transport {
	return &Transport{conn: conn}
}

func NewStrictTransport(conn *websocket.Conn) *Transport {
	return &Transport{conn: conn, strict: true}
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
	defer lease.Release()
	data := make([]byte, len(lease.Data))
	copy(data, lease.Data)
	return data, nil
}

func (t *Transport) ReadFrameLease() (*etp.FrameLease, error) {
	for {
		messageType, reader, err := t.conn.NextReader()
		if err != nil {
			return nil, err
		}
		if messageType == websocket.BinaryMessage {
			return readETPFrameLease(reader, t.strict)
		}
	}
}

func (t *Transport) Close() error { return t.conn.Close() }

func readETPFrameLease(reader io.Reader, strict bool) (*etp.FrameLease, error) {
	buf := readBufferPool.Get().(*readBuffer)
	data := ensureReadBuffer(buf, etp.HeaderSize)
	if _, err := io.ReadFull(reader, data[:etp.HeaderSize]); err != nil {
		putReadBuffer(buf)
		return nil, err
	}
	header, err := etp.DecodeHeader(data[:etp.HeaderSize])
	if err != nil {
		putReadBuffer(buf)
		return nil, err
	}
	total := int(header.PayloadOffset) + int(header.PayloadLength)
	if total > etp.MaxFrameBytes {
		putReadBuffer(buf)
		return nil, errors.New("incoming frame exceeds max size")
	}
	data = ensureReadBufferPreserve(buf, total, etp.HeaderSize)
	if _, err := io.ReadFull(reader, data[etp.HeaderSize:total]); err != nil {
		putReadBuffer(buf)
		return nil, err
	}
	if strict {
		var extra [1]byte
		n, err := reader.Read(extra[:])
		if n != 0 || err != io.EOF {
			putReadBuffer(buf)
			if err != nil {
				return nil, err
			}
			return nil, errors.New("websocket message has trailing frame bytes")
		}
	}
	buf.data = data
	return etp.InitFrameLease(&buf.lease, data, buf), nil
}

func ensureReadBuffer(buf *readBuffer, size int) []byte {
	if cap(buf.data) < size {
		buf.data = make([]byte, size)
		return buf.data
	}
	buf.data = buf.data[:size]
	return buf.data
}

func ensureReadBufferPreserve(buf *readBuffer, size int, preserve int) []byte {
	if cap(buf.data) < size {
		next := make([]byte, size)
		copy(next, buf.data[:preserve])
		buf.data = next
		return next
	}
	buf.data = buf.data[:size]
	return buf.data
}

func putReadBuffer(buf *readBuffer) {
	if cap(buf.data) <= maxPooledFrameCap {
		buf.data = buf.data[:0]
		readBufferPool.Put(buf)
	}
}

type Adapter struct {
	Addr                string
	Path                string
	App                 *fiber.App
	Upgrader            websocket.FastHTTPUpgrader
	StrictFrameBoundary bool
}

func New(addr string) *Adapter {
	return &Adapter{Addr: addr, Path: "/etp"}
}

func (a *Adapter) Name() string { return "websocket/fiber" }

func (a *Adapter) Handler(app *etp.App) fiber.Handler {
	upgrader := a.Upgrader
	if upgrader.ReadBufferSize == 0 {
		upgrader.ReadBufferSize = 4096
	}
	if upgrader.WriteBufferSize == 0 {
		upgrader.WriteBufferSize = 4096
	}
	return func(c fiber.Ctx) error {
		ctx := c.Context()
		remote := c.IP()
		return upgrader.Upgrade(c.RequestCtx(), func(conn *websocket.Conn) {
			transport := NewTransport(conn)
			transport.strict = a.StrictFrameBoundary
			_, _ = app.ServeTransportWithRemote(ctx, a.Name(), remote, transport)
		})
	}
}

func (a *Adapter) Serve(ctx context.Context, app *etp.App) error {
	path := a.Path
	if path == "" {
		path = "/etp"
	}
	fiberApp := a.App
	if fiberApp == nil {
		fiberApp = fiber.New()
	}
	fiberApp.Get(path, a.Handler(app))
	go func() {
		<-ctx.Done()
		_ = fiberApp.Shutdown()
	}()
	err := fiberApp.Listen(a.Addr)
	if errors.Is(err, io.EOF) || ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
