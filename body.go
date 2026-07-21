package etp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"

	protocol "github.com/elum-utils/go-etp/internal/etp"
)

var (
	ErrBodyTooLargeForBytes = errors.New("transport: body is not fully in memory")
	ErrBodyWriterClosed     = errors.New("transport: body writer is closed")
)

type inlineBody struct {
	data []byte
}

func NewBytesBody(data []byte) Body {
	return inlineBody{data: data}
}

func (b inlineBody) Size() int64 { return int64(len(b.data)) }

func (b inlineBody) Open() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(b.data)), nil
}

func (b inlineBody) Bytes() ([]byte, error) {
	out := make([]byte, len(b.data))
	copy(out, b.data)
	return out, nil
}

func (b inlineBody) View() ([]byte, bool) { return b.data, true }

func (b inlineBody) IsInline() bool { return true }

type fileBody struct {
	path string
	size int64
}

func (b fileBody) Size() int64 { return b.size }

func (b fileBody) Open() (io.ReadCloser, error) {
	return os.Open(b.path)
}

func (b fileBody) Bytes() ([]byte, error) {
	return nil, ErrBodyTooLargeForBytes
}

func (b fileBody) View() ([]byte, bool) { return nil, false }

func (b fileBody) IsInline() bool { return false }

type spooledBodyWriter struct {
	app       *App
	ctx       Context
	maxMemory int64
	size      int64
	memory    bytes.Buffer
	file      *os.File
	path      string
	released  bool
}

func (a *App) acquireBodyWriter(ctx context.Context, peer *Peer, event string, info protocol.IncomingTransferInfo) *spooledBodyWriter {
	w := a.bodyWriters.Get().(*spooledBodyWriter)
	w.app = a
	w.maxMemory = a.config.MaxMemoryBody
	w.released = false
	w.ctx.Context = ctx
	w.ctx.App = a
	w.ctx.Peer = peer
	w.ctx.Event = event
	w.ctx.RequestID = info.RequestID
	w.ctx.TransferID = info.TransferID
	w.ctx.Fields = info.Meta.Fields
	return w
}

func (w *spooledBodyWriter) Write(p []byte) (int, error) {
	if w.released {
		return 0, ErrBodyWriterClosed
	}
	if w.file == nil && int64(w.memory.Len()+len(p)) <= w.maxMemory {
		n, err := w.memory.Write(p)
		w.size += int64(n)
		return n, err
	}
	if w.file == nil {
		file, err := os.CreateTemp("", "go-etp-body-*")
		if err != nil {
			return 0, err
		}
		w.file = file
		w.path = file.Name()
		if w.memory.Len() > 0 {
			if _, err := file.Write(w.memory.Bytes()); err != nil {
				return 0, err
			}
			w.memory.Reset()
		}
	}
	n, err := w.file.Write(p)
	w.size += int64(n)
	return n, err
}

func (w *spooledBodyWriter) Close() (err error) {
	if w.released {
		return ErrBodyWriterClosed
	}
	defer w.release()
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
		w.file = nil
		w.ctx.file.path = w.path
		w.ctx.file.size = w.size
		w.ctx.Body = &w.ctx.file
	} else {
		w.ctx.inline.data = w.memory.Bytes()
		w.ctx.Body = &w.ctx.inline
	}
	return w.app.emit(&w.ctx)
}

func (w *spooledBodyWriter) Abort() (err error) {
	if w.released {
		return ErrBodyWriterClosed
	}
	defer w.release()
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}
	if w.path != "" {
		return os.Remove(w.path)
	}
	return nil
}

func (w *spooledBodyWriter) release() {
	w.released = true
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}
	if w.path != "" {
		_ = os.Remove(w.path)
	}
	w.path = ""
	w.size = 0
	w.maxMemory = 0
	w.ctx = Context{}
	if w.memory.Cap() > w.app.config.MaxPooledBodyBytes {
		w.memory = bytes.Buffer{}
	} else {
		w.memory.Reset()
	}
	app := w.app
	w.app = nil
	app.bodyWriters.Put(w)
}
