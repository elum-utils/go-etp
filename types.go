package etp

import (
	"context"
	"io"
	"strings"
	"time"

	protocol "github.com/elum-utils/go-etp/internal/etp"
)

type Field = protocol.TransferField

type Handler func(*Context) error

type Middleware func(Handler) Handler

type ErrorHandler func(*Context, error)

type Config struct {
	Session            protocol.SessionConfig
	MaxMemoryBody      int64
	MaxPooledBodyBytes int
	OnError            ErrorHandler
}

type SendOptions struct {
	Event       string
	Data        []byte
	Fields      []Field
	Reader      io.Reader
	Size        uint64
	Name        string
	Field       string
	Index       uint32
	ContentType uint32
	ChunkSize   int
	AckTimeout  time.Duration
	RetryLimit  int
}

type MessageHandle = protocol.MessageHandle

type Body interface {
	Size() int64
	Open() (io.ReadCloser, error)
	Bytes() ([]byte, error)
	View() ([]byte, bool)
	IsInline() bool
}

type Peer struct {
	app       *App
	session   *protocol.Session
	adapter   string
	remote    string
	closeOnce chan struct{}
}

func (p *Peer) Session() *protocol.Session { return p.session }

func (p *Peer) Adapter() string { return p.adapter }

func (p *Peer) RemoteAddr() string { return p.remote }

func (p *Peer) Send(ctx context.Context, opts SendOptions) (MessageHandle, error) {
	return p.session.Send(ctx, toETPMessageOptions(opts))
}

func (p *Peer) Request(ctx context.Context, opts SendOptions) (MessageHandle, error) {
	return p.session.Request(ctx, toETPMessageOptions(opts))
}

func (p *Peer) Respond(ctx context.Context, requestID uint64, opts SendOptions) (MessageHandle, error) {
	return p.session.Respond(ctx, requestID, toETPMessageOptions(opts))
}

// Context is borrowed for one synchronous Handler call and must not be retained.
type Context struct {
	context.Context
	App        *App
	Peer       *Peer
	Event      string
	RequestID  uint64
	TransferID uint64
	Fields     []Field
	Body       Body
	inline     inlineBody
	file       fileBody
}

// EventCopy returns an event name that remains valid after the handler returns.
func (c *Context) EventCopy() string { return strings.Clone(c.Event) }

func (c *Context) Field(name string) string {
	for _, field := range c.Fields {
		if field.Key == name {
			return field.Value
		}
	}
	return ""
}

func (c *Context) Bytes() ([]byte, error) {
	if c.Body == nil {
		return nil, nil
	}
	return c.Body.Bytes()
}

// BodyView returns body bytes without copying. The view is valid only until the handler returns.
func (c *Context) BodyView() ([]byte, bool) {
	if c.Body == nil {
		return nil, true
	}
	return c.Body.View()
}

func (c *Context) Respond(opts SendOptions) (MessageHandle, error) {
	return c.Peer.Respond(c.Context, c.RequestID, opts)
}

func toETPMessageOptions(opts SendOptions) protocol.MessageOptions {
	return protocol.MessageOptions{
		Event:       opts.Event,
		Data:        opts.Data,
		Fields:      opts.Fields,
		Reader:      opts.Reader,
		Size:        opts.Size,
		Name:        opts.Name,
		Field:       opts.Field,
		Index:       opts.Index,
		ContentType: opts.ContentType,
		ChunkSize:   opts.ChunkSize,
		AckTimeout:  opts.AckTimeout,
		RetryLimit:  opts.RetryLimit,
	}
}
