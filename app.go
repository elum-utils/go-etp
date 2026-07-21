package etp

import (
	"context"
	"errors"
	"io"
	"sync"

	protocol "github.com/elum-utils/go-etp/internal/etp"
)

const defaultMaxMemoryBody = 8 << 20
const defaultMaxPooledBody = 64 << 10

var ErrNilFrameTransport = errors.New("transport: nil frame transport")

type App struct {
	router      *Router
	config      Config
	contexts    sync.Pool
	bodyWriters sync.Pool
}

func New(config Config) *App {
	if config.Session.Role == "" {
		config.Session.Role = protocol.RoleServer
	}
	config.Session = protocol.NormalizeSessionConfig(config.Session)
	if config.MaxMemoryBody <= 0 {
		config.MaxMemoryBody = defaultMaxMemoryBody
	}
	if config.MaxPooledBodyBytes == 0 {
		config.MaxPooledBodyBytes = defaultMaxPooledBody
	} else if config.MaxPooledBodyBytes < 0 {
		config.MaxPooledBodyBytes = 0
	}
	app := &App{
		router: NewRouter(),
		config: config,
	}
	app.contexts.New = func() any { return new(Context) }
	app.bodyWriters.New = func() any { return new(spooledBodyWriter) }
	return app
}

func (a *App) Use(pattern string, middleware Middleware) error {
	return a.router.Use(pattern, middleware)
}

func (a *App) On(pattern string, handler Handler) error {
	return a.router.On(pattern, handler)
}

func (a *App) Group(prefix string, middlewares ...Middleware) *Group {
	return &Group{router: a.router.Group(prefix, middlewares...)}
}

func (a *App) Compile() {
	a.router.Compile()
}

func (a *App) MaxFrameBytes() uint32 {
	return a.config.Session.Payload.MaxFrameBytes
}

func (a *App) ServeTransport(ctx context.Context, name string, transport protocol.FrameTransport) (*Peer, error) {
	return a.ServeTransportWithRemote(ctx, name, "", transport)
}

func (a *App) ServeTransportWithRemote(ctx context.Context, name string, remote string, transport protocol.FrameTransport) (*Peer, error) {
	if transport == nil {
		return nil, ErrNilFrameTransport
	}
	a.Compile()
	config := a.config.Session
	peer := &Peer{app: a, adapter: name, remote: remote}
	config.Receive.RequestHandler = peer.handleRequest
	config.Receive.ResponseHandler = peer.handleResponse
	config.Receive.TransferHandler = peer.handleTransfer
	peer.session = protocol.NewSessionWithConfig(transport, config)
	return peer, peer.session.Run(ctx)
}

func (a *App) emit(ctx *Context) error {
	err := a.router.Emit(ctx)
	if err != nil && a.config.OnError != nil {
		a.config.OnError(ctx, err)
	}
	return err
}

func (a *App) acquireContext() *Context {
	return a.contexts.Get().(*Context)
}

func (a *App) releaseContext(ctx *Context) {
	ctx.Context = nil
	ctx.App = nil
	ctx.Peer = nil
	ctx.Event = ""
	ctx.RequestID = 0
	ctx.TransferID = 0
	ctx.Fields = nil
	ctx.Body = nil
	ctx.inline.data = nil
	a.contexts.Put(ctx)
}

func (p *Peer) dispatchEvent(ctx context.Context, frame protocol.Frame, message protocol.EventMessageView) (err error) {
	request := p.app.acquireContext()
	defer p.app.releaseContext(request)

	request.Context = ctx
	request.App = p.app
	request.Peer = p
	request.Event = bytesToStringView(message.Event)
	request.RequestID = frame.Header.RequestID
	request.Fields = message.Fields
	request.inline.data = message.Data
	request.Body = &request.inline
	return p.app.emit(request)
}

func (p *Peer) handleRequest(ctx context.Context, frame protocol.Frame, message protocol.EventMessageView) error {
	return p.dispatchEvent(ctx, frame, message)
}

func (p *Peer) handleResponse(ctx context.Context, frame protocol.Frame, message protocol.EventMessageView) error {
	return p.dispatchEvent(ctx, frame, message)
}

func (p *Peer) handleTransfer(ctx context.Context, info protocol.IncomingTransferInfo) (protocol.IncomingTransferWriter, error) {
	event := info.Meta.Event
	if event == "" {
		event = info.Meta.Name
	}
	return p.app.acquireBodyWriter(ctx, p, event, info), nil
}

type Group struct {
	router *Router
}

func (g *Group) Use(pattern string, middleware Middleware) error {
	return g.router.Use(pattern, middleware)
}

func (g *Group) On(pattern string, handler Handler) error {
	return g.router.On(pattern, handler)
}

func (g *Group) Group(prefix string, middlewares ...Middleware) *Group {
	return &Group{router: g.router.Group(prefix, middlewares...)}
}

type Adapter interface {
	Name() string
	Serve(context.Context, *App) error
}

type FrameTransportAdapter struct {
	AdapterName string
	Transport   protocol.FrameTransport
}

func (a FrameTransportAdapter) Name() string {
	if a.AdapterName == "" {
		return "frame"
	}
	return a.AdapterName
}

func (a FrameTransportAdapter) Serve(ctx context.Context, app *App) error {
	_, err := app.ServeTransport(ctx, a.Name(), a.Transport)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}
