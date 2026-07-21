package webtransport

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"

	etp "github.com/elum-utils/go-etp"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	webtransport "github.com/quic-go/webtransport-go"
)

const DefaultPath = "/etp"

type Adapter struct {
	Addr                 string
	Path                 string
	TLSConfig            *tls.Config
	QUICConfig           *quic.Config
	Server               *webtransport.Server
	Guard                etp.SlowlorisConfig
	ApplicationProtocols []string
}

func New(addr string, tlsConfig *tls.Config) *Adapter {
	return &Adapter{Addr: addr, TLSConfig: tlsConfig, Path: DefaultPath}
}

func (a *Adapter) Name() string { return "webtransport" }

func (a *Adapter) Handler(app *etp.App) http.Handler {
	server := a.server(nil)
	return a.handler(app, server)
}

func (a *Adapter) handler(app *etp.App, server *webtransport.Server) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		session, err := server.Upgrade(w, r)
		if err != nil {
			return
		}
		go a.serveSession(context.Background(), app, session)
	})
}

func (a *Adapter) Serve(ctx context.Context, app *etp.App) error {
	path := a.Path
	if path == "" {
		path = DefaultPath
	}
	mux := http.NewServeMux()
	server := a.server(mux)
	mux.Handle(path, a.handler(app, server))
	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()
	err := server.ListenAndServe()
	if ctx.Err() != nil || errors.Is(err, http.ErrServerClosed) || errors.Is(err, io.EOF) {
		return ctx.Err()
	}
	return err
}

func (a *Adapter) server(handler http.Handler) *webtransport.Server {
	if a.Server != nil {
		return a.Server
	}
	h3 := &http3.Server{
		Addr:            a.Addr,
		TLSConfig:       cloneTLSConfig(a.TLSConfig),
		QUICConfig:      a.QUICConfig,
		Handler:         handler,
		EnableDatagrams: true,
	}
	webtransport.ConfigureHTTP3Server(h3)
	return &webtransport.Server{
		H3:                   h3,
		ApplicationProtocols: a.ApplicationProtocols,
	}
}

func (a *Adapter) serveSession(ctx context.Context, app *etp.App, session *webtransport.Session) {
	defer session.CloseWithError(0, "closed")
	ft := etp.NewMultiStreamTransport(etp.MultiStreamTransportConfig{
		Context: ctx,
		Guard:   multiStreamGuard(a.Guard),
		OpenStream: func(ctx context.Context) (etp.DeadlineStream, error) {
			return session.OpenStreamSync(ctx)
		},
		AcceptStream: func(ctx context.Context) (etp.DeadlineStream, error) {
			return session.AcceptStream(ctx)
		},
	})
	_, _ = app.ServeTransportWithRemote(ctx, a.Name(), session.RemoteAddr().String(), ft)
}

type Dialer struct {
	URL                  string
	TLSConfig            *tls.Config
	QUICConfig           *quic.Config
	Guard                etp.SlowlorisConfig
	ApplicationProtocols []string
	Header               http.Header
}

func Dial(ctx context.Context, url string, tlsConfig *tls.Config) (etp.FrameTransport, io.Closer, error) {
	return Dialer{URL: url, TLSConfig: tlsConfig}.Dial(ctx)
}

func (d Dialer) Dial(ctx context.Context) (etp.FrameTransport, io.Closer, error) {
	dialer := &webtransport.Dialer{
		TLSClientConfig:      cloneTLSConfig(d.TLSConfig),
		QUICConfig:           d.QUICConfig,
		ApplicationProtocols: d.ApplicationProtocols,
	}
	_, session, err := dialer.Dial(ctx, d.URL, d.Header)
	if err != nil {
		return nil, nil, err
	}
	ft := etp.NewMultiStreamTransport(etp.MultiStreamTransportConfig{
		Context: ctx,
		Guard:   multiStreamGuard(d.Guard),
		OpenStream: func(ctx context.Context) (etp.DeadlineStream, error) {
			return session.OpenStreamSync(ctx)
		},
		AcceptStream: func(ctx context.Context) (etp.DeadlineStream, error) {
			return session.AcceptStream(ctx)
		},
	})
	closer := closerFunc(func() error {
		_ = ft.Close()
		_ = session.CloseWithError(0, "closed")
		return dialer.Close()
	})
	return ft, closer, nil
}

func cloneTLSConfig(config *tls.Config) *tls.Config {
	if config == nil {
		config = &tls.Config{}
	}
	return config.Clone()
}

func multiStreamGuard(guard etp.SlowlorisConfig) etp.SlowlorisConfig {
	if guard.LengthTimeout == 0 && guard.FrameGrace == 0 && guard.MinReadRate == 0 {
		guard = etp.DefaultSlowlorisConfig()
	}
	guard.DisableDeadlines = true
	return guard
}

type closerFunc func() error

func (f closerFunc) Close() error { return f() }
