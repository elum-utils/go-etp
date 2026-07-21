package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"sync"

	etp "github.com/elum-utils/go-etp"
	"github.com/quic-go/quic-go"
)

const DefaultALPN = "elum-etp"

type Adapter struct {
	Addr      string
	TLSConfig *tls.Config
	Config    *quic.Config
	Guard     etp.SlowlorisConfig
}

func New(addr string, tlsConfig *tls.Config) *Adapter {
	return &Adapter{Addr: addr, TLSConfig: tlsConfig}
}

func (a *Adapter) Name() string { return "quic" }

func (a *Adapter) Serve(ctx context.Context, app *etp.App) error {
	tlsConfig := cloneTLSConfig(a.TLSConfig)
	listener, err := quic.ListenAddr(a.Addr, tlsConfig, a.Config)
	if err != nil {
		return err
	}
	defer listener.Close()

	var wg sync.WaitGroup
	defer wg.Wait()
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, quic.ErrServerClosed) {
				return ctx.Err()
			}
			return err
		}
		wg.Add(1)
		go func(conn *quic.Conn) {
			defer wg.Done()
			a.serveConn(ctx, app, conn)
		}(conn)
	}
}

func (a *Adapter) serveConn(ctx context.Context, app *etp.App, conn *quic.Conn) {
	defer conn.CloseWithError(0, "closed")
	ft := etp.NewMultiStreamTransport(etp.MultiStreamTransportConfig{
		Context: ctx,
		Guard:   multiStreamGuard(a.Guard),
		OpenStream: func(ctx context.Context) (etp.DeadlineStream, error) {
			return conn.OpenStreamSync(ctx)
		},
		AcceptStream: func(ctx context.Context) (etp.DeadlineStream, error) {
			return conn.AcceptStream(ctx)
		},
	})
	_, _ = app.ServeTransportWithRemote(ctx, a.Name(), conn.RemoteAddr().String(), ft)
}

type Dialer struct {
	Addr      string
	TLSConfig *tls.Config
	Config    *quic.Config
	Guard     etp.SlowlorisConfig
}

func Dial(ctx context.Context, addr string, tlsConfig *tls.Config) (etp.FrameTransport, io.Closer, error) {
	return Dialer{Addr: addr, TLSConfig: tlsConfig}.Dial(ctx)
}

func (d Dialer) Dial(ctx context.Context) (etp.FrameTransport, io.Closer, error) {
	tlsConfig := cloneTLSConfig(d.TLSConfig)
	conn, err := quic.DialAddr(ctx, d.Addr, tlsConfig, d.Config)
	if err != nil {
		return nil, nil, err
	}
	ft := etp.NewMultiStreamTransport(etp.MultiStreamTransportConfig{
		Context: ctx,
		Guard:   multiStreamGuard(d.Guard),
		OpenStream: func(ctx context.Context) (etp.DeadlineStream, error) {
			return conn.OpenStreamSync(ctx)
		},
		AcceptStream: func(ctx context.Context) (etp.DeadlineStream, error) {
			return conn.AcceptStream(ctx)
		},
	})
	closer := closerFunc(func() error {
		_ = ft.Close()
		return conn.CloseWithError(0, "closed")
	})
	return ft, closer, nil
}

func cloneTLSConfig(config *tls.Config) *tls.Config {
	if config == nil {
		config = &tls.Config{}
	}
	out := config.Clone()
	if len(out.NextProtos) == 0 {
		out.NextProtos = []string{DefaultALPN}
	}
	return out
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
