package tls

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"

	etp "github.com/elum-utils/go-etp"
)

type Adapter struct {
	Addr      string
	Net       string
	TLSConfig *tls.Config
	Guard     etp.SlowlorisConfig
}

func New(addr string, tlsConfig *tls.Config) *Adapter {
	return &Adapter{Addr: addr, Net: "tcp", TLSConfig: tlsConfig}
}

func (a *Adapter) Name() string { return "tls" }

func (a *Adapter) Serve(ctx context.Context, app *etp.App) error {
	network := a.Net
	if network == "" {
		network = "tcp"
	}
	listener, err := tls.Listen(network, a.Addr, a.TLSConfig)
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
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return ctx.Err()
			}
			return err
		}
		wg.Add(1)
		go func(conn net.Conn) {
			defer wg.Done()
			defer conn.Close()
			_, _ = app.ServeTransportWithRemote(ctx, a.Name(), conn.RemoteAddr().String(), etp.NewStreamTransportForStream(conn, a.Guard))
		}(conn)
	}
}

type Dialer struct {
	Addr      string
	Net       string
	TLSConfig *tls.Config
	Guard     etp.SlowlorisConfig
}

func Dial(ctx context.Context, addr string, tlsConfig *tls.Config) (etp.FrameTransport, io.Closer, error) {
	return Dialer{Addr: addr, TLSConfig: tlsConfig}.Dial(ctx)
}

func (d Dialer) Dial(ctx context.Context) (etp.FrameTransport, io.Closer, error) {
	network := d.Net
	if network == "" {
		network = "tcp"
	}
	dialer := tls.Dialer{Config: d.TLSConfig}
	conn, err := dialer.DialContext(ctx, network, d.Addr)
	if err != nil {
		return nil, nil, err
	}
	return etp.NewStreamTransportForStream(conn, d.Guard), conn, nil
}
