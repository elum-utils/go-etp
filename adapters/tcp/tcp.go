package tcp

import (
	"context"
	"errors"
	"net"
	"sync"

	etp "github.com/elum-utils/go-etp"
)

type Adapter struct {
	Addr string
	Net  string
}

func New(addr string) *Adapter {
	return &Adapter{Addr: addr, Net: "tcp"}
}

func (a *Adapter) Name() string { return "tcp" }

func (a *Adapter) Serve(ctx context.Context, app *etp.App) error {
	network := a.Net
	if network == "" {
		network = "tcp"
	}
	listener, err := net.Listen(network, a.Addr)
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
			_, _ = app.ServeTransportWithRemote(ctx, a.Name(), conn.RemoteAddr().String(), etp.NewStreamTransport(conn))
		}(conn)
	}
}
