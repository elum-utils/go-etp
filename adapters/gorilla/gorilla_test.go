package gorilla

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	etp "github.com/elum-utils/go-etp"
	"github.com/gorilla/websocket"
)

func TestAdapterRoutesChunkedTransfer(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 96*1024)
	received := make(chan []byte, 1)

	app := etp.New(etp.Config{MaxMemoryBody: 4 << 10})
	if err := app.On("attach.upload", func(ctx *etp.Context) error {
		reader, err := ctx.Body.Open()
		if err != nil {
			return err
		}
		defer reader.Close()
		data, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		received <- data
		return nil
	}); err != nil {
		t.Fatalf("on: %v", err)
	}

	server := httptest.NewServer((&Adapter{}).Handler(app))
	defer server.Close()

	wsURL := "ws" + server.URL[len("http"):]
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	clientConfig := etp.DefaultClientConfig()
	clientConfig.Payload.MaxInlineBodyBytes = 8
	client := etp.NewSessionWithConfig(NewTransport(conn), clientConfig)

	clientCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clientDone := make(chan error, 1)
	go func() {
		err := client.Run(clientCtx)
		if errors.Is(err, context.Canceled) || clientCtx.Err() != nil {
			clientDone <- nil
			return
		}
		clientDone <- err
	}()
	if err := client.SendHello(etp.RoleClient); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	waitSessionEstablished(t, client)

	handle, err := client.Request(context.Background(), etp.MessageOptions{
		Event:     "attach.upload",
		Data:      body,
		Field:     "file",
		Name:      "chunked.bin",
		ChunkSize: 8 * 1024,
	})
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if handle.TransferID == 0 {
		t.Fatalf("expected transfer path")
	}
	if err := <-handle.Done(); err != nil {
		t.Fatalf("transfer done: %v", err)
	}

	select {
	case got := <-received:
		if !bytes.Equal(got, body) {
			t.Fatalf("received body mismatch: got %d bytes, want %d", len(got), len(body))
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for transfer handler")
	}

	cancel()
	_ = conn.Close()
	<-clientDone
}

func waitSessionEstablished(t *testing.T, session *etp.Session) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for session.State() != etp.SessionEstablished {
		if state := session.State(); state == etp.SessionFailed || time.Now().After(deadline) {
			t.Fatalf("session did not become established: %s", state)
		}
		time.Sleep(time.Millisecond)
	}
}
