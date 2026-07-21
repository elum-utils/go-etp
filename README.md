# go-etp

Go implementation of the Elum Transport Protocol and its application server layer.

The root package exposes `App`, compiled routing, middleware groups, peers, unified
request/response and body handling. The wire/session implementation lives in
`internal/etp` and is exposed through the root package where low-level integration
is required.

The protocol includes:

- binary frame header encode/decode;
- payload encode/decode;
- capabilities and schema constants;
- session runtime;
- auth frames;
- heartbeat;
- request/response frames;
- chunked transfers;
- ack/nack/retry;
- flow control;
- cancellation;
- checksum;
- graceful close;
- transfer resume;
- terminal transfer commit confirmation;
- bounded send/receive queues and configurable per-session handler workers;
- strict decoder, capability, rate-limit, and slowloris enforcement;
- panic isolation at application callback boundaries;
- stream slowloris guard.

The wire specification lives in [RFC.md](RFC.md).

## Import

```go
import etp "github.com/elum-utils/go-etp"
```

```go
app := etp.New(etp.Config{})
app.Use("message.*", func(next etp.Handler) etp.Handler {
	return func(ctx *etp.Context) error {
		return next(ctx)
	}
})
app.On("message.send", func(ctx *etp.Context) error {
	body, ok := ctx.BodyView()
	if !ok {
		var err error
		body, err = ctx.Bytes()
		if err != nil {
			return err
		}
	}
	_, err := ctx.Respond(etp.SendOptions{Event: "message.sent", Data: body})
	return err
})
```

`Context` and `BodyView` are valid only while the handler is running. Use
`EventCopy` or `Bytes` when data must outlive the callback.

## Adapters

Adapters are independent nested modules, so an application downloads only the
network stack it imports:

- `github.com/elum-utils/go-etp/adapters/gorilla`
- `github.com/elum-utils/go-etp/adapters/fiber`
- `github.com/elum-utils/go-etp/adapters/nbio`
- `github.com/elum-utils/go-etp/adapters/tcp`
- `github.com/elum-utils/go-etp/adapters/tls`
- `github.com/elum-utils/go-etp/adapters/quic`
- `github.com/elum-utils/go-etp/adapters/webtransport`

Large bodies are selected automatically by `Session.Send`, `Request`, and
`Respond`: inline payloads use one request frame while larger or streaming bodies
use transfer begin/data/end frames.

## Test

```bash
go test ./...
go test -race ./...
```

Each adapter is tested from its own module directory.
