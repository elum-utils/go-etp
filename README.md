# go-etp

Go reference implementation of the Elum Transport Protocol core.

This package contains the protocol/session layer only:

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
- transfer resume primitives;
- stream slowloris guard.

It intentionally does not contain the higher-level application API:

- router;
- `server.On`;
- middleware groups;
- `Request.Validate`;
- `elum.Body` app binding;
- React/Solid/Swift/Android SDK code.

The wire specification lives in [docs/RFC.md](docs/RFC.md).

## Import

```go
import etp "github.com/elum-utils/go-etp"
```

```go
client := etp.NewSessionWithConfig(transport, etp.DefaultClientConfig())
server := etp.NewSessionWithConfig(transport, etp.DefaultServerConfig())
```

## Test

```bash
go test ./...
```
