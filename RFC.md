# Elum Transport Protocol RFC

Status: MVP  
Protocol name: `elum-protocol`  
Wire version: `1`  
Primary goals: realtime messaging, request/response, safe large payload delivery, unified handler API.

## 1. Overview

Elum Transport Protocol is a binary session protocol for application messages over WebSocket, TCP, QUIC, and WebTransport adapters.

The protocol separates:

- frame transport: ordered binary frames over an adapter;
- session control: hello, auth, heartbeat, close, errors;
- application messaging: event-based request/response;
- body delivery: inline bodies for small data and chunked transfer for large data;
- safety controls: flow control, rate limits, slowloris detection, cancellation, retry, checksum, graceful close.

Upper application code MUST NOT depend on the concrete adapter or on whether a body arrived inline or as a chunked transfer.

Application handlers SHOULD see a single logical message:

```go
type Upload struct {
    Dialog string    `msgpack:"dialog" validate:"required,uuid"`
    Image  elum.Body `msgpack:"image" validate:"required"`
}
```

Small `Image` data MAY arrive inside a `FrameRequest`. Large `Image` data MAY arrive as `TransferBegin + FrameData + TransferEnd`. In both cases the server handler receives `elum.Body`.

## 2. Terminology

- Endpoint: one side of a protocol connection.
- Adapter: the lower transport implementation, for example WebSocket, TCP, QUIC, or WebTransport.
- Session: stateful protocol connection over an adapter.
- Frame: one binary protocol unit with a fixed 40-byte header and variable payload.
- Event: application route name, for example `message.get` or `attach.upload`.
- Request ID: unsigned 64-bit identifier used for request/response correlation.
- Transfer ID: unsigned 64-bit identifier used for chunked body transfer correlation.
- Body: application binary field that can be inline or streamed.
- Part: one named body segment inside a unified chunked message.
- Field: lightweight string metadata attached to a request or transfer.

## 3. Byte Order

All numeric fields in frame headers and protocol payloads are encoded in big-endian byte order.

Strings and binary payloads are length-prefixed unless they are the remaining bytes of a frame payload.

## 4. Frame Header

Every frame starts with a fixed 40-byte header.

| Offset | Size | Type | Name | Description |
| --- | ---: | --- | --- | --- |
| 0 | 1 | uint8 | Version | Wire version. Current value is `1`. |
| 1 | 1 | uint8 | FrameType | Frame type identifier. |
| 2 | 2 | uint16 | Flags | Frame flags. |
| 4 | 1 | uint8 | Priority | Scheduler priority. Lower value means higher priority. |
| 5 | 1 | uint8 | HeaderLength | Current value is `40`. |
| 6 | 2 | uint16 | ChannelID | Logical channel. |
| 8 | 2 | uint16 | PayloadOffset | Current value is `40`. |
| 10 | 2 | uint16 | HeaderFlags | Reserved for future header extensions. |
| 12 | 4 | uint32 | PayloadLength | Number of payload bytes. |
| 16 | 4 | uint32 | SchemaID | Payload schema identifier. |
| 20 | 8 | uint64 | RequestID | Request/response correlation ID. |
| 28 | 8 | uint64 | TransferID | Transfer correlation ID. |
| 36 | 4 | uint32 | ChunkID | Chunk sequence number for `FrameData`. |

Endpoints MUST reject frames with unsupported `Version`, invalid header length, invalid payload offset, payload length mismatch, or payload length above configured limits.

## 5. Frame Types

| ID | Name | Direction | Purpose |
| ---: | --- | --- | --- |
| 1 | `FrameData` | both | Text payload or transfer chunk payload. |
| 2 | `FrameAck` | both | Acknowledge received transfer chunks. |
| 3 | `FrameNack` | both | Request retransmit or signal transfer failure. |
| 4 | `FramePing` | both | Heartbeat ping. |
| 5 | `FramePong` | both | Heartbeat pong. |
| 6 | `FrameWindow` | both | Receiver-advertised transfer window. |
| 7 | `FrameCancel` | both | Cancel transfer. |
| 8 | `FrameCancelAck` | both | Acknowledge transfer cancellation. |
| 9 | `FrameHello` | both | Capability and limit announcement. |
| 10 | `FrameHelloAck` | both | Hello acknowledgement. |
| 11 | `FrameClose` | both | Close request. |
| 12 | `FrameTransferBegin` | both | Start chunked body message. |
| 13 | `FrameTransferEnd` | both | End chunked body message. |
| 14 | `FrameTransferState` | both | Transfer state/progress metadata. |
| 15 | `FrameAuth` | client to server | Authentication request. |
| 16 | `FrameAuthAccept` | server to client | Authentication accepted. |
| 17 | `FrameAuthReject` | server to client | Authentication rejected. |
| 18 | `FrameRequest` | both | Application request/event. |
| 19 | `FrameResponse` | both | Application response/event. |
| 20 | `FrameError` | both | Formal protocol/application error. |
| 21 | `FrameGoAway` | both | Stop accepting new work, graceful shutdown. |
| 22 | `FrameCloseAck` | both | Close acknowledgement. |
| 23 | `FrameTransferResume` | both | Resume transfer after reconnect. |

## 6. Schema IDs

| ID | Name | Used by |
| ---: | --- | --- |
| 1 | `SchemaHello` | `FrameHello`, `FrameHelloAck` |
| 100 | `SchemaTextMessage` | text `FrameData` |
| 200 | `SchemaTransferBegin` | `FrameTransferBegin`, `FrameTransferEnd` |
| 201 | `SchemaAck` | `FrameAck` |
| 202 | `SchemaCancel` | `FrameCancel`, `FrameCancelAck` |
| 203 | `SchemaNack` | `FrameNack` |
| 204 | `SchemaAuth` | `FrameAuth` |
| 205 | `SchemaAuthResult` | `FrameAuthAccept`, `FrameAuthReject` |
| 300 | `SchemaEvent` | `FrameRequest`, `FrameResponse` |
| 400 | `SchemaError` | `FrameError` |
| 401 | `SchemaGoAway` | `FrameGoAway` |
| 402 | `SchemaClose` | `FrameClose`, `FrameCloseAck` |
| 500 | `SchemaWindow` | `FrameWindow` |
| 501 | `SchemaTransferState` | `FrameTransferState` |

## 7. Flags, Priorities, and Channels

Frame flags:

| Bit | Name | Meaning |
| ---: | --- | --- |
| 0 | `FlagFirst` | First frame in a logical sequence. |
| 1 | `FlagLast` | Last frame in a logical sequence. |
| 2 | `FlagAckRequest` | Sender asks receiver to acknowledge. |
| 3 | `FlagEncrypted` | Payload is encrypted. Reserved for future use. |
| 4 | `FlagCompressed` | Payload is compressed. Reserved for future use. |
| 5 | `FlagControl` | Control frame. |

Priorities:

| Value | Name |
| ---: | --- |
| 0 | Critical |
| 1 | High |
| 2 | Normal |
| 3 | Low |
| 4 | Idle |

Channels:

| Value | Name | Typical use |
| ---: | --- | --- |
| 0 | Control | auth, hello, close, error |
| 1 | Realtime | chat messages, presence, request/response |
| 2 | Bulk | large bodies/files |
| 3 | Sync | synchronization |
| 4 | Background | low-priority background work |

Schedulers SHOULD prefer lower priority values and SHOULD avoid letting bulk traffic block realtime/control frames.

## 8. Capabilities

Capabilities are announced in `Hello`.

| Bit | Name |
| ---: | --- |
| 0 | Transfers |
| 1 | Cancel |
| 2 | Ack |
| 3 | Nack |
| 4 | Heartbeat |
| 5 | Transfer SHA-256 |
| 6 | Flow control |
| 7 | Slowloris guard |
| 8 | Protocol events |
| 9 | Request/response |
| 10 | Graceful close |
| 11 | Transfer resume |
| 12 | Transfer commit |

Endpoints MUST NOT use a feature that the remote endpoint did not advertise.

## 9. Session Lifecycle

The normal lifecycle is:

1. Adapter connection is established.
2. Optional authentication is performed if required by the server.
3. Endpoints exchange `Hello` / `HelloAck`.
4. Application messages and transfers are exchanged.
5. Either endpoint sends graceful close, immediate close, or the adapter disconnects.

If auth is required, application frames before successful auth MUST be rejected.

Go implementations SHOULD use the exported role constants:

```go
etp.RoleClient
etp.RoleServer
```

Convenience constructors are provided for the default behavior:

```go
etp.DefaultClientConfig()
etp.DefaultServerConfig()
```

## 10. Hello Payload

`Hello` payload:

| Field | Type |
| --- | --- |
| Capabilities | uint64 |
| MaxFrameBytes | uint32 |
| MaxChunkSize | uint32 |
| MaxTransferBytes | uint64 |
| MaxInFlightChunks | uint32 |
| HeartbeatMillis | uint32 |
| Reserved | uint32 |
| RoleLength | uint32 |
| Role | bytes |

The receiver SHOULD store remote capabilities and limits. Effective transfer limits SHOULD be the stricter of local and remote limits.

## 11. Authentication

Auth request payload:

| Field | Type |
| --- | --- |
| Method | uint16 |
| Flags | uint16 |
| AuthSchemaID | uint32 |
| PayloadLength | uint32 |
| Payload | bytes |

Auth methods:

| Value | Name |
| ---: | --- |
| 1 | Bearer |
| 2 | API key |
| 3 | Session |
| 255 | Custom |

Server responses:

- `FrameAuthAccept`: contains accepted user ID.
- `FrameAuthReject`: contains status code, reason code, and message.

Reject codes:

| Value | Meaning |
| ---: | --- |
| 401 | Unauthorized |
| 403 | Forbidden |
| 408 | Auth timeout |
| 413 | Auth payload too large |
| 440 | Auth protocol error |

Server implementations SHOULD enforce:

- auth timeout;
- max auth payload size;
- max auth attempts;
- no application frames before auth accept.

## 12. Request/Response

Application requests use `FrameRequest` with `SchemaEvent`.

Payload format:

| Field | Type |
| --- | --- |
| EventLength | uint32 |
| Event | bytes |
| DataLength | uint32 |
| Data | bytes |
| Fields | optional transfer field block |

`RequestID` in the frame header identifies a request. A response MUST use `FrameResponse` with the same `RequestID`.

Event names are UTF-8 strings by convention. Recommended naming style is dot-separated: `message.get`, `attach.upload`, `typing.start`.

`Data` is opaque to the transport. The current application layer uses MessagePack for structured payloads.

Small bodies MAY be encoded inline in `Data` as MessagePack `bin` values. Large bodies SHOULD use chunked body transfer.

## 13. Unified Body Model

The application API treats inline and streamed data as the same logical type: `elum.Body`.

Frontend send API SHOULD be:

```ts
await socket.send("attach.upload", {
  dialog,
  image: fileOrBlobOrUint8Array,
})
```

The client chooses transport form:

- if encoded payload size is less than or equal to `MaxInlineBodyBytes`, send `FrameRequest`;
- otherwise send `FrameTransferBegin`, one or more `FrameData`, then `FrameTransferEnd`.

The server handler MUST NOT branch on inline vs stream:

```go
var data Upload
if !req.Validate(&data) {
    return req.ValidationError()
}
return data.Image.Accept(writer)
```

For inline body:

- `Body.IsInline()` returns true;
- `Body.Accept(writer)` writes bytes immediately;
- `Body.Bytes()` returns a copy.

For streamed body:

- `Body.IsStream()` returns true;
- `Body.Accept(writer)` registers the writer before data chunks are delivered;
- `Body.Bytes()` returns an error.

## 14. Chunked Body Transfer

Chunked body transfer is used for large logical messages. It sends one application event as one transfer, not as many separate file events.

`TransferID` values used by `FrameTransferBegin` MUST be non-zero, strictly increasing, and MUST NOT be reused within one session, including after cancellation or completion. Reconnect resume uses `FrameTransferResume` with the original ID; it does not send another `FrameTransferBegin`.

Sequence:

1. `FrameTransferBegin`
2. `FrameData` chunks with the same `TransferID`
3. `FrameTransferEnd`

`FrameTransferBegin` payload:

| Field | Type |
| --- | --- |
| TotalSize | uint64 |
| ChunkSize | uint32 |
| ChunkCount | uint32 |
| ContentType | uint32 |
| Flags | uint32 |
| Checksum | 32 bytes |
| NameLength | uint32 |
| Name | bytes |
| EventLength | uint32 |
| Event | bytes |
| FieldLength | uint32 |
| Field | bytes |
| Index | uint32 |
| Parts | transfer part block |
| Fields | transfer field block |

`Field` and `Index` are used for single-body transfer compatibility. `Parts` is the preferred representation for unified multipart messages.

Transfer part format:

| Field | Type |
| --- | --- |
| PartCount | uint32 |
| FieldLength | uint32 |
| Field | bytes |
| Index | uint32 |
| NameLength | uint32 |
| Name | bytes |
| TotalSize | uint64 |
| ContentType | uint32 |

For each part, `Field` matches the application struct tag, and `Index` is used for slices.

Example:

```text
event = "attach.upload"
fields = { dialog: "..." }
parts = [
  { field: "image",  index: 0, name: "first.jpg",  totalSize: 10485760 },
  { field: "image2", index: 0, name: "second.jpg", totalSize: 2048 }
]
stream bytes = bytes(image) || bytes(image2)
```

The receiver demultiplexes the single byte stream into application body writers according to part order and `TotalSize`.

## 15. Transfer Data

Each `FrameData` chunk in a transfer MUST set:

- `TransferID` to the active transfer;
- `ChunkID` to the zero-based chunk number;
- payload to chunk bytes.

Payload size MUST NOT exceed negotiated or configured max chunk size.

The final byte count across all chunks MUST equal `TransferBegin.TotalSize`. If fewer bytes arrive, receiver MUST fail the transfer. If more bytes arrive, receiver MUST fail the transfer.

## 16. Ack, Nack, Retry

Receivers SHOULD acknowledge received chunk ranges with `FrameAck`. An ACK means the bytes were successfully committed by `IncomingTransferWriter.Write`; merely accepting a chunk into an in-memory queue is not sufficient.

Ack payload:

| Field | Type |
| --- | --- |
| TransferID | uint64 |
| ChunkFrom | uint32 |
| ChunkTo | uint32 |
| ReceivedBytes | uint64 |

Senders SHOULD retain in-flight chunks until acknowledged. If ack timeout expires, sender SHOULD retry missing chunks until retry limit is reached.

Receivers MAY send `FrameNack` for missing chunks, protocol errors, write failures, checksum failures, or bad state.

If retry limit is reached, sender MUST fail the transfer and surface the error to upper layers.

After all chunks are ACKed, the sender sends `FrameTransferEnd`. Success is final only after the receiver durably closes/commits its writer and replies with `FrameTransferState(Completed)`. The sender MUST retry `FrameTransferEnd` until that terminal state arrives or the retry limit is exhausted. This behavior requires `CapabilityTransferCommit`.

## 17. Flow Control

Flow control prevents unbounded memory use and prevents large transfers from blocking realtime traffic.

Configurable values include:

- max concurrent transfers;
- max in-flight chunks;
- max in-flight bytes;
- max send buffer bytes;
- max receive buffer bytes;
- max transfer bytes;
- max chunk size;
- ack timeout;
- retry limit;
- transfer open timeout;
- transfer commit timeout;
- completed transfer cache size and TTL.

Receivers MAY advertise available capacity via `FrameWindow`. Senders MUST respect the most recent remote window when flow control capability is enabled.

Bulk body chunks SHOULD use low priority and bulk channel. Control and realtime frames SHOULD remain schedulable while large transfers are active.

## 18. Cancellation

Either endpoint MAY cancel an active transfer with `FrameCancel`.

Cancelled transfers SHOULD NOT be retained as long-lived state. If a later chunk arrives for an unknown or cancelled transfer, receiver SHOULD reject it with `Nack` or protocol error and MAY report a protocol event to the application policy layer.

Cancellation MUST be acknowledged with `FrameCancelAck` when cancel capability is enabled.

## 19. Checksum

If `TransferFlagChecksumSHA256` is set, `TransferBegin.Checksum` contains SHA-256 of the full transfer byte stream.

Receivers MUST verify the checksum after receiving all bytes. On mismatch, receiver MUST fail the transfer and send a negative acknowledgement or error.

Checksum is optional. Implementations MAY disable it for optimized trusted clients after conformance and fuzz testing.

## 20. Resume

Transfer resume uses `FrameTransferResume` and MUST be capability-negotiated.

The receiver may decide whether it can resume a partially received transfer. If accepted, it returns the authoritative committed byte count and next expected chunk. Both values MUST describe the same chunk boundary and MUST fit the original transfer metadata. The sender reopens its source at that byte offset and resumes from the returned chunk.

Resume storage is application-defined. Store callbacks are bounded by the transfer-open timeout; an implementation MUST keep a timed-out callback counted against the concurrent-open limit until it actually returns. Tokens passed to an asynchronous store MUST remain stable after the input frame is released.

## 21. Heartbeat

Heartbeat is idle-based. Any valid incoming frame counts as connection activity and refreshes the receiver idle timeout. `FramePing` is only a lightweight activity frame used when a client has no real application data to send.

Client behavior:

- if no outgoing frame was sent for `Heartbeat.Interval`, client SHOULD send `FramePing`;
- any ordinary outgoing application/control frame resets this ping timer;
- client SHOULD treat lack of incoming frames for `Heartbeat.Timeout` as connection failure.

Server behavior:

- server MUST answer `FramePing` with `FramePong`;
- server SHOULD NOT send heartbeat pings by default;
- server SHOULD disconnect or fail the session if no incoming frame of any kind is received for `Heartbeat.Timeout`.

If heartbeat timeout expires, the session SHOULD close and surface a protocol event. This is not limited to missing pongs: any incoming frame keeps the session alive.

Default values in the Go implementation:

- interval: 10 seconds;
- timeout: 20 seconds.

## 22. Rate Limits

Endpoints SHOULD enforce:

- frames per second;
- bytes per second;
- auth attempts;
- bad frames per second.

On rate limit violation, endpoint SHOULD send `FrameError` with `ErrorRateLimited` where possible, then close if the violation continues or is severe.

Default Go implementation values:

- max frames per second: 4096;
- max bytes per second: 64 MiB;
- max auth attempts: 3;
- max bad frames per second: 64.

## 23. Slowloris Protection

Stream adapters SHOULD enforce slowloris protection while reading frame headers and payloads.

Protection parameters:

- header timeout;
- minimum read rate;
- maximum frame bytes.

If an endpoint sends too slowly, the adapter SHOULD return a slowloris error, close the connection, and surface a protocol event for upper-layer policy decisions.

Application policy may choose to ban, mark, throttle, or ignore the peer.

## 24. Error Handling

Error codes:

| Value | Name |
| ---: | --- |
| 1 | Protocol violation |
| 2 | Unauthorized |
| 3 | Unsupported feature |
| 4 | Frame too large |
| 5 | Bad state |
| 6 | Rate limited |
| 7 | Server shutdown |
| 8 | Drain timeout |
| 9 | Invalid request |
| 10 | Internal |

Protocol violations include:

- unsupported version;
- invalid frame length;
- unknown required schema;
- application frame before auth;
- chunk for unknown transfer;
- duplicate active transfer ID;
- transfer exceeding configured limits;
- checksum mismatch.

## 25. Graceful Close

Close reason codes:

| Value | Name |
| ---: | --- |
| 0 | Normal |
| 1 | Protocol violation |
| 2 | Auth failed |
| 3 | Server shutdown |
| 4 | Client shutdown |
| 5 | Drain timeout |

Close flags:

| Bit | Name |
| ---: | --- |
| 0 | Immediate |
| 1 | Drain |
| 2 | No new requests |
| 3 | No new transfers |

In drain mode, endpoint SHOULD reject new work while allowing active transfers to finish until timeout. Close completes only after `FrameCloseAck` is physically written and received. Simultaneous close is valid: both endpoints MUST acknowledge the peer close without deadlocking and then converge on `Closed`.

## 26. Security Requirements

Implementations MUST:

- enforce payload and frame size limits;
- enforce auth timeout when auth is required;
- reject application frames before auth;
- enforce max transfer size;
- enforce max chunk size;
- limit concurrent transfers;
- avoid storing cancelled transfers indefinitely;
- handle unknown transfer chunks safely;
- avoid unbounded buffering of streamed bodies;
- reject non-canonical headers, reserved bits, overflowing lengths, and trailing payload bytes;
- keep opening, timed-out, and stuck-commit transfers counted against configured concurrency limits;
- bound send queues, handler queues, completed-transfer caches, and multistream prefaces;
- recover panics from application handlers, auth/storage callbacks, transfer readers/writers, and notification callbacks;
- surface protocol events to upper policy layer.

Implementations SHOULD:

- run binary decoder fuzz tests;
- keep golden conformance vectors;
- verify checksums for untrusted clients;
- prefer streaming to memory for large bodies;
- isolate policy decisions from protocol mechanics.

## 27. Frontend Sending Algorithm

Frontend clients SHOULD expose one API:

```ts
await socket.send(event, data, options)
```

Algorithm:

1. Walk `data`.
2. Treat primitive fields as inline structured fields.
3. Treat `File`, `Blob`, `ArrayBuffer`, `Uint8Array`, and readable byte streams as body candidates.
4. Estimate encoded inline size.
5. If all body candidates fit `MaxInlineBodyBytes`, encode one `FrameRequest` with MessagePack data.
6. Otherwise, create `TransferPart` for each body candidate and send one chunked transfer.
7. Report progress using total bytes and acknowledged bytes.
8. Honor cancellation via `AbortSignal`.

The client MUST preserve field names so that server-side `msgpack` tags map correctly to `elum.Body`.

## 28. Server Handler Model

The server application layer SHOULD expose:

```go
server.On("attach.upload", func(ctx *elum.Context, req *elum.Request) error {
    var data Upload
    if !req.Validate(&data) {
        return req.ValidationError()
    }
    return data.Image.Accept(writer)
})
```

Handlers SHOULD NOT need separate transfer callbacks for normal application bodies.

Policy hooks such as auth, ban decisions, bad-frame handling, and slowloris decisions remain above the protocol layer.

Application handlers and context-aware transfer writers SHOULD honor cancellation promptly. The Go runtime does not wait forever for an uncooperative callback during session shutdown; such calls remain bounded by configured handler workers or transfer slots until they return.

## 29. Default Limits

Default Go implementation limits:

| Name | Value |
| --- | ---: |
| Chunk size | 16 KiB default send chunk, 64 KiB max chunk |
| Max concurrent transfers | 8 |
| Max in-flight chunks | 16 |
| Max in-flight bytes | 8 MiB |
| Max send buffer | 16 MiB |
| Max receive buffer | 32 MiB |
| Max transfer size | 512 MiB |
| Ack timeout | 3 seconds |
| Retry limit | 3 |
| Transfer open timeout | 5 seconds |
| Transfer commit timeout | 30 seconds |
| Completed transfer cache | 1024 entries, 1 minute TTL |
| Max frame | 8 MiB |
| Max text payload | 1 MiB |
| Max request payload | 1 MiB |
| Max response payload | 1 MiB |
| Auth timeout | 5 seconds |
| Auth max payload | 16 KiB |
| Heartbeat idle ping interval | 10 seconds |
| Heartbeat idle timeout | 20 seconds |

Deployments SHOULD override these limits according to product requirements.

## 30. Conformance

A conforming implementation MUST pass:

- frame header encode/decode tests;
- payload encode/decode tests;
- request/response correlation tests;
- auth accept/reject/timeout tests;
- inline body tests;
- chunked body tests;
- multipart body demux tests;
- cancel and late-chunk tests;
- ack/nack/retry tests;
- flow control tests;
- slowloris tests;
- rate limit tests;
- capability and state-machine enforcement tests;
- disconnect, simultaneous-close, drain, and resume tests;
- concurrent transfer-finalization tests;
- race-detector tests;
- fuzz tests for binary decoders.

Language clients SHOULD share golden vectors for:

- `Frame`;
- `Hello`;
- `AuthRequest`;
- `EventMessage`;
- `TransferBegin`;
- `TransferPart`;
- `Ack`;
- `Nack`;
- `Window`;
- `Close`;
- `Error`.

## 31. Non-Goals

This protocol does not define:

- business-level authorization policy;
- ban policy;
- application event schema;
- persistent transfer storage backend;
- encryption format beyond reserved frame flags;
- compression format beyond reserved frame flags.

These concerns belong to the application or adapter layer.

## 32. Open Implementation Notes

The Go reference implementation implements the protocol behavior described above. Work outside the core wire/session contract remains:

- public conformance vectors for TS/Swift/Java;
- production WebTransport interoperability tests;
- documented frontend package API;
- benchmarks for inline vs chunked thresholds;
- explicit `MaxInlineBodyBytes` configuration in the app layer.
