package etp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultChunkSize = 16 * 1024
	RoleClient       = "client"
	RoleServer       = "server"
)

type Progress struct {
	TransferID        uint64
	TotalBytes        uint64
	LocalWrittenBytes uint64
	RemoteAckedBytes  uint64
	SentChunks        uint32
	AckedChunks       uint32
	State             TransferState
}

type Session struct {
	t             FrameTransport
	nextRequest   atomic.Uint64
	nextTransfer  atomic.Uint64
	onProgress    func(Progress)
	onEvent       func(ProtocolEvent)
	state         atomic.Uint32
	lastReadNano  atomic.Int64
	lastWriteNano atomic.Int64
	remoteCaps    atomic.Uint64
	mu            sync.Mutex
	handleMu      sync.Mutex
	identity      SessionIdentity
	outgoing      map[uint64]*outgoingTransfer
	incoming      map[uint64]*incomingTransfer
	config        SessionConfig
	rate          rateLimitState
	scheduler     *FrameScheduler
	writer        *sessionWriter
}

type SessionConfig struct {
	Role         string
	Capabilities uint64
	FlowControl  FlowControlConfig
	Heartbeat    HeartbeatConfig
	Auth         AuthConfig
	Receive      ReceiveConfig
	RateLimit    RateLimitConfig
	Resume       ResumeConfig
	Scheduler    SchedulerConfig
	Payload      PayloadLimitConfig
	SendQueue    SendQueueConfig
}

type FlowControlConfig struct {
	MaxConcurrentTransfers int
	MaxInFlightChunks      int
	MaxInFlightBytes       uint64
	MaxSendBufferBytes     uint64
	MaxReceiveBufferBytes  uint64
	AckTimeout             time.Duration
	RetryLimit             int
	MaxTransferBytes       uint64
	MaxChunkSize           int
}

type SessionLimits = FlowControlConfig

type ChecksumMode uint8

const (
	ChecksumOff ChecksumMode = iota
	ChecksumTransferSHA256
)

type HeartbeatConfig struct {
	Interval time.Duration
	Timeout  time.Duration
}

type AuthConfig struct {
	Required        bool
	Timeout         time.Duration
	MaxPayloadBytes uint32
	Handler         AuthHandler
}

type RateLimitConfig struct {
	MaxFramesPerSecond    int
	MaxBytesPerSecond     uint64
	MaxAuthAttempts       int
	MaxBadFramesPerSecond int
}

type PayloadLimitConfig struct {
	MaxTextBytes       uint32
	MaxRequestBytes    uint32
	MaxResponseBytes   uint32
	MaxInlineBodyBytes uint32
}

type rateLimitState struct {
	mu           sync.Mutex
	windowStart  time.Time
	frames       int
	bytes        uint64
	authAttempts int
	badFrames    int
}

type ResumeConfig struct {
	Store TransferResumeStore
}

type TransferResumeStore interface {
	ResumeIncoming(context.Context, TransferResumeView) (TransferResumeDecision, error)
}

type TransferResumeDecision struct {
	Accepted      bool
	Meta          TransferBegin
	Writer        IncomingTransferWriter
	ReceivedBytes uint64
	NextChunk     uint32
	Hash          hash.Hash
	ReasonCode    uint16
}

type SchedulerConfig struct {
	Enabled bool
}

type SendQueueConfig struct {
	Enabled        bool
	MaxFrames      int
	MaxBytes       uint64
	EnqueueTimeout time.Duration
	WriteTimeout   time.Duration
}

type AuthHandler func(context.Context, AuthRequest) (AuthResult, error)

type AuthResult struct {
	OK         bool
	UserID     string
	Attributes []AuthAttribute
	RejectCode uint16
	Reason     string
}

type SessionIdentity struct {
	UserID     string
	Attributes []AuthAttribute
}

type AuthAttribute struct {
	Key   string
	Value string
}

type TransferField struct {
	Key   string
	Value string
}

type ReceiveConfig struct {
	TextHandler     TextHandler
	RequestHandler  RequestHandler
	ResponseHandler ResponseHandler
	TransferHandler TransferHandler
}

type TextHandler func(context.Context, Frame, string) error

type RequestHandler func(context.Context, Frame, EventMessageView) error

type ResponseHandler func(context.Context, Frame, EventMessageView) error

type TransferHandler func(context.Context, IncomingTransferInfo) (IncomingTransferWriter, error)

type IncomingTransferInfo struct {
	TransferID uint64
	Meta       TransferBegin
}

type IncomingTransferWriter interface {
	io.Writer
	Close() error
}

type IncomingTransferAborter interface {
	Abort() error
}

type TransferOptions struct {
	RequestID        uint64
	Event            string
	Field            string
	Index            uint32
	Parts            []TransferPart
	Name             string
	ContentType      uint32
	Reader           io.Reader
	TotalSize        uint64
	Fields           []TransferField
	ChunkSize        int
	DelayPerChunk    time.Duration
	AckTimeout       time.Duration
	RetryLimit       int
	MaxInFlight      int
	MaxInFlightBytes uint64
	Checksum         [32]byte
	ChecksumMode     ChecksumMode
}

type TransferHandle struct {
	TransferID uint64
	done       chan error
	cancelOnce sync.Once
	cancel     context.CancelFunc
	session    *Session
}

type MessageOptions struct {
	Event       string
	Data        []byte
	Fields      []TransferField
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

type MessageHandle struct {
	RequestID  uint64
	TransferID uint64
	done       <-chan error
}

var messageDoneOK = func() <-chan error {
	done := make(chan error)
	close(done)
	return done
}()

func (h MessageHandle) Done() <-chan error {
	if h.done != nil {
		return h.done
	}
	return messageDoneOK
}

type sentChunk struct {
	chunkID       uint32
	frame         Frame
	sentAt        time.Time
	retries       int
	acked         bool
	payloadN      uint64
	pooledPayload *[]byte
}

type outgoingTransfer struct {
	id                 uint64
	totalBytes         uint64
	chunkCount         uint32
	ackTimeout         time.Duration
	retryLimit         int
	maxInFlight        int
	maxInFlightBytes   uint64
	mu                 sync.Mutex
	cond               sync.Cond
	pending            []sentChunk
	pendingBytes       uint64
	remoteWindowBytes  uint64
	remoteWindowChunks uint32
	hasRemoteWindow    bool
	ackedBytes         uint64
	doneSending        bool
	failed             error
}

func NewSession(t FrameTransport) *Session {
	return NewSessionWithConfig(t, DefaultClientConfig())
}

func DefaultClientConfig() SessionConfig {
	return DefaultSessionConfig(RoleClient)
}

func DefaultServerConfig() SessionConfig {
	return DefaultSessionConfig(RoleServer)
}

func DefaultSessionConfig(role string) SessionConfig {
	return SessionConfig{
		Role:         role,
		Capabilities: DefaultCapabilities,
		FlowControl: FlowControlConfig{
			MaxConcurrentTransfers: 8,
			MaxInFlightChunks:      16,
			MaxInFlightBytes:       8 << 20,
			MaxSendBufferBytes:     16 << 20,
			MaxReceiveBufferBytes:  32 << 20,
			AckTimeout:             3 * time.Second,
			RetryLimit:             3,
			MaxTransferBytes:       512 << 20,
			MaxChunkSize:           64 * 1024,
		},
		Heartbeat: HeartbeatConfig{
			Interval: 10 * time.Second,
			Timeout:  20 * time.Second,
		},
		Auth: AuthConfig{
			Timeout:         5 * time.Second,
			MaxPayloadBytes: 16 << 10,
		},
		RateLimit: RateLimitConfig{
			MaxFramesPerSecond:    4096,
			MaxBytesPerSecond:     64 << 20,
			MaxAuthAttempts:       3,
			MaxBadFramesPerSecond: 64,
		},
		Payload: PayloadLimitConfig{
			MaxTextBytes:       1 << 20,
			MaxRequestBytes:    1 << 20,
			MaxResponseBytes:   1 << 20,
			MaxInlineBodyBytes: 64 << 10,
		},
		Scheduler: SchedulerConfig{
			Enabled: true,
		},
		SendQueue: SendQueueConfig{
			Enabled:        true,
			MaxFrames:      4096,
			MaxBytes:       16 << 20,
			EnqueueTimeout: 2 * time.Second,
			WriteTimeout:   5 * time.Second,
		},
	}
}

func NewSessionWithConfig(t FrameTransport, config SessionConfig) *Session {
	config = normalizeSessionConfig(config)
	s := &Session{
		t:         t,
		outgoing:  map[uint64]*outgoingTransfer{},
		incoming:  map[uint64]*incomingTransfer{},
		config:    config,
		scheduler: NewFrameScheduler(),
	}
	if t != nil && config.SendQueue.Enabled {
		s.writer = newSessionWriter(t, config.SendQueue)
	}
	s.nextRequest.Store(1000)
	s.nextTransfer.Store(7000)
	now := time.Now().UnixNano()
	s.lastReadNano.Store(now)
	s.lastWriteNano.Store(now)
	s.remoteCaps.Store(DefaultCapabilities)
	s.setState(SessionNew)
	return s
}

func normalizeSessionConfig(config SessionConfig) SessionConfig {
	defaults := DefaultSessionConfig(config.Role)
	if config.Capabilities == 0 {
		config.Capabilities = defaults.Capabilities
	}
	if config.FlowControl.MaxConcurrentTransfers <= 0 {
		config.FlowControl.MaxConcurrentTransfers = defaults.FlowControl.MaxConcurrentTransfers
	}
	if config.FlowControl.MaxInFlightChunks <= 0 {
		config.FlowControl.MaxInFlightChunks = defaults.FlowControl.MaxInFlightChunks
	}
	if config.FlowControl.MaxInFlightBytes == 0 {
		config.FlowControl.MaxInFlightBytes = defaults.FlowControl.MaxInFlightBytes
	}
	if config.FlowControl.MaxSendBufferBytes == 0 {
		config.FlowControl.MaxSendBufferBytes = defaults.FlowControl.MaxSendBufferBytes
	}
	if config.FlowControl.MaxReceiveBufferBytes == 0 {
		config.FlowControl.MaxReceiveBufferBytes = defaults.FlowControl.MaxReceiveBufferBytes
	}
	if config.FlowControl.AckTimeout <= 0 {
		config.FlowControl.AckTimeout = defaults.FlowControl.AckTimeout
	}
	if config.FlowControl.RetryLimit <= 0 {
		config.FlowControl.RetryLimit = defaults.FlowControl.RetryLimit
	}
	if config.FlowControl.MaxTransferBytes == 0 {
		config.FlowControl.MaxTransferBytes = defaults.FlowControl.MaxTransferBytes
	}
	if config.FlowControl.MaxChunkSize <= 0 {
		config.FlowControl.MaxChunkSize = defaults.FlowControl.MaxChunkSize
	}
	if config.Heartbeat.Interval <= 0 {
		config.Heartbeat.Interval = defaults.Heartbeat.Interval
	}
	if config.Heartbeat.Timeout <= 0 {
		config.Heartbeat.Timeout = defaults.Heartbeat.Timeout
	}
	if config.Auth.Timeout <= 0 {
		config.Auth.Timeout = defaults.Auth.Timeout
	}
	if config.Auth.MaxPayloadBytes == 0 {
		config.Auth.MaxPayloadBytes = defaults.Auth.MaxPayloadBytes
	}
	if config.RateLimit.MaxFramesPerSecond <= 0 {
		config.RateLimit.MaxFramesPerSecond = defaults.RateLimit.MaxFramesPerSecond
	}
	if config.RateLimit.MaxBytesPerSecond == 0 {
		config.RateLimit.MaxBytesPerSecond = defaults.RateLimit.MaxBytesPerSecond
	}
	if config.RateLimit.MaxAuthAttempts <= 0 {
		config.RateLimit.MaxAuthAttempts = defaults.RateLimit.MaxAuthAttempts
	}
	if config.RateLimit.MaxBadFramesPerSecond <= 0 {
		config.RateLimit.MaxBadFramesPerSecond = defaults.RateLimit.MaxBadFramesPerSecond
	}
	if config.Payload.MaxTextBytes == 0 {
		config.Payload.MaxTextBytes = defaults.Payload.MaxTextBytes
	}
	if config.Payload.MaxRequestBytes == 0 {
		config.Payload.MaxRequestBytes = defaults.Payload.MaxRequestBytes
	}
	if config.Payload.MaxResponseBytes == 0 {
		config.Payload.MaxResponseBytes = defaults.Payload.MaxResponseBytes
	}
	if config.Payload.MaxInlineBodyBytes == 0 {
		config.Payload.MaxInlineBodyBytes = defaults.Payload.MaxInlineBodyBytes
	}
	if !config.SendQueue.Enabled &&
		config.SendQueue.MaxFrames == 0 &&
		config.SendQueue.MaxBytes == 0 &&
		config.SendQueue.EnqueueTimeout == 0 &&
		config.SendQueue.WriteTimeout == 0 {
		config.SendQueue.Enabled = defaults.SendQueue.Enabled
	}
	if config.SendQueue.MaxFrames == 0 {
		config.SendQueue.MaxFrames = defaults.SendQueue.MaxFrames
	}
	if config.SendQueue.MaxBytes == 0 {
		config.SendQueue.MaxBytes = defaults.SendQueue.MaxBytes
	}
	if config.SendQueue.EnqueueTimeout <= 0 {
		config.SendQueue.EnqueueTimeout = defaults.SendQueue.EnqueueTimeout
	}
	if config.SendQueue.WriteTimeout <= 0 {
		config.SendQueue.WriteTimeout = defaults.SendQueue.WriteTimeout
	}
	return config
}

func (s *Session) OnProgress(fn func(Progress)) {
	s.onProgress = fn
}

func (s *Session) OnProtocolEvent(fn func(ProtocolEvent)) {
	s.onEvent = fn
}

func (s *Session) State() SessionState {
	return SessionState(s.state.Load())
}

func (s *Session) Config() SessionConfig {
	return s.config
}

func (s *Session) RemoteCapabilities() uint64 {
	return s.remoteCaps.Load()
}

func (s *Session) RemoteSupports(capability uint64) bool {
	return s.remoteCaps.Load()&capability != 0
}

func (s *Session) requireRemoteCapability(capability uint64, name string) error {
	if s.RemoteSupports(capability) {
		return nil
	}
	return fmt.Errorf("remote does not support %s", name)
}

func (s *Session) Identity() SessionIdentity {
	s.mu.Lock()
	defer s.mu.Unlock()
	return SessionIdentity{
		UserID:     s.identity.UserID,
		Attributes: cloneAuthAttributes(s.identity.Attributes),
	}
}

func (s *Session) GetAttribute(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, attr := range s.identity.Attributes {
		if attr.Key == key {
			return attr.Value
		}
	}
	return ""
}

func (s *Session) send(frame Frame) error {
	return s.sendContext(context.Background(), frame)
}

func (s *Session) sendContext(ctx context.Context, frame Frame) error {
	if s.writer == nil {
		if s.config.Scheduler.Enabled && s.scheduler != nil {
			frame = s.scheduler.PushPop(frame)
		}
		err := Send(s.t, frame)
		s.markWrite()
		return err
	}
	size := EncodedFrameSize(frame)
	bufp := getFrameBuffer(size)
	data, err := EncodeFrameInto((*bufp)[:0], frame)
	if err != nil {
		putFrameBuffer(bufp)
		s.markWrite()
		return err
	}
	err = s.writer.enqueue(ctx, sendItem{
		priority: frame.Header.Priority,
		channel:  frame.Header.ChannelID,
		data:     data,
		bufp:     bufp,
	})
	s.markWrite()
	return err
}

func (s *Session) Flush(ctx context.Context) error {
	if s.writer == nil {
		return nil
	}
	return s.writer.flush(ctx)
}

func (s *Session) Authenticate(req AuthRequest) error {
	payload := EncodeAuthRequest(req)
	f := NewFrame(FrameAuth, SchemaAuth, payload)
	f.Header.Priority = PriorityCritical
	f.Header.ChannelID = ChannelControl
	f.Header.Flags = FlagControl | FlagFirst | FlagLast
	if err := s.send(f); err != nil {
		return err
	}
	s.setState(SessionAuthPending)
	return nil
}

func (s *Session) HandleAuth(ctx context.Context, frame Frame) error {
	if uint32(len(frame.Payload)) > s.config.Auth.MaxPayloadBytes {
		return s.RejectAuth(AuthRejectTooLarge, AuthRejectTooLarge, "auth payload too large")
	}
	req, err := DecodeAuthRequestView(frame.Payload)
	if err != nil {
		_ = s.RejectAuth(AuthRejectUnauthorized, AuthRejectProtocol, "invalid auth payload")
		return err
	}
	if s.config.Auth.Handler == nil {
		if s.config.Auth.Required {
			return s.RejectAuth(AuthRejectUnauthorized, AuthRejectUnauthorized, "auth handler is not configured")
		}
		return s.AcceptAuth(AuthResult{OK: true})
	}
	result, err := s.config.Auth.Handler(ctx, req)
	if err != nil {
		_ = s.RejectAuth(AuthRejectUnauthorized, AuthRejectUnauthorized, err.Error())
		return err
	}
	if !result.OK {
		code := result.RejectCode
		if code == 0 {
			code = AuthRejectUnauthorized
		}
		reason := result.Reason
		if reason == "" {
			reason = "unauthorized"
		}
		return s.RejectAuth(code, code, reason)
	}
	return s.AcceptAuth(result)
}

func (s *Session) AcceptAuth(result AuthResult) error {
	s.mu.Lock()
	s.identity = SessionIdentity{
		UserID:     result.UserID,
		Attributes: cloneAuthAttributes(result.Attributes),
	}
	s.mu.Unlock()

	f := NewFrame(FrameAuthAccept, SchemaAuthResult, EncodeAuthAccept(AuthAccept{UserID: result.UserID}))
	f.Header.Priority = PriorityCritical
	f.Header.ChannelID = ChannelControl
	f.Header.Flags = FlagControl | FlagFirst | FlagLast
	if err := s.send(f); err != nil {
		return err
	}
	s.setState(SessionAuthAccepted)
	s.emitEvent(ProtocolEvent{Code: EventAuthAccepted, Message: "auth accepted", FrameType: FrameAuthAccept})
	return nil
}

func (s *Session) RejectAuth(statusCode uint16, reasonCode uint16, message string) error {
	f := NewFrame(FrameAuthReject, SchemaAuthResult, EncodeAuthReject(AuthReject{
		StatusCode: statusCode,
		ReasonCode: reasonCode,
		Message:    message,
	}))
	f.Header.Priority = PriorityCritical
	f.Header.ChannelID = ChannelControl
	f.Header.Flags = FlagControl | FlagFirst | FlagLast
	if err := s.send(f); err != nil {
		return err
	}
	s.setState(SessionAuthRejected)
	s.emitEvent(ProtocolEvent{Code: EventAuthRejected, Message: message, FrameType: FrameAuthReject})
	return nil
}

func (s *Session) HandleAuthAccept(frame Frame) error {
	accept, err := DecodeAuthAccept(frame.Payload)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.identity.UserID = accept.UserID
	s.mu.Unlock()
	s.setState(SessionAuthAccepted)
	s.emitEvent(ProtocolEvent{Code: EventAuthAccepted, Message: "auth accepted", FrameType: frame.Header.FrameType})
	return nil
}

func (s *Session) HandleAuthReject(frame Frame) error {
	reject, err := DecodeAuthReject(frame.Payload)
	if err != nil {
		return err
	}
	s.setState(SessionAuthRejected)
	s.emitEvent(ProtocolEvent{Code: EventAuthRejected, Message: reject.Message, FrameType: frame.Header.FrameType})
	return fmt.Errorf("auth rejected: status=%d reason=%d %s", reject.StatusCode, reject.ReasonCode, reject.Message)
}

func (s *Session) EnsureAuthenticatedFor(frame Frame) error {
	if !s.config.Auth.Required {
		return nil
	}
	switch frame.Header.FrameType {
	case FrameAuth, FrameAuthAccept, FrameAuthReject, FrameClose:
		return nil
	}
	switch s.State() {
	case SessionAuthAccepted, SessionHelloSent, SessionEstablished, SessionDraining:
		return nil
	}
	s.emitEvent(ProtocolEvent{Code: EventAuthRequired, Message: "auth required", FrameType: frame.Header.FrameType})
	return fmt.Errorf("auth required before frame type %d", frame.Header.FrameType)
}

func (s *Session) StartAuthTimeout(ctx context.Context) {
	if !s.config.Auth.Required {
		return
	}
	go func() {
		timer := time.NewTimer(s.config.Auth.Timeout)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		state := s.State()
		switch state {
		case SessionAuthAccepted, SessionHelloSent, SessionEstablished, SessionDraining, SessionClosing, SessionClosed, SessionAuthRejected:
			return
		}
		s.setState(SessionAuthRejected)
		s.emitEvent(ProtocolEvent{Code: EventAuthTimeout, Message: "auth timeout"})
		_ = s.t.Close()
	}()
}

func (s *Session) SendHello(role string) error {
	if err := s.EnsureAuthenticatedFor(Frame{Header: Header{FrameType: FrameHello}}); err != nil {
		return err
	}
	f := NewFrame(FrameHello, SchemaHello, EncodeHelloMessage(s.hello(role)))
	f.Header.Priority = PriorityCritical
	f.Header.ChannelID = ChannelControl
	f.Header.Flags = FlagControl
	if err := s.send(f); err != nil {
		return err
	}
	s.setState(SessionHelloSent)
	return nil
}

func (s *Session) SendHelloAck(role string) error {
	f := NewFrame(FrameHelloAck, SchemaHello, EncodeHelloMessage(s.hello(role)))
	f.Header.Priority = PriorityCritical
	f.Header.ChannelID = ChannelControl
	f.Header.Flags = FlagControl
	if err := s.send(f); err != nil {
		return err
	}
	s.setState(SessionEstablished)
	return nil
}

func (s *Session) SendText(text string) error {
	if err := s.EnsureAuthenticatedFor(Frame{Header: Header{FrameType: FrameData}}); err != nil {
		return err
	}
	if 4+len(text) > int(s.config.Payload.MaxTextBytes) {
		return fmt.Errorf("text payload exceeds max size: %d > %d", 4+len(text), s.config.Payload.MaxTextBytes)
	}
	requestID := s.nextRequest.Add(1)
	if s.writer != nil {
		size := HeaderSize + 4 + len(text)
		bufp := getFrameBuffer(size)
		data := (*bufp)[:size]
		payload := EncodeTextMessageInto(data[HeaderSize:HeaderSize], text)
		encodeFrameHeaderInto(data[:HeaderSize], Header{
			FrameType:  FrameData,
			Flags:      FlagFirst | FlagLast,
			Priority:   PriorityNormal,
			ChannelID:  ChannelRealtime,
			SchemaID:   SchemaTextMessage,
			RequestID:  requestID,
			TransferID: 0,
		}, len(payload))
		err := s.writer.enqueue(context.Background(), sendItem{
			priority: PriorityNormal,
			channel:  ChannelRealtime,
			data:     data,
			bufp:     bufp,
		})
		s.markWrite()
		return err
	}
	payloadBuf := getFrameBuffer(4 + len(text))
	payload := EncodeTextMessageInto((*payloadBuf)[:0], text)
	f := NewFrame(FrameData, SchemaTextMessage, payload)
	f.Header.Priority = PriorityNormal
	f.Header.ChannelID = ChannelRealtime
	f.Header.RequestID = requestID
	f.Header.Flags = FlagFirst | FlagLast
	err := s.send(f)
	putFrameBuffer(payloadBuf)
	return err
}

func (s *Session) SendRequest(event string, data []byte) (uint64, error) {
	return s.SendRequestFields(event, data, nil)
}

func (s *Session) SendRequestFields(event string, data []byte, fields []TransferField) (uint64, error) {
	requestID := s.nextRequest.Add(1)
	return requestID, s.sendRequestWithID(requestID, event, data, fields)
}

func (s *Session) sendRequestWithID(requestID uint64, event string, data []byte, fields []TransferField) error {
	if err := s.requireRemoteCapability(CapabilityRequestResponse, "request/response"); err != nil {
		return err
	}
	if err := s.EnsureAuthenticatedFor(Frame{Header: Header{FrameType: FrameRequest}}); err != nil {
		return err
	}
	if payloadSize := EventMessagePayloadSizeString(event, data, fields); payloadSize > int(s.config.Payload.MaxRequestBytes) {
		return fmt.Errorf("request payload exceeds max size: %d > %d", payloadSize, s.config.Payload.MaxRequestBytes)
	}
	return s.sendEventFrame(FrameRequest, requestID, event, data, fields)
}

func (s *Session) SendResponse(requestID uint64, event string, data []byte) error {
	return s.SendResponseFields(requestID, event, data, nil)
}

func (s *Session) SendResponseFields(requestID uint64, event string, data []byte, fields []TransferField) error {
	return s.sendResponseWithID(requestID, event, data, fields)
}

func (s *Session) sendResponseWithID(requestID uint64, event string, data []byte, fields []TransferField) error {
	if requestID == 0 {
		return errors.New("response request id is required")
	}
	if err := s.requireRemoteCapability(CapabilityRequestResponse, "request/response"); err != nil {
		return err
	}
	if err := s.EnsureAuthenticatedFor(Frame{Header: Header{FrameType: FrameResponse}}); err != nil {
		return err
	}
	if payloadSize := EventMessagePayloadSizeString(event, data, fields); payloadSize > int(s.config.Payload.MaxResponseBytes) {
		return fmt.Errorf("response payload exceeds max size: %d > %d", payloadSize, s.config.Payload.MaxResponseBytes)
	}
	return s.sendEventFrame(FrameResponse, requestID, event, data, fields)
}

func (s *Session) sendEventFrame(frameType uint8, requestID uint64, event string, data []byte, fields []TransferField) error {
	payloadSize := EventMessagePayloadSizeString(event, data, fields)
	if s.writer != nil {
		size := HeaderSize + payloadSize
		bufp := getFrameBuffer(size)
		frameData := (*bufp)[:size]
		payload, err := EncodeEventMessageStringWithFieldsInto(frameData[HeaderSize:HeaderSize], event, data, fields)
		if err != nil {
			putFrameBuffer(bufp)
			return err
		}
		encodeFrameHeaderInto(frameData[:HeaderSize], Header{
			FrameType: frameType,
			Flags:     FlagFirst | FlagLast,
			Priority:  PriorityNormal,
			ChannelID: ChannelRealtime,
			SchemaID:  SchemaEvent,
			RequestID: requestID,
		}, len(payload))
		err = s.writer.enqueue(context.Background(), sendItem{
			priority: PriorityNormal,
			channel:  ChannelRealtime,
			data:     frameData,
			bufp:     bufp,
		})
		s.markWrite()
		return err
	}
	payloadBuf := getFrameBuffer(payloadSize)
	payload, err := EncodeEventMessageStringWithFieldsInto((*payloadBuf)[:0], event, data, fields)
	if err != nil {
		putFrameBuffer(payloadBuf)
		return err
	}
	f := NewFrame(frameType, SchemaEvent, payload)
	f.Header.Priority = PriorityNormal
	f.Header.ChannelID = ChannelRealtime
	f.Header.RequestID = requestID
	f.Header.Flags = FlagFirst | FlagLast
	err = s.send(f)
	putFrameBuffer(payloadBuf)
	return err
}

func (s *Session) Send(ctx context.Context, opts MessageOptions) (MessageHandle, error) {
	return s.Request(ctx, opts)
}

func (s *Session) Request(ctx context.Context, opts MessageOptions) (MessageHandle, error) {
	requestID := s.nextRequest.Add(1)
	if s.shouldSendInline(opts, s.config.Payload.MaxRequestBytes) {
		return MessageHandle{RequestID: requestID}, s.sendRequestWithID(requestID, opts.Event, opts.Data, opts.Fields)
	}
	handle, err := s.startMessageTransfer(ctx, requestID, opts)
	if err != nil {
		return MessageHandle{}, err
	}
	return handle, nil
}

func (s *Session) Respond(ctx context.Context, requestID uint64, opts MessageOptions) (MessageHandle, error) {
	if requestID == 0 {
		return MessageHandle{}, errors.New("response request id is required")
	}
	if s.shouldSendInline(opts, s.config.Payload.MaxResponseBytes) {
		return MessageHandle{RequestID: requestID}, s.sendResponseWithID(requestID, opts.Event, opts.Data, opts.Fields)
	}
	handle, err := s.startMessageTransfer(ctx, requestID, opts)
	if err != nil {
		return MessageHandle{}, err
	}
	return handle, nil
}

func (s *Session) shouldSendInline(opts MessageOptions, maxPayload uint32) bool {
	if opts.Reader != nil {
		return false
	}
	payloadSize := EventMessagePayloadSizeString(opts.Event, opts.Data, opts.Fields)
	return payloadSize <= int(maxPayload) && payloadSize <= int(s.config.Payload.MaxInlineBodyBytes)
}

func (s *Session) startMessageTransfer(ctx context.Context, requestID uint64, opts MessageOptions) (MessageHandle, error) {
	reader := opts.Reader
	size := opts.Size
	if reader == nil {
		reader = bytes.NewReader(opts.Data)
		size = uint64(len(opts.Data))
	} else if size == 0 {
		return MessageHandle{}, errors.New("message reader size is required")
	}
	if opts.ContentType == 0 {
		opts.ContentType = ContentFile
	}
	handle := s.StartTransfer(ctx, TransferOptions{
		RequestID:   requestID,
		Event:       opts.Event,
		Field:       opts.Field,
		Index:       opts.Index,
		Name:        opts.Name,
		ContentType: opts.ContentType,
		Reader:      reader,
		TotalSize:   size,
		Fields:      opts.Fields,
		ChunkSize:   opts.ChunkSize,
		AckTimeout:  opts.AckTimeout,
		RetryLimit:  opts.RetryLimit,
	})
	return MessageHandle{RequestID: requestID, TransferID: handle.TransferID, done: handle.Done()}, nil
}

func (s *Session) SendWindow(window Window) error {
	if err := s.requireRemoteCapability(CapabilityFlowControl, "flow control"); err != nil {
		return err
	}
	payloadBuf := getFrameBuffer(24)
	payload := EncodeWindowInto((*payloadBuf)[:0], window)
	defer putFrameBuffer(payloadBuf)
	f := NewFrame(FrameWindow, SchemaWindow, payload)
	f.Header.Priority = PriorityHigh
	f.Header.ChannelID = ChannelControl
	f.Header.TransferID = window.TransferID
	f.Header.Flags = FlagControl
	return s.send(f)
}

func (s *Session) RequestTransferResume(resume TransferResume) error {
	f := NewFrame(FrameTransferResume, SchemaTransferState, EncodeTransferResume(resume))
	f.Header.Priority = PriorityHigh
	f.Header.ChannelID = ChannelBulk
	f.Header.TransferID = resume.TransferID
	f.Header.Flags = FlagControl
	return s.send(f)
}

func (s *Session) SendTransferState(state TransferStateMessage) error {
	payloadBuf := getFrameBuffer(24)
	payload := EncodeTransferStateMessageInto((*payloadBuf)[:0], state)
	defer putFrameBuffer(payloadBuf)
	f := NewFrame(FrameTransferState, SchemaTransferState, payload)
	f.Header.Priority = PriorityHigh
	f.Header.ChannelID = ChannelBulk
	f.Header.TransferID = state.TransferID
	f.Header.Flags = FlagControl
	return s.send(f)
}

func (s *Session) HandleWindow(frame Frame) error {
	window, err := DecodeWindow(frame.Payload)
	if err != nil {
		return err
	}
	ot := s.outgoingTransfer(window.TransferID)
	if ot == nil {
		return nil
	}
	ot.mu.Lock()
	ot.remoteWindowBytes = window.WindowBytes
	ot.remoteWindowChunks = window.WindowChunks
	ot.hasRemoteWindow = true
	ot.cond.Broadcast()
	ot.mu.Unlock()
	return nil
}

func (s *Session) SendTransfer(name string, contentType uint32, r io.Reader, totalSize uint64) (uint64, error) {
	handle := s.StartTransfer(context.Background(), TransferOptions{
		Name:        name,
		ContentType: contentType,
		Reader:      r,
		TotalSize:   totalSize,
	})
	return handle.TransferID, <-handle.Done()
}

func (s *Session) StartTransfer(ctx context.Context, opts TransferOptions) *TransferHandle {
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = DefaultChunkSize
	}
	if opts.ChunkSize > s.config.FlowControl.MaxChunkSize {
		opts.ChunkSize = s.config.FlowControl.MaxChunkSize
	}
	ctx, cancel := context.WithCancel(ctx)
	transferID := s.nextTransfer.Add(1)
	handle := &TransferHandle{
		TransferID: transferID,
		done:       make(chan error, 1),
		cancel:     cancel,
		session:    s,
	}
	s.registerOutgoing(transferID, opts)
	go s.monitorOutgoing(ctx, transferID)
	go func() {
		defer close(handle.done)
		defer cancel()
		err := s.sendTransfer(ctx, handle.TransferID, opts)
		s.unregisterOutgoing(handle.TransferID)
		handle.done <- err
	}()
	return handle
}

func (h *TransferHandle) Done() <-chan error {
	return h.done
}

func (h *TransferHandle) Cancel(reason uint16, flags uint16) error {
	var err error
	h.cancelOnce.Do(func() {
		h.cancel()
		h.session.cancelOutgoing(h.TransferID)
		err = h.session.CancelTransfer(h.TransferID, reason, flags)
	})
	return err
}

func (s *Session) sendTransfer(ctx context.Context, transferID uint64, opts TransferOptions) error {
	if err := s.requireRemoteCapability(CapabilityTransfers, "transfers"); err != nil {
		return err
	}
	if err := s.EnsureAuthenticatedFor(Frame{Header: Header{FrameType: FrameTransferBegin}}); err != nil {
		return err
	}
	if opts.TotalSize > s.config.FlowControl.MaxTransferBytes {
		return fmt.Errorf("transfer exceeds max size: %d > %d", opts.TotalSize, s.config.FlowControl.MaxTransferBytes)
	}
	if err := s.outgoingFailure(transferID); err != nil {
		return err
	}
	chunkCount := uint32(0)
	if opts.TotalSize > 0 {
		chunkCount = uint32(math.Ceil(float64(opts.TotalSize) / float64(opts.ChunkSize)))
	}

	beginPayloadSize := 72 + len(opts.Name) + len(opts.Event) + len(opts.Field) + transferPartsPayloadSize(opts.Parts) + transferFieldsPayloadSize(opts.Fields)
	beginPayloadBuf := getFrameBuffer(beginPayloadSize)
	beginPayload := EncodeTransferBeginInto((*beginPayloadBuf)[:0], TransferBegin{
		TotalSize:   opts.TotalSize,
		ChunkSize:   uint32(opts.ChunkSize),
		ChunkCount:  chunkCount,
		ContentType: opts.ContentType,
		Flags:       checksumFlag(opts.ChecksumMode),
		Checksum:    opts.Checksum,
		Name:        opts.Name,
		Event:       opts.Event,
		Field:       opts.Field,
		Index:       opts.Index,
		Parts:       opts.Parts,
		Fields:      opts.Fields,
	})
	begin := NewFrame(FrameTransferBegin, SchemaTransferBegin, beginPayload)
	begin.Header.Priority = PriorityLow
	begin.Header.ChannelID = ChannelBulk
	begin.Header.RequestID = opts.RequestID
	begin.Header.TransferID = transferID
	begin.Header.Flags = FlagFirst
	err := s.send(begin)
	putFrameBuffer(beginPayloadBuf)
	if err != nil {
		return err
	}

	var written uint64
	var chunkID uint32
	for {
		if err := ctx.Err(); err != nil {
			s.emitProgress(Progress{
				TransferID:        transferID,
				TotalBytes:        opts.TotalSize,
				LocalWrittenBytes: written,
				SentChunks:        chunkID,
				State:             TransferCanceling,
			})
			return err
		}

		if opts.TotalSize > 0 && written >= opts.TotalSize {
			break
		}

		nextReadSize := opts.ChunkSize
		if opts.TotalSize > 0 {
			remaining := opts.TotalSize - written
			if remaining < uint64(nextReadSize) {
				nextReadSize = int(remaining)
			}
		}
		payloadBuf := getChunkBuffer(nextReadSize)
		payload := (*payloadBuf)[:nextReadSize]
		n, readErr := opts.Reader.Read(payload)
		if n > 0 {
			payload = payload[:n]
			data := NewFrame(FrameData, 0, payload)
			data.Header.Priority = PriorityLow
			data.Header.ChannelID = ChannelBulk
			data.Header.RequestID = opts.RequestID
			data.Header.TransferID = transferID
			data.Header.ChunkID = chunkID
			data.Header.Flags = FlagAckRequest
			if chunkID == 0 {
				data.Header.Flags |= FlagFirst
			}
			written += uint64(n)
			if opts.TotalSize > 0 && written >= opts.TotalSize {
				data.Header.Flags |= FlagLast
			}
			if err := ctx.Err(); err != nil {
				s.emitProgress(Progress{
					TransferID:        transferID,
					TotalBytes:        opts.TotalSize,
					LocalWrittenBytes: written - uint64(n),
					SentChunks:        chunkID,
					State:             TransferCanceling,
				})
				return err
			}
			if err := s.sendReliableChunk(ctx, transferID, data, uint64(n), payloadBuf); err != nil {
				return err
			}
			payloadBuf = nil
			chunkID++
			s.emitProgress(Progress{
				TransferID:        transferID,
				TotalBytes:        opts.TotalSize,
				LocalWrittenBytes: written,
				SentChunks:        chunkID,
				State:             TransferSending,
			})
			if opts.DelayPerChunk > 0 {
				timer := time.NewTimer(opts.DelayPerChunk)
				select {
				case <-ctx.Done():
					timer.Stop()
					s.emitProgress(Progress{
						TransferID:        transferID,
						TotalBytes:        opts.TotalSize,
						LocalWrittenBytes: written,
						SentChunks:        chunkID,
						State:             TransferCanceling,
					})
					return ctx.Err()
				case <-timer.C:
				}
			}
		}
		putChunkBuffer(payloadBuf)
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}

	end := NewFrame(FrameTransferEnd, 0, nil)
	end.Header.Priority = PriorityLow
	end.Header.ChannelID = ChannelBulk
	end.Header.RequestID = opts.RequestID
	end.Header.TransferID = transferID
	end.Header.Flags = FlagLast
	if err := s.send(end); err != nil {
		return err
	}
	if err := s.waitOutgoingAcked(ctx, transferID); err != nil {
		return err
	}
	s.emitProgress(Progress{
		TransferID:        transferID,
		TotalBytes:        opts.TotalSize,
		LocalWrittenBytes: written,
		SentChunks:        chunkID,
		State:             TransferCompleted,
	})
	return nil
}

func (s *Session) CancelTransfer(transferID uint64, reason uint16, flags uint16) error {
	if err := s.requireRemoteCapability(CapabilityCancel, "cancel"); err != nil {
		return err
	}
	f := NewFrame(FrameCancel, SchemaCancel, EncodeCancel(Cancel{
		TransferID: transferID,
		ReasonCode: reason,
		Flags:      flags,
	}))
	f.Header.Priority = PriorityCritical
	f.Header.ChannelID = ChannelControl
	f.Header.TransferID = transferID
	f.Header.Flags = FlagControl
	return s.send(f)
}

func (s *Session) HandleAck(frame Frame) error {
	if err := s.requireRemoteCapability(CapabilityAck, "ack"); err != nil {
		return err
	}
	ack, err := DecodeAck(frame.Payload)
	if err != nil {
		return err
	}
	s.applyAck(ack)
	s.emitProgress(Progress{
		TransferID:       ack.TransferID,
		RemoteAckedBytes: ack.ReceivedBytes,
		AckedChunks:      ack.ChunkTo + 1,
		State:            TransferAcked,
	})
	return nil
}

func (s *Session) HandleNack(frame Frame) error {
	if err := s.requireRemoteCapability(CapabilityNack, "nack"); err != nil {
		return err
	}
	nack, err := DecodeNack(frame.Payload)
	if err != nil {
		return err
	}
	s.emitEvent(ProtocolEvent{
		Code:       EventNackReceived,
		Message:    "nack received",
		FrameType:  frame.Header.FrameType,
		TransferID: nack.TransferID,
		ChunkID:    nack.ChunkFrom,
	})
	return s.applyNack(nack)
}

func (s *Session) SendPing() error {
	if err := s.requireRemoteCapability(CapabilityHeartbeat, "heartbeat"); err != nil {
		return err
	}
	f := NewFrame(FramePing, 0, nil)
	f.Header.Priority = PriorityCritical
	f.Header.ChannelID = ChannelControl
	f.Header.Flags = FlagControl
	return s.send(f)
}

func (s *Session) SendPong() error {
	if err := s.requireRemoteCapability(CapabilityHeartbeat, "heartbeat"); err != nil {
		return err
	}
	f := NewFrame(FramePong, 0, nil)
	f.Header.Priority = PriorityCritical
	f.Header.ChannelID = ChannelControl
	f.Header.Flags = FlagControl
	return s.send(f)
}

func (s *Session) HandlePing() error {
	return s.SendPong()
}

func (s *Session) HandlePong() {
}

func (s *Session) StartHeartbeat(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(s.config.Heartbeat.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Now()
				lastRead := time.Unix(0, s.lastReadNano.Load())
				if now.Sub(lastRead) > s.config.Heartbeat.Timeout {
					s.setState(SessionFailed)
					s.emitEvent(ProtocolEvent{
						Code:    EventProtocolViolation,
						Message: "heartbeat timeout",
					})
					return
				}
				if s.config.Role == RoleServer {
					continue
				}
				lastWrite := time.Unix(0, s.lastWriteNano.Load())
				if now.Sub(lastWrite) <= s.config.Heartbeat.Interval {
					continue
				}
				if err := s.SendPing(); err != nil {
					s.setState(SessionFailed)
					s.emitEvent(ProtocolEvent{
						Code:    EventProtocolViolation,
						Message: fmt.Sprintf("heartbeat ping failed: %v", err),
					})
					return
				}
			}
		}
	}()
}

func (s *Session) markRead() {
	s.lastReadNano.Store(time.Now().UnixNano())
}

func (s *Session) markWrite() {
	s.lastWriteNano.Store(time.Now().UnixNano())
}

func (s *Session) emitProgress(p Progress) {
	if s.onProgress != nil {
		s.onProgress(p)
	}
}

func (s *Session) emitEvent(e ProtocolEvent) {
	if s.onEvent != nil {
		s.onEvent(e)
	}
}

func (s *Session) setState(state SessionState) {
	s.state.Store(uint32(state))
}

func (s *Session) hello(role string) Hello {
	if role == "" {
		role = s.config.Role
	}
	return Hello{
		Role:              role,
		Capabilities:      s.config.Capabilities,
		MaxFrameBytes:     MaxFrameBytes,
		MaxChunkSize:      uint32(s.config.FlowControl.MaxChunkSize),
		MaxTransferBytes:  s.config.FlowControl.MaxTransferBytes,
		MaxInFlightChunks: uint32(s.config.FlowControl.MaxInFlightChunks),
		HeartbeatMillis:   uint32(s.config.Heartbeat.Interval / time.Millisecond),
	}
}

func (s *Session) Close() error {
	return s.CloseWith(CloseMessage{ReasonCode: CloseNormal, Flags: CloseFlagImmediate})
}

func (s *Session) CloseWith(closeMessage CloseMessage) error {
	s.setState(SessionClosing)
	f := NewFrame(FrameClose, SchemaClose, EncodeCloseMessage(closeMessage))
	f.Header.Priority = PriorityCritical
	f.Header.ChannelID = ChannelControl
	f.Header.Flags = FlagControl
	if err := s.send(f); err != nil {
		s.setState(SessionFailed)
		return fmt.Errorf("send close: %w", err)
	}
	if s.writer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), s.config.SendQueue.EnqueueTimeout+s.config.SendQueue.WriteTimeout)
		err := s.Flush(ctx)
		cancel()
		if err != nil {
			s.setState(SessionFailed)
			return fmt.Errorf("flush close: %w", err)
		}
		s.writer.close()
	}
	if err := s.t.Close(); err != nil {
		s.setState(SessionFailed)
		return err
	}
	s.setState(SessionClosed)
	return nil
}

func (s *Session) SendCloseAck(closeMessage CloseMessage) error {
	f := NewFrame(FrameCloseAck, SchemaClose, EncodeCloseMessage(closeMessage))
	f.Header.Priority = PriorityCritical
	f.Header.ChannelID = ChannelControl
	f.Header.Flags = FlagControl
	return s.send(f)
}

func (s *Session) SendError(code uint32, frame Frame, message string) error {
	if s.writer == nil {
		payloadSize := ErrorMessagePayloadSizeString(message)
		size := HeaderSize + payloadSize
		bufp := getFrameBuffer(size)
		data := (*bufp)[:size]
		payload, err := EncodeErrorMessageStringInto(data[HeaderSize:HeaderSize], code, frame.Header.FrameType, frame.Header.SchemaID, frame.Header.RequestID, frame.Header.TransferID, message)
		if err != nil {
			putFrameBuffer(bufp)
			return err
		}
		f := NewFrame(FrameError, SchemaError, payload)
		f.Header.Priority = PriorityCritical
		f.Header.ChannelID = ChannelControl
		f.Header.RequestID = frame.Header.RequestID
		f.Header.TransferID = frame.Header.TransferID
		f.Header.Flags = FlagControl | FlagFirst | FlagLast
		data, err = EncodeFrameInto(data[:0], f)
		if err != nil {
			putFrameBuffer(bufp)
			return err
		}
		err = s.t.SendFrame(data)
		putFrameBuffer(bufp)
		s.markWrite()
		return err
	}
	payload, err := EncodeErrorMessageStringInto(nil, code, frame.Header.FrameType, frame.Header.SchemaID, frame.Header.RequestID, frame.Header.TransferID, message)
	if err != nil {
		return err
	}
	f := NewFrame(FrameError, SchemaError, payload)
	f.Header.Priority = PriorityCritical
	f.Header.ChannelID = ChannelControl
	f.Header.RequestID = frame.Header.RequestID
	f.Header.TransferID = frame.Header.TransferID
	f.Header.Flags = FlagControl | FlagFirst | FlagLast
	return s.send(f)
}

func (s *Session) SendGoAway(goAway GoAway) error {
	if goAway.LastAcceptedRequestID == 0 {
		goAway.LastAcceptedRequestID = s.nextRequest.Load()
	}
	if goAway.LastAcceptedTransferID == 0 {
		goAway.LastAcceptedTransferID = s.nextTransfer.Load()
	}
	f := NewFrame(FrameGoAway, SchemaGoAway, EncodeGoAway(goAway))
	f.Header.Priority = PriorityCritical
	f.Header.ChannelID = ChannelControl
	f.Header.Flags = FlagControl | FlagFirst | FlagLast
	if goAway.Flags&CloseFlagDrain != 0 {
		s.setState(SessionDraining)
	} else {
		s.setState(SessionClosing)
	}
	return s.send(f)
}

func (s *Session) registerOutgoing(transferID uint64, opts TransferOptions) {
	ackTimeout := opts.AckTimeout
	if ackTimeout <= 0 {
		ackTimeout = s.config.FlowControl.AckTimeout
	}
	retryLimit := opts.RetryLimit
	if retryLimit <= 0 {
		retryLimit = s.config.FlowControl.RetryLimit
	}
	maxInFlight := opts.MaxInFlight
	if maxInFlight <= 0 {
		maxInFlight = s.config.FlowControl.MaxInFlightChunks
	}
	maxInFlightBytes := opts.MaxInFlightBytes
	if maxInFlightBytes == 0 {
		maxInFlightBytes = s.config.FlowControl.MaxInFlightBytes
	}
	if maxInFlightBytes > s.config.FlowControl.MaxSendBufferBytes {
		maxInFlightBytes = s.config.FlowControl.MaxSendBufferBytes
	}
	chunkCount := uint32(0)
	if opts.TotalSize > 0 && opts.ChunkSize > 0 {
		chunkCount = uint32(math.Ceil(float64(opts.TotalSize) / float64(opts.ChunkSize)))
	}
	ot := &outgoingTransfer{
		id:               transferID,
		totalBytes:       opts.TotalSize,
		chunkCount:       chunkCount,
		ackTimeout:       ackTimeout,
		retryLimit:       retryLimit,
		maxInFlight:      maxInFlight,
		maxInFlightBytes: maxInFlightBytes,
		pending:          make([]sentChunk, 0, initialPendingCapacity(maxInFlight)),
	}
	ot.cond.L = &ot.mu
	s.mu.Lock()
	if len(s.outgoing) >= s.config.FlowControl.MaxConcurrentTransfers {
		ot.failed = fmt.Errorf("too many concurrent transfers: %d", s.config.FlowControl.MaxConcurrentTransfers)
	}
	s.outgoing[transferID] = ot
	s.mu.Unlock()
}

func (s *Session) unregisterOutgoing(transferID uint64) {
	s.mu.Lock()
	ot := s.outgoing[transferID]
	delete(s.outgoing, transferID)
	s.mu.Unlock()
	if ot != nil {
		ot.releasePending()
	}
}

func (s *Session) outgoingTransfer(transferID uint64) *outgoingTransfer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.outgoing[transferID]
}

func (s *Session) outgoingFailure(transferID uint64) error {
	ot := s.outgoingTransfer(transferID)
	if ot == nil {
		return nil
	}
	ot.mu.Lock()
	err := ot.failed
	ot.mu.Unlock()
	return err
}

func (s *Session) sendReliableChunk(ctx context.Context, transferID uint64, frame Frame, payloadN uint64, pooledPayload *[]byte) error {
	ot := s.outgoingTransfer(transferID)
	if ot == nil {
		err := s.send(frame)
		putChunkBuffer(pooledPayload)
		return err
	}
	ot.mu.Lock()
	for (len(ot.pending) >= ot.maxInFlight ||
		ot.pendingBytes+payloadN > ot.maxInFlightBytes ||
		(ot.hasRemoteWindow && (ot.remoteWindowBytes < payloadN || ot.remoteWindowChunks == 0))) && ot.failed == nil {
		ot.cond.Wait()
		if err := ctx.Err(); err != nil {
			ot.mu.Unlock()
			putChunkBuffer(pooledPayload)
			return err
		}
	}
	if ot.failed != nil {
		err := ot.failed
		ot.mu.Unlock()
		putChunkBuffer(pooledPayload)
		return err
	}
	ot.pending = append(ot.pending, sentChunk{
		chunkID:       frame.Header.ChunkID,
		frame:         frame,
		sentAt:        time.Now(),
		payloadN:      payloadN,
		pooledPayload: pooledPayload,
	})
	ot.pendingBytes += payloadN
	if ot.hasRemoteWindow {
		ot.remoteWindowBytes -= payloadN
		ot.remoteWindowChunks--
	}
	ot.mu.Unlock()
	if err := s.sendContext(ctx, frame); err != nil {
		ot.fail(err)
		return err
	}
	return nil
}

func (s *Session) monitorOutgoing(ctx context.Context, transferID uint64) {
	ot := s.outgoingTransfer(transferID)
	if ot == nil {
		return
	}
	interval := ot.ackTimeout / 2
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		now := time.Now()
		var resend []Frame
		var resendChunkIDs []uint32
		ot.mu.Lock()
		if ot.failed != nil {
			ot.mu.Unlock()
			return
		}
		if ot.doneSending && len(ot.pending) == 0 {
			ot.mu.Unlock()
			return
		}
		for i := range ot.pending {
			chunk := &ot.pending[i]
			chunkID := chunk.chunkID
			if chunk.acked || now.Sub(chunk.sentAt) < ot.ackTimeout {
				continue
			}
			if chunk.retries >= ot.retryLimit {
				err := fmt.Errorf("ack timeout for transfer=%d chunk=%d", ot.id, chunkID)
				ot.failed = err
				ot.cond.Broadcast()
				ot.mu.Unlock()
				s.emitEvent(ProtocolEvent{Code: EventAckTimeout, Message: err.Error(), TransferID: ot.id, ChunkID: chunkID})
				return
			}
			chunk.retries++
			chunk.sentAt = now
			resend = append(resend, chunk.frame)
			resendChunkIDs = append(resendChunkIDs, chunkID)
		}
		ot.mu.Unlock()

		for i, frame := range resend {
			chunkID := resendChunkIDs[i]
			s.emitEvent(ProtocolEvent{Code: EventAckTimeout, Message: "resend chunk after ack timeout", TransferID: ot.id, ChunkID: chunkID})
			if err := s.send(frame); err != nil {
				ot.fail(err)
				return
			}
		}
	}
}

func (s *Session) waitOutgoingAcked(ctx context.Context, transferID uint64) error {
	ot := s.outgoingTransfer(transferID)
	if ot == nil {
		return nil
	}
	ot.mu.Lock()
	ot.doneSending = true
	for len(ot.pending) > 0 && ot.failed == nil {
		ot.cond.Wait()
		if err := ctx.Err(); err != nil {
			ot.mu.Unlock()
			return err
		}
	}
	err := ot.failed
	ot.mu.Unlock()
	return err
}

func (s *Session) applyAck(ack Ack) {
	ot := s.outgoingTransfer(ack.TransferID)
	if ot == nil {
		return
	}
	ot.mu.Lock()
	dst := ot.pending[:0]
	for _, chunk := range ot.pending {
		chunkID := chunk.chunkID
		if chunkID >= ack.ChunkFrom && chunkID <= ack.ChunkTo {
			chunk.acked = true
			ot.ackedBytes += chunk.payloadN
			if ot.pendingBytes >= chunk.payloadN {
				ot.pendingBytes -= chunk.payloadN
			} else {
				ot.pendingBytes = 0
			}
			putChunkBuffer(chunk.pooledPayload)
			continue
		}
		dst = append(dst, chunk)
	}
	ot.pending = dst
	ot.cond.Broadcast()
	ot.mu.Unlock()
}

func (s *Session) applyNack(nack Nack) error {
	ot := s.outgoingTransfer(nack.TransferID)
	if ot == nil {
		return nil
	}
	ot.mu.Lock()
	var frames []Frame
	for i := range ot.pending {
		chunk := &ot.pending[i]
		chunkID := chunk.chunkID
		if chunkID >= nack.ChunkFrom && chunkID <= nack.ChunkTo {
			if chunk.retries >= ot.retryLimit {
				err := fmt.Errorf("retry limit exceeded for transfer=%d chunk=%d", ot.id, chunkID)
				ot.failed = err
				ot.cond.Broadcast()
				ot.mu.Unlock()
				return err
			}
			chunk.retries++
			chunk.sentAt = time.Now()
			frames = append(frames, chunk.frame)
		}
	}
	ot.mu.Unlock()
	for _, frame := range frames {
		if err := s.send(frame); err != nil {
			ot.fail(err)
			return err
		}
	}
	return nil
}

func (ot *outgoingTransfer) fail(err error) {
	ot.mu.Lock()
	ot.failed = err
	ot.cond.Broadcast()
	ot.mu.Unlock()
}

func (ot *outgoingTransfer) releasePending() {
	ot.mu.Lock()
	pending := ot.pending
	ot.pending = nil
	ot.pendingBytes = 0
	ot.mu.Unlock()
	for i := range pending {
		putChunkBuffer(pending[i].pooledPayload)
	}
}

func (s *Session) cancelOutgoing(transferID uint64) {
	ot := s.outgoingTransfer(transferID)
	if ot == nil {
		return
	}
	ot.mu.Lock()
	ot.cond.Broadcast()
	ot.mu.Unlock()
}

func checksumFlag(mode ChecksumMode) uint32 {
	if mode == ChecksumTransferSHA256 {
		return TransferFlagChecksumSHA256
	}
	return 0
}

func initialPendingCapacity(maxInFlight int) int {
	if maxInFlight <= 0 {
		return 0
	}
	if maxInFlight < 4 {
		return maxInFlight
	}
	return 4
}

func cloneAuthAttributes(in []AuthAttribute) []AuthAttribute {
	if len(in) == 0 {
		return nil
	}
	out := make([]AuthAttribute, len(in))
	copy(out, in)
	return out
}
