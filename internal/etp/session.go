package etp

import (
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultChunkSize = 16 * 1024
	RoleClient       = "client"
	RoleServer       = "server"
	maxHeartbeatWire = time.Duration(1<<32-1) * time.Millisecond
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
	remoteFrame   atomic.Uint32
	receiveBytes  atomic.Uint64
	running       atomic.Bool
	started       atomic.Bool
	peerCloseSeen atomic.Bool
	mu            sync.Mutex
	callbackMu    sync.RWMutex
	closeMu       sync.Mutex
	identity      SessionIdentity
	outgoing      map[uint64]*outgoingTransfer
	incoming      map[uint64]*incomingTransfer
	opening       map[uint64]struct{}
	completed     map[uint64]completedTransfer
	completedIDs  []uint64
	lastIncoming  uint64
	remoteHello   atomic.Pointer[Hello]
	config        SessionConfig
	rate          rateLimitState
	scheduler     *FrameScheduler
	writer        *sessionWriter
	heartbeatOnce sync.Once
	authOnce      sync.Once
	closeAck      chan CloseMessage
	peerCloseDone chan error
	handlerJobs   chan handlerJob
	handlerOnce   sync.Once
	outgoingWake  chan struct{}
	outgoingStop  chan struct{}
	outgoingNext  atomic.Int64
	outgoingOnce  sync.Once
	stopOnce      sync.Once
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
	Handlers     HandlerConfig
	Close        CloseConfig
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
	MaxCompletedTransfers  int
	CompletedTransferTTL   time.Duration
	TransferOpenTimeout    time.Duration
	TransferCommitTimeout  time.Duration
}

type completedTransfer struct {
	state     TransferStateMessage
	expiresAt time.Time
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
	MaxFrameBytes      uint32
	PreHandshakeBytes  uint32
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

type HandlerConfig struct {
	Workers int
	Queue   int
}

type CloseConfig struct {
	AckTimeout time.Duration
}

type handlerJob struct {
	frame Frame
	run   func() error
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
	RequestID  uint64
	Meta       TransferBegin
}

type IncomingTransferWriter interface {
	io.Writer
	Close() error
}

type IncomingTransferContextWriter interface {
	WriteContext(context.Context, []byte) (int, error)
}

type IncomingTransferContextCloser interface {
	CloseContext(context.Context) error
}

type IncomingTransferAborter interface {
	Abort() error
}

// IncomingTransferSuspender preserves committed partial data for a later
// reconnect. Cancellation and protocol failures still use Abort.
type IncomingTransferSuspender interface {
	Suspend() error
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
	data             []byte
	useData          bool
}

type ResumeTransferOptions struct {
	TransferOptions
	TransferID    uint64
	Token         []byte
	ReceivedBytes uint64
	NextChunk     uint32
	OpenReader    func(offset uint64) (io.Reader, error)
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
	chunkSize          uint32
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
	completed          bool
	endFrame           Frame
	commitSentAt       time.Time
	commitRetries      int
	monitorRefs        int
	refs               sync.WaitGroup
	resumeDecided      bool
	resumeAccepted     bool
	resumeBytes        uint64
	resumeChunk        uint32
	failed             error
}

var outgoingTransferPool sync.Pool

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
	if role != RoleServer && role != RoleClient {
		role = RoleClient
	}
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
			MaxCompletedTransfers:  1024,
			CompletedTransferTTL:   time.Minute,
			TransferOpenTimeout:    5 * time.Second,
			TransferCommitTimeout:  30 * time.Second,
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
			MaxFrameBytes:      MaxFrameBytes,
			PreHandshakeBytes:  64 << 10,
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
		Handlers: HandlerConfig{Workers: 1, Queue: 64},
		Close:    CloseConfig{AckTimeout: 2 * time.Second},
	}
}

func NewSessionWithConfig(t FrameTransport, config SessionConfig) *Session {
	config = normalizeSessionConfig(config)
	s := &Session{
		t:             t,
		outgoing:      map[uint64]*outgoingTransfer{},
		incoming:      map[uint64]*incomingTransfer{},
		opening:       map[uint64]struct{}{},
		completed:     map[uint64]completedTransfer{},
		config:        config,
		scheduler:     NewFrameScheduler(),
		closeAck:      make(chan CloseMessage, 1),
		peerCloseDone: make(chan error, 1),
		handlerJobs:   make(chan handlerJob, config.Handlers.Queue),
		outgoingWake:  make(chan struct{}, 1),
		outgoingStop:  make(chan struct{}),
	}
	if t != nil && config.SendQueue.Enabled {
		s.writer = newSessionWriter(t, config.SendQueue, s.handleWriterFailure)
	}
	s.nextRequest.Store(1000)
	s.nextTransfer.Store(7000)
	now := time.Now().UnixNano()
	s.lastReadNano.Store(now)
	s.lastWriteNano.Store(now)
	if negotiated, ok := t.(NegotiatedFrameTransport); ok {
		caps := negotiated.NegotiatedCapabilities() & config.Capabilities & AllCapabilities
		s.remoteCaps.Store(caps)
		if caps != 0 && !config.Auth.Required {
			s.setState(SessionEstablished)
		} else {
			s.setState(SessionNew)
		}
	} else {
		s.setState(SessionNew)
	}
	s.applyReceiveFrameLimit()
	return s
}

func normalizeSessionConfig(config SessionConfig) SessionConfig {
	defaults := DefaultSessionConfig(config.Role)
	config.Role = defaults.Role
	if config.Capabilities == 0 {
		config.Capabilities = defaults.Capabilities
	}
	config.Capabilities &= AllCapabilities
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
	if config.FlowControl.MaxCompletedTransfers <= 0 {
		config.FlowControl.MaxCompletedTransfers = defaults.FlowControl.MaxCompletedTransfers
	}
	if config.FlowControl.CompletedTransferTTL <= 0 {
		config.FlowControl.CompletedTransferTTL = defaults.FlowControl.CompletedTransferTTL
	}
	if config.FlowControl.TransferOpenTimeout <= 0 {
		config.FlowControl.TransferOpenTimeout = defaults.FlowControl.TransferOpenTimeout
	}
	if config.FlowControl.TransferCommitTimeout <= 0 {
		config.FlowControl.TransferCommitTimeout = defaults.FlowControl.TransferCommitTimeout
	}
	if config.Heartbeat.Interval <= 0 {
		config.Heartbeat.Interval = defaults.Heartbeat.Interval
	}
	if config.Heartbeat.Interval < time.Millisecond {
		config.Heartbeat.Interval = time.Millisecond
	}
	if config.Heartbeat.Interval > maxHeartbeatWire {
		config.Heartbeat.Interval = maxHeartbeatWire
	}
	if config.Heartbeat.Timeout <= 0 {
		config.Heartbeat.Timeout = defaults.Heartbeat.Timeout
	}
	if config.Heartbeat.Timeout <= config.Heartbeat.Interval {
		config.Heartbeat.Timeout = config.Heartbeat.Interval * 2
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
	if config.Payload.MaxFrameBytes < HeaderSize || config.Payload.MaxFrameBytes > MaxFrameBytes {
		config.Payload.MaxFrameBytes = defaults.Payload.MaxFrameBytes
	}
	if config.Payload.PreHandshakeBytes < HeaderSize || config.Payload.PreHandshakeBytes > config.Payload.MaxFrameBytes {
		config.Payload.PreHandshakeBytes = defaults.Payload.PreHandshakeBytes
	}
	maxChunkForFrame := int(config.Payload.MaxFrameBytes) - HeaderSize
	if config.FlowControl.MaxChunkSize > maxChunkForFrame {
		config.FlowControl.MaxChunkSize = maxChunkForFrame
	}
	if max := config.FlowControl.MaxInFlightBytes; max > 0 && uint64(config.FlowControl.MaxChunkSize) > max {
		config.FlowControl.MaxChunkSize = int(max)
	}
	if max := config.FlowControl.MaxSendBufferBytes; max > 0 && uint64(config.FlowControl.MaxChunkSize) > max {
		config.FlowControl.MaxChunkSize = int(max)
	}
	if max := config.FlowControl.MaxReceiveBufferBytes; max > 0 && uint64(config.FlowControl.MaxChunkSize) > max {
		config.FlowControl.MaxChunkSize = int(max)
	}
	if config.FlowControl.MaxChunkSize <= 0 {
		config.FlowControl.MaxChunkSize = 1
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
	maxInline := config.Payload.MaxRequestBytes
	if config.Payload.MaxResponseBytes < maxInline {
		maxInline = config.Payload.MaxResponseBytes
	}
	if framePayload := config.Payload.MaxFrameBytes - HeaderSize; framePayload < maxInline {
		maxInline = framePayload
	}
	if config.Payload.MaxInlineBodyBytes > maxInline {
		config.Payload.MaxInlineBodyBytes = maxInline
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
	if config.Handlers.Workers <= 0 {
		config.Handlers.Workers = defaults.Handlers.Workers
	}
	if config.Handlers.Queue <= 0 {
		config.Handlers.Queue = defaults.Handlers.Queue
	}
	if config.Close.AckTimeout <= 0 {
		config.Close.AckTimeout = defaults.Close.AckTimeout
	}
	return config
}

func NormalizeSessionConfig(config SessionConfig) SessionConfig {
	return normalizeSessionConfig(config)
}

func (s *Session) OnProgress(fn func(Progress)) {
	s.callbackMu.Lock()
	s.onProgress = fn
	s.callbackMu.Unlock()
}

func (s *Session) OnProtocolEvent(fn func(ProtocolEvent)) {
	s.callbackMu.Lock()
	s.onEvent = fn
	s.callbackMu.Unlock()
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

func (s *Session) NegotiatedCapabilities() uint64 {
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

func (s *Session) requireCapability(capability uint64, name string) error {
	if s.config.Capabilities&capability == 0 {
		return fmt.Errorf("local %s support is disabled", name)
	}
	return s.requireRemoteCapability(capability, name)
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
	if s.t == nil {
		return errors.New("frame transport is nil")
	}
	if len(frame.Payload) > MaxFrameBytes-HeaderSize {
		return errors.New("frame exceeds max size")
	}
	if err := s.checkRemoteFrameSize(EncodedFrameSize(frame)); err != nil {
		return err
	}
	if s.writer == nil {
		if s.config.Scheduler.Enabled && s.scheduler != nil {
			frame = s.scheduler.PushPop(frame)
		}
		err := Send(s.t, frame)
		if err == nil {
			s.markWrite()
		}
		return err
	}
	size := EncodedFrameSize(frame)
	bufp := getFrameBuffer(size)
	data, err := EncodeFrameInto((*bufp)[:0], frame)
	if err != nil {
		putFrameBuffer(bufp)
		return err
	}
	err = s.writer.enqueue(ctx, sendItem{
		priority: frame.Header.Priority,
		channel:  frame.Header.ChannelID,
		data:     data,
		bufp:     bufp,
	})
	if err == nil {
		s.markWrite()
	}
	return err
}

func (s *Session) sendAndWait(ctx context.Context, frame Frame) error {
	if s.writer == nil {
		return s.sendContext(ctx, frame)
	}
	if err := s.checkRemoteFrameSize(EncodedFrameSize(frame)); err != nil {
		return err
	}
	size := EncodedFrameSize(frame)
	bufp := getFrameBuffer(size)
	data, err := EncodeFrameInto((*bufp)[:0], frame)
	if err != nil {
		putFrameBuffer(bufp)
		return err
	}
	done := make(chan error, 1)
	if err := s.writer.enqueue(ctx, sendItem{
		priority: frame.Header.Priority,
		channel:  frame.Header.ChannelID,
		data:     data,
		bufp:     bufp,
		done:     done,
	}); err != nil {
		return err
	}
	s.markWrite()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Session) Flush(ctx context.Context) error {
	if s.writer == nil {
		return nil
	}
	return s.writer.flush(ctx)
}

func (s *Session) Authenticate(req AuthRequest) error {
	if uint64(len(req.Payload))+12 > uint64(s.config.Payload.PreHandshakeBytes-HeaderSize) || uint32(len(req.Payload)) > s.config.Auth.MaxPayloadBytes {
		return errors.New("auth payload exceeds configured pre-handshake limit")
	}
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
		return s.rejectAuthAndClose(AuthRejectTooLarge, AuthRejectTooLarge, "auth payload too large")
	}
	req, err := DecodeAuthRequestView(frame.Payload)
	if err != nil {
		_ = s.rejectAuthAndClose(AuthRejectUnauthorized, AuthRejectProtocol, "invalid auth payload")
		return err
	}
	if s.config.Auth.Handler == nil {
		if s.config.Auth.Required {
			return s.rejectAuthAndClose(AuthRejectUnauthorized, AuthRejectUnauthorized, "auth handler is not configured")
		}
		return s.AcceptAuth(AuthResult{OK: true})
	}
	authCtx, cancel := context.WithTimeout(ctx, s.config.Auth.Timeout)
	defer cancel()
	req.Payload = append([]byte(nil), req.Payload...)
	type authOutcome struct {
		result AuthResult
		err    error
	}
	outcomes := make(chan authOutcome, 1)
	go func() {
		result, handlerErr := callAuthHandlerSafely(s.config.Auth.Handler, authCtx, req)
		outcomes <- authOutcome{result: result, err: handlerErr}
	}()
	var outcome authOutcome
	select {
	case outcome = <-outcomes:
	case <-authCtx.Done():
		_ = s.rejectAuthAndClose(AuthRejectTimeout, AuthRejectTimeout, "auth timeout")
		s.emitEvent(ProtocolEvent{Code: EventAuthTimeout, Message: "auth timeout"})
		return authCtx.Err()
	}
	if outcome.err != nil {
		_ = s.rejectAuthAndClose(AuthRejectUnauthorized, AuthRejectUnauthorized, "authentication failed")
		return outcome.err
	}
	result := outcome.result
	if !result.OK {
		code := result.RejectCode
		if code == 0 {
			code = AuthRejectUnauthorized
		}
		reason := result.Reason
		if reason == "" {
			reason = "unauthorized"
		}
		return s.rejectAuthAndClose(code, code, reason)
	}
	return s.AcceptAuth(result)
}

func (s *Session) AcceptAuth(result AuthResult) error {
	if uint64(len(result.UserID))+4 > uint64(s.config.Payload.PreHandshakeBytes-HeaderSize) {
		return errors.New("auth user id exceeds configured pre-handshake limit")
	}
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
	if uint64(len(message))+8 > uint64(s.config.Payload.PreHandshakeBytes-HeaderSize) {
		return errors.New("auth reject message exceeds configured pre-handshake limit")
	}
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
	if reasonCode != AuthRejectTimeout {
		s.emitEvent(ProtocolEvent{Code: EventAuthRejected, Message: message, FrameType: FrameAuthReject})
	}
	return nil
}

func (s *Session) rejectAuthAndClose(statusCode uint16, reasonCode uint16, message string) error {
	if err := s.RejectAuth(statusCode, reasonCode, message); err != nil {
		return err
	}
	if s.writer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), s.config.SendQueue.WriteTimeout)
		err := s.Flush(ctx)
		cancel()
		if err != nil {
			return err
		}
		s.writer.close()
	}
	return s.t.Close()
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
	s.authOnce.Do(func() {
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
			_ = s.rejectAuthAndClose(AuthRejectTimeout, AuthRejectTimeout, "auth timeout")
			s.emitEvent(ProtocolEvent{Code: EventAuthTimeout, Message: "auth timeout"})
		}()
	})
}

func (s *Session) SendHello(role string) error {
	if s.config.Role != RoleClient || (s.State() != SessionNew && s.State() != SessionAuthAccepted) {
		return fmt.Errorf("hello is not allowed for role=%s state=%s", s.config.Role, s.State())
	}
	if role != "" && role != RoleClient {
		return fmt.Errorf("invalid hello role: %q", role)
	}
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
	if s.config.Role != RoleServer {
		return errors.New("only server sessions can send hello ack")
	}
	f := NewFrame(FrameHelloAck, SchemaHello, EncodeHelloMessage(s.hello(role)))
	f.Header.Priority = PriorityCritical
	f.Header.ChannelID = ChannelControl
	f.Header.Flags = FlagControl
	if err := s.send(f); err != nil {
		return err
	}
	s.setState(SessionEstablished)
	s.applyReceiveFrameLimit()
	return nil
}

func (s *Session) SendText(text string) error {
	if err := s.requireEstablished("send text"); err != nil {
		return err
	}
	if err := s.EnsureAuthenticatedFor(Frame{Header: Header{FrameType: FrameData}}); err != nil {
		return err
	}
	payloadSize := uint64(4) + uint64(len(text))
	if payloadSize > uint64(s.config.Payload.MaxTextBytes) || payloadSize > uint64(s.config.Payload.MaxFrameBytes-HeaderSize) {
		return fmt.Errorf("text payload exceeds max size: %d", payloadSize)
	}
	requestID := s.nextRequest.Add(1)
	if s.writer != nil {
		size := HeaderSize + 4 + len(text)
		if err := s.checkRemoteFrameSize(size); err != nil {
			return err
		}
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
		if err == nil {
			s.markWrite()
		}
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
	payloadSize := eventMessagePayloadSize64(len(event), len(data), fields)
	return s.sendRequestWithIDSize(requestID, event, data, fields, payloadSize)
}

func (s *Session) sendRequestWithIDSize(requestID uint64, event string, data []byte, fields []TransferField, payloadSize uint64) error {
	if err := s.requireEstablished("send request"); err != nil {
		return err
	}
	if err := s.requireCapability(CapabilityRequestResponse, "request/response"); err != nil {
		return err
	}
	if err := s.EnsureAuthenticatedFor(Frame{Header: Header{FrameType: FrameRequest}}); err != nil {
		return err
	}
	if payloadSize > uint64(s.config.Payload.MaxRequestBytes) {
		return fmt.Errorf("request payload exceeds max size: %d > %d", payloadSize, s.config.Payload.MaxRequestBytes)
	}
	return s.sendEventFrame(FrameRequest, requestID, event, data, fields, payloadSize)
}

func (s *Session) SendResponse(requestID uint64, event string, data []byte) error {
	return s.SendResponseFields(requestID, event, data, nil)
}

func (s *Session) SendResponseFields(requestID uint64, event string, data []byte, fields []TransferField) error {
	return s.sendResponseWithID(requestID, event, data, fields)
}

func (s *Session) sendResponseWithID(requestID uint64, event string, data []byte, fields []TransferField) error {
	payloadSize := eventMessagePayloadSize64(len(event), len(data), fields)
	return s.sendResponseWithIDSize(requestID, event, data, fields, payloadSize)
}

func (s *Session) sendResponseWithIDSize(requestID uint64, event string, data []byte, fields []TransferField, payloadSize uint64) error {
	if requestID == 0 {
		return errors.New("response request id is required")
	}
	if err := s.requireEstablished("send response"); err != nil {
		return err
	}
	if err := s.requireCapability(CapabilityRequestResponse, "request/response"); err != nil {
		return err
	}
	if err := s.EnsureAuthenticatedFor(Frame{Header: Header{FrameType: FrameResponse}}); err != nil {
		return err
	}
	if payloadSize > uint64(s.config.Payload.MaxResponseBytes) {
		return fmt.Errorf("response payload exceeds max size: %d > %d", payloadSize, s.config.Payload.MaxResponseBytes)
	}
	return s.sendEventFrame(FrameResponse, requestID, event, data, fields, payloadSize)
}

func (s *Session) sendEventFrame(frameType uint8, requestID uint64, event string, data []byte, fields []TransferField, payloadSize64 uint64) error {
	if payloadSize64 > uint64(s.config.Payload.MaxFrameBytes-HeaderSize) {
		return fmt.Errorf("event payload exceeds max frame size: %d", payloadSize64)
	}
	payloadSize := int(payloadSize64)
	if s.writer != nil {
		size := HeaderSize + payloadSize
		if err := s.checkRemoteFrameSize(size); err != nil {
			return err
		}
		bufp := getFrameBuffer(size)
		frameData := (*bufp)[:size]
		payload := encodeEventMessageStringWithFieldsSizedInto(frameData[HeaderSize:HeaderSize], event, data, fields, payloadSize)
		encodeFrameHeaderInto(frameData[:HeaderSize], Header{
			FrameType: frameType,
			Flags:     FlagFirst | FlagLast,
			Priority:  PriorityNormal,
			ChannelID: ChannelRealtime,
			SchemaID:  SchemaEvent,
			RequestID: requestID,
		}, len(payload))
		err := s.writer.enqueue(context.Background(), sendItem{
			priority: PriorityNormal,
			channel:  ChannelRealtime,
			data:     frameData,
			bufp:     bufp,
		})
		if err == nil {
			s.markWrite()
		}
		return err
	}
	payloadBuf := getFrameBuffer(payloadSize)
	payload := encodeEventMessageStringWithFieldsSizedInto((*payloadBuf)[:0], event, data, fields, payloadSize)
	f := NewFrame(frameType, SchemaEvent, payload)
	f.Header.Priority = PriorityNormal
	f.Header.ChannelID = ChannelRealtime
	f.Header.RequestID = requestID
	f.Header.Flags = FlagFirst | FlagLast
	err := s.send(f)
	putFrameBuffer(payloadBuf)
	return err
}

func (s *Session) Send(ctx context.Context, opts MessageOptions) (MessageHandle, error) {
	return s.Request(ctx, opts)
}

func (s *Session) Request(ctx context.Context, opts MessageOptions) (MessageHandle, error) {
	requestID := s.nextRequest.Add(1)
	if payloadSize, ok := s.inlinePayloadSize(opts, s.config.Payload.MaxRequestBytes); ok {
		return MessageHandle{RequestID: requestID}, s.sendRequestWithIDSize(requestID, opts.Event, opts.Data, opts.Fields, payloadSize)
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
	if payloadSize, ok := s.inlinePayloadSize(opts, s.config.Payload.MaxResponseBytes); ok {
		return MessageHandle{RequestID: requestID}, s.sendResponseWithIDSize(requestID, opts.Event, opts.Data, opts.Fields, payloadSize)
	}
	handle, err := s.startMessageTransfer(ctx, requestID, opts)
	if err != nil {
		return MessageHandle{}, err
	}
	return handle, nil
}

func (s *Session) inlinePayloadSize(opts MessageOptions, maxPayload uint32) (uint64, bool) {
	if opts.Reader != nil {
		return 0, false
	}
	payloadSize := eventMessagePayloadSize64(len(opts.Event), len(opts.Data), opts.Fields)
	return payloadSize, payloadSize <= uint64(maxPayload) && payloadSize <= uint64(s.config.Payload.MaxInlineBodyBytes)
}

func (s *Session) startMessageTransfer(ctx context.Context, requestID uint64, opts MessageOptions) (MessageHandle, error) {
	reader := opts.Reader
	size := opts.Size
	if reader == nil {
		size = uint64(len(opts.Data))
	} else if size == 0 {
		return MessageHandle{}, errors.New("message reader size is required")
	}
	if opts.ContentType == 0 {
		opts.ContentType = ContentFile
	}
	transferOpts := TransferOptions{
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
	}
	if reader == nil {
		transferOpts.data = opts.Data
		transferOpts.useData = true
	}
	transferID, done, _ := s.launchTransfer(ctx, transferOpts, false)
	return MessageHandle{RequestID: requestID, TransferID: transferID, done: done}, nil
}

func (s *Session) SendWindow(window Window) error {
	if err := s.requireCapability(CapabilityFlowControl, "flow control"); err != nil {
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
	return s.requestTransferResume(0, resume)
}

func (s *Session) requestTransferResume(requestID uint64, resume TransferResume) error {
	if err := s.requireEstablished("resume transfer"); err != nil {
		return err
	}
	if err := s.requireCapability(CapabilityTransferResume, "transfer resume"); err != nil {
		return err
	}
	payload, err := EncodeTransferResumeInto(nil, resume.TransferID, resume.ReceivedBytes, resume.NextChunk, resume.Token)
	if err != nil {
		return err
	}
	f := NewFrame(FrameTransferResume, SchemaTransferState, payload)
	f.Header.Priority = PriorityHigh
	f.Header.ChannelID = ChannelBulk
	f.Header.RequestID = requestID
	f.Header.TransferID = resume.TransferID
	f.Header.Flags = FlagControl
	return s.send(f)
}

func (s *Session) ResumeTransfer(ctx context.Context, opts ResumeTransferOptions) *TransferHandle {
	opts.TransferOptions = s.normalizeTransferOptions(opts.TransferOptions)
	if ctx == nil {
		ctx = context.Background()
	}
	parentCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	handle := &TransferHandle{
		TransferID: opts.TransferID,
		done:       make(chan error, 1),
		cancel:     cancel,
		session:    s,
	}
	var registerErr error
	if opts.TransferID == 0 {
		registerErr = errors.New("resume transfer id is required")
	} else if opts.OpenReader == nil {
		registerErr = errors.New("resume reader opener is required")
	} else {
		registerErr = s.registerOutgoing(opts.TransferID, opts.TransferOptions)
	}
	if registerErr == nil {
		s.bumpTransferID(opts.TransferID)
	}
	go func() {
		defer close(handle.done)
		var stopContextWatch func() bool
		if registerErr == nil && parentCtx.Done() != nil {
			stopContextWatch = context.AfterFunc(parentCtx, func() {
				if ot := s.acquireOutgoingTransfer(opts.TransferID); ot != nil {
					defer ot.refs.Done()
					ot.fail(parentCtx.Err())
				}
			})
		}
		err := registerErr
		if err == nil {
			err = callSafely("outgoing resumed transfer", func() error {
				return s.sendResumedTransfer(ctx, opts)
			})
		}
		if stopContextWatch != nil {
			stopContextWatch()
		}
		if registerErr == nil {
			s.unregisterOutgoing(opts.TransferID)
		}
		cancel()
		handle.done <- err
	}()
	return handle
}

func (s *Session) sendResumedTransfer(ctx context.Context, opts ResumeTransferOptions) error {
	if err := s.requireEstablished("resume transfer"); err != nil {
		return err
	}
	if s.config.Capabilities&CapabilityTransferResume == 0 {
		return errors.New("local transfer resume support is disabled")
	}
	if err := s.requireRemoteCapability(CapabilityTransferResume, "transfer resume"); err != nil {
		return err
	}
	if err := s.validateOutgoingTransfer(opts.TransferOptions, false); err != nil {
		return err
	}
	hintBytes, validHint := transferOffsetForChunk(opts.TotalSize, uint32(opts.ChunkSize), opts.NextChunk)
	if !validHint || opts.ReceivedBytes != hintBytes {
		return errors.New("invalid local transfer resume counters")
	}
	if err := s.requestTransferResume(opts.RequestID, TransferResume{
		TransferID:    opts.TransferID,
		ReceivedBytes: opts.ReceivedBytes,
		NextChunk:     opts.NextChunk,
		Token:         opts.Token,
	}); err != nil {
		return err
	}
	receivedBytes, nextChunk, err := s.waitResumeDecision(ctx, opts.TransferID)
	if err != nil {
		return err
	}
	reader, err := opts.OpenReader(receivedBytes)
	if err != nil {
		_ = s.CancelTransfer(opts.TransferID, CancelProtocol, CancelDeletePartial)
		return fmt.Errorf("open resumed transfer reader at %d: %w", receivedBytes, err)
	}
	if reader == nil {
		_ = s.CancelTransfer(opts.TransferID, CancelProtocol, CancelDeletePartial)
		return errors.New("resume reader opener returned nil")
	}
	opts.Reader = reader
	return s.sendTransferBody(ctx, opts.TransferID, opts.TransferOptions, receivedBytes, nextChunk)
}

func (s *Session) waitResumeDecision(ctx context.Context, transferID uint64) (uint64, uint32, error) {
	ot := s.acquireOutgoingTransfer(transferID)
	if ot == nil {
		return 0, 0, errors.New("resumed outgoing transfer is not registered")
	}
	defer ot.refs.Done()
	ot.mu.Lock()
	defer ot.mu.Unlock()
	for !ot.resumeDecided && ot.failed == nil {
		ot.cond.Wait()
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
	}
	if ot.failed != nil {
		return 0, 0, ot.failed
	}
	if !ot.resumeAccepted {
		return 0, 0, errors.New("remote rejected transfer resume")
	}
	return ot.resumeBytes, ot.resumeChunk, nil
}

func (s *Session) bumpTransferID(transferID uint64) {
	for {
		current := s.nextTransfer.Load()
		if current >= transferID || s.nextTransfer.CompareAndSwap(current, transferID) {
			return
		}
	}
}

func (s *Session) SendTransferState(state TransferStateMessage) error {
	payloadBuf := getFrameBuffer(24)
	payload := EncodeTransferStateMessageInto((*payloadBuf)[:0], state)
	defer putFrameBuffer(payloadBuf)
	f := NewFrame(FrameTransferState, SchemaTransferState, payload)
	f.Header.Priority = PriorityHigh
	f.Header.ChannelID = ChannelControl
	f.Header.TransferID = state.TransferID
	f.Header.Flags = FlagControl
	return s.send(f)
}

func (s *Session) HandleWindow(frame Frame) error {
	window, err := DecodeWindow(frame.Payload)
	if err != nil {
		return err
	}
	if window.TransferID == 0 || (frame.Header.TransferID != 0 && window.TransferID != frame.Header.TransferID) || window.Flags != WindowFlagTransfer {
		return errors.New("invalid transfer window")
	}
	ot := s.lockOutgoingTransfer(window.TransferID)
	if ot == nil {
		return nil
	}
	ot.remoteWindowBytes = window.WindowBytes
	ot.remoteWindowChunks = window.WindowChunks
	ot.hasRemoteWindow = true
	ot.cond.Broadcast()
	ot.mu.Unlock()
	return nil
}

func (s *Session) SendTransfer(name string, contentType uint32, r io.Reader, totalSize uint64) (uint64, error) {
	opts := s.normalizeTransferOptions(TransferOptions{
		Name:        name,
		ContentType: contentType,
		Reader:      r,
		TotalSize:   totalSize,
	})
	transferID := s.nextTransfer.Add(1)
	if err := s.registerOutgoing(transferID, opts); err != nil {
		return transferID, err
	}
	ctx := context.Background()
	err := callSafely("outgoing transfer", func() error {
		return s.sendTransfer(ctx, transferID, opts)
	})
	s.unregisterOutgoing(transferID)
	return transferID, err
}

func (s *Session) StartTransfer(ctx context.Context, opts TransferOptions) *TransferHandle {
	transferID, done, cancel := s.launchTransfer(ctx, opts, true)
	return &TransferHandle{
		TransferID: transferID,
		done:       done,
		cancel:     cancel,
		session:    s,
	}
}

func (s *Session) launchTransfer(ctx context.Context, opts TransferOptions, cancelable bool) (uint64, chan error, context.CancelFunc) {
	opts = s.normalizeTransferOptions(opts)
	if ctx == nil {
		ctx = context.Background()
	}
	parentCtx := ctx
	var cancel context.CancelFunc
	if cancelable {
		ctx, cancel = context.WithCancel(ctx)
	}
	transferID := s.nextTransfer.Add(1)
	done := make(chan error, 1)
	registerErr := s.registerOutgoing(transferID, opts)
	go func() {
		defer close(done)
		var stopContextWatch func() bool
		if registerErr == nil && parentCtx.Done() != nil {
			stopContextWatch = context.AfterFunc(parentCtx, func() {
				if ot := s.acquireOutgoingTransfer(transferID); ot != nil {
					defer ot.refs.Done()
					ot.fail(parentCtx.Err())
				}
			})
		}
		err := registerErr
		if err == nil {
			err = callSafely("outgoing transfer", func() error {
				return s.sendTransfer(ctx, transferID, opts)
			})
		}
		if stopContextWatch != nil {
			stopContextWatch()
		}
		if registerErr == nil {
			s.unregisterOutgoing(transferID)
		}
		if cancel != nil {
			cancel()
		}
		done <- err
	}()
	return transferID, done, cancel
}

func (s *Session) normalizeTransferOptions(opts TransferOptions) TransferOptions {
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = DefaultChunkSize
	}
	if opts.ChunkSize > s.config.FlowControl.MaxChunkSize {
		opts.ChunkSize = s.config.FlowControl.MaxChunkSize
	}
	if remote := s.remoteLimits(); remote.MaxChunkSize > 0 && uint32(opts.ChunkSize) > remote.MaxChunkSize {
		opts.ChunkSize = int(remote.MaxChunkSize)
	}
	return opts
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
	if err := s.requireEstablished("start transfer"); err != nil {
		return err
	}
	if err := s.validateOutgoingTransfer(opts, true); err != nil {
		return err
	}
	if err := s.outgoingFailure(transferID); err != nil {
		return err
	}
	chunkCount := chunkCountFor(opts.TotalSize, uint32(opts.ChunkSize))

	beginPayloadSize := int(transferBeginPayloadSize64(opts.Name, opts.Event, opts.Field, opts.Parts, opts.Fields))
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

	return s.sendTransferBody(ctx, transferID, opts, 0, 0)
}

func (s *Session) validateOutgoingTransfer(opts TransferOptions, requireReader bool) error {
	const required = CapabilityTransfers | CapabilityTransferCommit | CapabilityAck
	if s.config.Capabilities&required != required {
		return errors.New("local transfer, commit, and ack capabilities are required")
	}
	if err := s.requireRemoteCapability(CapabilityTransfers, "transfers"); err != nil {
		return err
	}
	if err := s.requireRemoteCapability(CapabilityTransferCommit, "transfer commit"); err != nil {
		return err
	}
	if err := s.requireRemoteCapability(CapabilityAck, "ack"); err != nil {
		return err
	}
	if err := s.EnsureAuthenticatedFor(Frame{Header: Header{FrameType: FrameTransferBegin}}); err != nil {
		return err
	}
	if opts.TotalSize > s.config.FlowControl.MaxTransferBytes {
		return fmt.Errorf("transfer exceeds max size: %d > %d", opts.TotalSize, s.config.FlowControl.MaxTransferBytes)
	}
	if remote := s.remoteLimits(); remote.MaxTransferBytes > 0 && opts.TotalSize > remote.MaxTransferBytes {
		return fmt.Errorf("transfer exceeds remote max size: %d > %d", opts.TotalSize, remote.MaxTransferBytes)
	}
	if requireReader && opts.Reader == nil && !opts.useData {
		return errors.New("transfer reader is required")
	}
	if opts.ChunkSize <= 0 || opts.ChunkSize > s.config.FlowControl.MaxChunkSize {
		return fmt.Errorf("invalid transfer chunk size: %d", opts.ChunkSize)
	}
	if opts.TotalSize > 0 && chunkCountFor(opts.TotalSize, uint32(opts.ChunkSize)) == ^uint32(0) {
		return errors.New("transfer requires too many chunks")
	}
	if len(opts.Parts) > MaxTransferParts || len(opts.Fields) > MaxTransferFields {
		return errors.New("transfer contains too many parts or fields")
	}
	if len(opts.Parts) > 0 {
		var partsSize uint64
		for _, part := range opts.Parts {
			if part.TotalSize > opts.TotalSize-partsSize {
				return errors.New("transfer part sizes exceed total size")
			}
			partsSize += part.TotalSize
		}
		if partsSize != opts.TotalSize {
			return errors.New("transfer part sizes do not match total size")
		}
	}
	if (opts.RequestID == 0) != (opts.Event == "") {
		return errors.New("transfer event and request id must be set together")
	}
	if opts.ChecksumMode == ChecksumTransferSHA256 {
		if opts.Checksum == ([32]byte{}) {
			return errors.New("sha256 checksum is required when checksum mode is enabled")
		}
		if s.config.Capabilities&CapabilityTransferSHA256 == 0 {
			return errors.New("local transfer checksum support is disabled")
		}
		if err := s.requireRemoteCapability(CapabilityTransferSHA256, "transfer sha256"); err != nil {
			return err
		}
	}
	beginSize := uint64(HeaderSize) + transferBeginPayloadSize64(opts.Name, opts.Event, opts.Field, opts.Parts, opts.Fields)
	if beginSize > uint64(s.config.Payload.MaxFrameBytes) {
		return fmt.Errorf("transfer begin exceeds local max frame size: %d > %d", beginSize, s.config.Payload.MaxFrameBytes)
	}
	if remote := s.remoteMaxFrameBytes(); remote > 0 && beginSize > uint64(remote) {
		return fmt.Errorf("frame exceeds remote max size: %d > %d", beginSize, remote)
	}
	return nil
}

func (s *Session) sendTransferBody(ctx context.Context, transferID uint64, opts TransferOptions, written uint64, chunkID uint32) error {
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

		if written >= opts.TotalSize {
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
		var n int
		var readErr error
		if opts.useData {
			n = copy(payload, opts.data[written:written+uint64(nextReadSize)])
		} else {
			n, readErr = io.ReadFull(opts.Reader, payload)
		}
		if readErr != nil {
			putChunkBuffer(payloadBuf)
			if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
				return fmt.Errorf("transfer reader ended early: read=%d expected=%d: %w", written+uint64(n), opts.TotalSize, io.ErrUnexpectedEOF)
			}
			return readErr
		}
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
				putChunkBuffer(payloadBuf)
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
	}

	end := NewFrame(FrameTransferEnd, 0, nil)
	end.Header.Priority = PriorityLow
	end.Header.ChannelID = ChannelBulk
	end.Header.RequestID = opts.RequestID
	end.Header.TransferID = transferID
	end.Header.Flags = FlagLast
	ot := s.acquireOutgoingTransfer(transferID)
	if ot != nil {
		ot.mu.Lock()
		ot.doneSending = true
		ot.endFrame = end
		ot.commitSentAt = time.Now()
		ot.mu.Unlock()
		ot.refs.Done()
	}
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
	if err := s.requireCapability(CapabilityCancel, "cancel"); err != nil {
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
	if err := s.requireCapability(CapabilityAck, "ack"); err != nil {
		return err
	}
	ack, err := DecodeAck(frame.Payload)
	if err != nil {
		return err
	}
	if ack.TransferID == 0 || (frame.Header.TransferID != 0 && ack.TransferID != frame.Header.TransferID) || ack.ChunkFrom > ack.ChunkTo {
		return errors.New("invalid ack range or transfer id")
	}
	if err := s.applyAck(ack); err != nil {
		return err
	}
	s.emitProgress(Progress{
		TransferID:       ack.TransferID,
		RemoteAckedBytes: ack.ReceivedBytes,
		AckedChunks:      ack.ChunkTo + 1,
		State:            TransferAcked,
	})
	return nil
}

func (s *Session) HandleNack(frame Frame) error {
	if err := s.requireCapability(CapabilityNack, "nack"); err != nil {
		return err
	}
	nack, err := DecodeNack(frame.Payload)
	if err != nil {
		return err
	}
	if nack.TransferID == 0 || (frame.Header.TransferID != 0 && nack.TransferID != frame.Header.TransferID) || nack.ChunkFrom > nack.ChunkTo {
		return errors.New("invalid nack range or transfer id")
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
	if err := s.requireCapability(CapabilityHeartbeat, "heartbeat"); err != nil {
		return err
	}
	f := NewFrame(FramePing, 0, nil)
	f.Header.Priority = PriorityCritical
	f.Header.ChannelID = ChannelControl
	f.Header.Flags = FlagControl
	return s.send(f)
}

func (s *Session) SendPong() error {
	if err := s.requireCapability(CapabilityHeartbeat, "heartbeat"); err != nil {
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
	s.heartbeatOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(s.config.Heartbeat.Interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					state := s.State()
					if state == SessionClosed || state == SessionFailed {
						return
					}
					now := time.Now()
					lastRead := time.Unix(0, s.lastReadNano.Load())
					if now.Sub(lastRead) > s.config.Heartbeat.Timeout {
						s.setState(SessionFailed)
						s.emitEvent(ProtocolEvent{
							Code:    EventProtocolViolation,
							Message: "heartbeat timeout",
						})
						s.failOutgoing(errors.New("heartbeat timeout"))
						s.closeWriter()
						_ = s.t.Close()
						return
					}
					if s.config.Role == RoleServer || state != SessionEstablished {
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
						s.failOutgoing(err)
						s.closeWriter()
						_ = s.t.Close()
						return
					}
				}
			}
		}()
	})
}

func (s *Session) markWrite() {
	s.lastWriteNano.Store(time.Now().UnixNano())
}

func (s *Session) emitProgress(p Progress) {
	s.callbackMu.RLock()
	fn := s.onProgress
	s.callbackMu.RUnlock()
	if fn != nil {
		_ = callSafely("progress callback", func() error {
			fn(p)
			return nil
		})
	}
}

func (s *Session) emitEvent(e ProtocolEvent) {
	s.callbackMu.RLock()
	fn := s.onEvent
	s.callbackMu.RUnlock()
	if fn != nil {
		_ = callSafely("protocol event callback", func() error {
			fn(e)
			return nil
		})
	}
}

func (s *Session) setState(state SessionState) {
	s.state.Store(uint32(state))
	if state == SessionClosed || state == SessionFailed || state == SessionAuthRejected {
		s.stopOutgoingMonitor()
	}
}

func (s *Session) requireEstablished(operation string) error {
	if s.State() != SessionEstablished {
		return fmt.Errorf("%s requires an established session, current state is %s", operation, s.State())
	}
	return nil
}

func (s *Session) hello(role string) Hello {
	if role == "" {
		role = s.config.Role
	}
	return Hello{
		Role:              role,
		Capabilities:      s.config.Capabilities,
		MaxFrameBytes:     s.config.Payload.MaxFrameBytes,
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
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if closeMessage.Flags == 0 {
		closeMessage.Flags = CloseFlagImmediate
	}
	if closeMessage.Flags&^uint16(CloseFlagImmediate|CloseFlagDrain|CloseFlagNoNewRequests|CloseFlagNoNewTransfers) != 0 || closeMessage.Flags&(CloseFlagImmediate|CloseFlagDrain) == CloseFlagImmediate|CloseFlagDrain {
		return errors.New("invalid close flags")
	}
	draining := closeMessage.Flags&CloseFlagDrain != 0
	if draining {
		if err := s.requireCapability(CapabilityGracefulClose, "graceful close"); err != nil {
			return err
		}
	}
	targetState := SessionClosing
	if draining {
		targetState = SessionDraining
	}
	if !s.beginLocalClose(targetState) {
		return nil
	}
	if !draining {
		s.cleanupIncoming("local session closed", false)
		s.failOutgoing(ErrSessionClosed)
		if s.writer != nil {
			s.writer.beginClosing()
		}
	}
	f := NewFrame(FrameClose, SchemaClose, EncodeCloseMessage(closeMessage))
	f.Header.Priority = PriorityCritical
	f.Header.ChannelID = ChannelControl
	f.Header.Flags = FlagControl
	sendCtx, sendCancel := context.WithTimeout(context.Background(), s.config.SendQueue.EnqueueTimeout+s.config.SendQueue.WriteTimeout)
	err := s.sendAndWait(sendCtx, f)
	sendCancel()
	if err != nil {
		if s.peerCloseSeen.Load() {
			if peerErr := s.waitPeerCloseDone(); peerErr == nil {
				if s.writer != nil {
					s.writer.close()
				}
				s.setState(SessionClosed)
				_ = s.t.Close()
				return nil
			}
		}
		s.setState(SessionFailed)
		return fmt.Errorf("send close: %w", err)
	}
	if draining {
		timeout := time.Duration(closeMessage.DrainTimeoutMillis) * time.Millisecond
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := s.waitTransfersDrained(ctx); err != nil {
			s.setState(SessionFailed)
			s.failOutgoing(err)
			_ = s.t.Close()
			return fmt.Errorf("drain local transfers: %w", err)
		}
		select {
		case <-ctx.Done():
			s.setState(SessionFailed)
			_ = s.t.Close()
			return fmt.Errorf("wait close ack: %w", ctx.Err())
		case <-s.closeAck:
		}
		s.setState(SessionClosing)
	} else {
		timer := time.NewTimer(s.config.Close.AckTimeout)
		select {
		case <-s.closeAck:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			if s.writer != nil {
				s.writer.close()
			}
			s.setState(SessionClosed)
			_ = s.t.Close()
			return fmt.Errorf("wait close ack: timeout after %s", s.config.Close.AckTimeout)
		}
	}
	if err := s.waitPeerCloseDone(); err != nil {
		s.setState(SessionFailed)
		_ = s.t.Close()
		return fmt.Errorf("send peer close ack: %w", err)
	}
	if s.writer != nil {
		s.writer.close()
	}
	s.setState(SessionClosed)
	if err := s.t.Close(); err != nil {
		s.setState(SessionFailed)
		return err
	}
	return nil
}

func (s *Session) beginLocalClose(target SessionState) bool {
	for {
		state := s.State()
		switch state {
		case SessionClosed, SessionClosing, SessionDraining:
			return false
		}
		if s.state.CompareAndSwap(uint32(state), uint32(target)) {
			return true
		}
	}
}

func (s *Session) waitPeerCloseDone() error {
	if !s.peerCloseSeen.Load() {
		return nil
	}
	timer := time.NewTimer(s.config.Close.AckTimeout)
	defer timer.Stop()
	select {
	case err := <-s.peerCloseDone:
		return err
	case <-timer.C:
		return fmt.Errorf("timeout after %s", s.config.Close.AckTimeout)
	}
}

func (s *Session) waitTransfersDrained(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		drained := len(s.outgoing) == 0 && len(s.incoming) == 0 && len(s.opening) == 0
		s.mu.Unlock()
		if drained {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Session) SendCloseAck(closeMessage CloseMessage) error {
	f := NewFrame(FrameCloseAck, SchemaClose, EncodeCloseMessage(closeMessage))
	f.Header.Priority = PriorityCritical
	f.Header.ChannelID = ChannelControl
	f.Header.Flags = FlagControl
	return s.send(f)
}

func (s *Session) sendCloseAckAndWait(ctx context.Context, closeMessage CloseMessage) error {
	f := NewFrame(FrameCloseAck, SchemaClose, EncodeCloseMessage(closeMessage))
	f.Header.Priority = PriorityCritical
	f.Header.ChannelID = ChannelControl
	f.Header.Flags = FlagControl
	return s.sendAndWait(ctx, f)
}

func (s *Session) SendError(code uint32, frame Frame, message string) error {
	if uint64(len(message))+32 > uint64(s.config.Payload.MaxFrameBytes-HeaderSize) {
		return errors.New("error message exceeds max frame size")
	}
	payloadSize := ErrorMessagePayloadSizeString(message)
	size := HeaderSize + payloadSize
	if err := s.checkRemoteFrameSize(size); err != nil {
		return err
	}
	bufp := getFrameBuffer(size)
	data := (*bufp)[:size]
	payload, err := EncodeErrorMessageStringInto(data[HeaderSize:HeaderSize], code, frame.Header.FrameType, frame.Header.SchemaID, frame.Header.RequestID, frame.Header.TransferID, message)
	if err != nil {
		putFrameBuffer(bufp)
		return err
	}
	encodeFrameHeaderInto(data[:HeaderSize], Header{
		FrameType:  FrameError,
		Flags:      FlagControl | FlagFirst | FlagLast,
		Priority:   PriorityCritical,
		ChannelID:  ChannelControl,
		SchemaID:   SchemaError,
		RequestID:  frame.Header.RequestID,
		TransferID: frame.Header.TransferID,
	}, len(payload))
	if s.writer != nil {
		err = s.writer.enqueue(context.Background(), sendItem{
			priority: PriorityCritical,
			channel:  ChannelControl,
			data:     data,
			bufp:     bufp,
		})
	} else if s.t == nil {
		putFrameBuffer(bufp)
		return errors.New("frame transport is nil")
	} else {
		err = s.t.SendFrame(data)
		putFrameBuffer(bufp)
	}
	if err == nil {
		s.markWrite()
	}
	return err
}

func (s *Session) SendGoAway(goAway GoAway) error {
	if uint64(len(goAway.Message))+32 > uint64(s.config.Payload.MaxFrameBytes-HeaderSize) {
		return errors.New("goaway message exceeds max frame size")
	}
	if goAway.Flags == 0 {
		goAway.Flags = CloseFlagImmediate
	}
	if goAway.Flags&^uint16(CloseFlagImmediate|CloseFlagDrain|CloseFlagNoNewRequests|CloseFlagNoNewTransfers) != 0 || goAway.Flags&(CloseFlagImmediate|CloseFlagDrain) == CloseFlagImmediate|CloseFlagDrain {
		return errors.New("invalid goaway flags")
	}
	if goAway.Flags&CloseFlagDrain != 0 {
		if err := s.requireCapability(CapabilityGracefulClose, "graceful close"); err != nil {
			return err
		}
	}
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
	ctx, cancel := context.WithTimeout(context.Background(), s.config.SendQueue.EnqueueTimeout+s.config.SendQueue.WriteTimeout)
	err := s.sendAndWait(ctx, f)
	cancel()
	if err != nil {
		s.setState(SessionFailed)
		return err
	}
	return nil
}

func (s *Session) registerOutgoing(transferID uint64, opts TransferOptions) error {
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
	if remote := s.remoteLimits(); remote.MaxInFlightChunks > 0 && uint32(maxInFlight) > remote.MaxInFlightChunks {
		maxInFlight = int(remote.MaxInFlightChunks)
	}
	maxInFlightBytes := opts.MaxInFlightBytes
	if maxInFlightBytes == 0 {
		maxInFlightBytes = s.config.FlowControl.MaxInFlightBytes
	}
	if maxInFlightBytes > s.config.FlowControl.MaxSendBufferBytes {
		maxInFlightBytes = s.config.FlowControl.MaxSendBufferBytes
	}
	chunkCount := chunkCountFor(opts.TotalSize, uint32(opts.ChunkSize))
	var ot *outgoingTransfer
	if pooled := outgoingTransferPool.Get(); pooled != nil {
		ot = pooled.(*outgoingTransfer)
	} else {
		ot = new(outgoingTransfer)
	}
	pending := ot.pending
	*ot = outgoingTransfer{
		id:               transferID,
		totalBytes:       opts.TotalSize,
		chunkCount:       chunkCount,
		chunkSize:        uint32(opts.ChunkSize),
		ackTimeout:       ackTimeout,
		retryLimit:       retryLimit,
		maxInFlight:      maxInFlight,
		maxInFlightBytes: maxInFlightBytes,
		pending:          pending[:0],
	}
	pendingCapacity := initialPendingCapacity(maxInFlight, chunkCount)
	if cap(ot.pending) < pendingCapacity {
		ot.pending = make([]sentChunk, 0, pendingCapacity)
	}
	if s.RemoteSupports(CapabilityFlowControl) && s.remoteLimits().MaxInFlightChunks > 0 {
		ot.hasRemoteWindow = true
	}
	ot.cond.L = &ot.mu
	s.mu.Lock()
	if s.outgoing[transferID] != nil {
		s.mu.Unlock()
		putOutgoingTransfer(ot)
		return fmt.Errorf("duplicate outgoing transfer id: %d", transferID)
	}
	if len(s.outgoing) >= s.config.FlowControl.MaxConcurrentTransfers {
		s.mu.Unlock()
		putOutgoingTransfer(ot)
		return fmt.Errorf("too many concurrent transfers: %d", s.config.FlowControl.MaxConcurrentTransfers)
	}
	s.outgoing[transferID] = ot
	s.mu.Unlock()
	s.startOutgoingMonitor()
	return nil
}

func (s *Session) unregisterOutgoing(transferID uint64) {
	s.mu.Lock()
	ot := s.outgoing[transferID]
	delete(s.outgoing, transferID)
	s.mu.Unlock()
	if ot != nil {
		ot.refs.Wait()
		ot.releasePending()
		putOutgoingTransfer(ot)
	}
}

func putOutgoingTransfer(ot *outgoingTransfer) {
	pending := ot.pending
	if cap(pending) > 4 {
		pending = nil
	} else if cap(pending) > 0 {
		pending = pending[:cap(pending)]
		clear(pending)
		pending = pending[:0]
	}
	*ot = outgoingTransfer{pending: pending}
	outgoingTransferPool.Put(ot)
}

func (s *Session) acquireOutgoingTransfer(transferID uint64) *outgoingTransfer {
	s.mu.Lock()
	ot := s.outgoing[transferID]
	if ot != nil {
		ot.refs.Add(1)
	}
	s.mu.Unlock()
	return ot
}

func (s *Session) lockOutgoingTransfer(transferID uint64) *outgoingTransfer {
	s.mu.Lock()
	ot := s.outgoing[transferID]
	if ot == nil {
		s.mu.Unlock()
		return nil
	}
	ot.mu.Lock()
	s.mu.Unlock()
	return ot
}

func (s *Session) outgoingFailure(transferID uint64) error {
	ot := s.acquireOutgoingTransfer(transferID)
	if ot == nil {
		return nil
	}
	defer ot.refs.Done()
	ot.mu.Lock()
	err := ot.failed
	ot.mu.Unlock()
	return err
}

func (s *Session) sendReliableChunk(ctx context.Context, transferID uint64, frame Frame, payloadN uint64, pooledPayload *[]byte) error {
	ot := s.acquireOutgoingTransfer(transferID)
	if ot == nil {
		err := s.send(frame)
		putChunkBuffer(pooledPayload)
		return err
	}
	defer ot.refs.Done()
	ot.mu.Lock()
	for (len(ot.pending) >= ot.maxInFlight ||
		ot.pendingBytes+payloadN > ot.maxInFlightBytes ||
		(ot.hasRemoteWindow && (ot.remoteWindowBytes < payloadN || ot.remoteWindowChunks == 0))) && ot.failed == nil {
		s.scheduleOutgoingMonitor(nextOutgoingDeadlineLocked(ot))
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

type outgoingRetryTask struct {
	transfer *outgoingTransfer
	frame    Frame
	chunkID  uint32
	commit   bool
}

func (s *Session) startOutgoingMonitor() {
	s.outgoingOnce.Do(func() {
		go s.runOutgoingMonitor()
	})
}

func (s *Session) scheduleOutgoingMonitor(deadline time.Time) {
	if deadline.IsZero() {
		return
	}
	next := deadline.UnixNano()
	for {
		current := s.outgoingNext.Load()
		if current != 0 && current <= next {
			return
		}
		if s.outgoingNext.CompareAndSwap(current, next) {
			select {
			case s.outgoingWake <- struct{}{}:
			default:
			}
			return
		}
	}
}

func (s *Session) stopOutgoingMonitor() {
	s.stopOnce.Do(func() {
		close(s.outgoingStop)
	})
}

func (s *Session) runOutgoingMonitor() {
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	var timerC <-chan time.Time
	var armed int64
	for {
		select {
		case <-s.outgoingStop:
			return
		case <-s.outgoingWake:
			target := s.outgoingNext.Load()
			if target == 0 || target == armed {
				continue
			}
			if timerC != nil && !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			delay := time.Until(time.Unix(0, target))
			if delay < time.Millisecond {
				delay = time.Millisecond
			}
			timer.Reset(delay)
			timerC = timer.C
			armed = target
		case <-timerC:
			fired := armed
			armed = 0
			timerC = nil
			s.outgoingNext.CompareAndSwap(fired, 0)
			now := time.Now()
			if delay := s.processOutgoingTimeouts(now); delay >= 0 {
				s.scheduleOutgoingMonitor(now.Add(delay))
			}
		}
	}
}

func nextOutgoingDeadlineLocked(ot *outgoingTransfer) time.Time {
	var next time.Time
	if ot.doneSending && !ot.completed && !ot.commitSentAt.IsZero() {
		next = ot.commitSentAt.Add(ot.ackTimeout)
	}
	for i := range ot.pending {
		chunk := &ot.pending[i]
		if chunk.acked {
			continue
		}
		deadline := chunk.sentAt.Add(ot.ackTimeout)
		if next.IsZero() || deadline.Before(next) {
			next = deadline
		}
	}
	return next
}

func (s *Session) processOutgoingTimeouts(now time.Time) time.Duration {
	next := time.Duration(-1)
	var taskStorage [16]outgoingRetryTask
	tasks := taskStorage[:0]
	var eventStorage [16]ProtocolEvent
	events := eventStorage[:0]

	s.mu.Lock()
	for _, ot := range s.outgoing {
		ot.mu.Lock()
		if ot.failed != nil || (ot.doneSending && len(ot.pending) == 0 && ot.completed) {
			ot.mu.Unlock()
			continue
		}
		failed := false
		if ot.doneSending && !ot.completed && !ot.commitSentAt.IsZero() {
			delay := ot.commitSentAt.Add(ot.ackTimeout).Sub(now)
			if delay <= 0 {
				if ot.commitRetries >= ot.retryLimit {
					err := fmt.Errorf("commit timeout for transfer=%d", ot.id)
					ot.failed = err
					ot.cond.Broadcast()
					events = append(events, ProtocolEvent{Code: EventAckTimeout, Message: err.Error(), TransferID: ot.id})
					failed = true
				} else {
					ot.commitRetries++
					ot.commitSentAt = now
					ot.monitorRefs++
					tasks = append(tasks, outgoingRetryTask{transfer: ot, frame: ot.endFrame, commit: true})
					delay = ot.ackTimeout
				}
			}
			if !failed && (next < 0 || delay < next) {
				next = delay
			}
		}
		if !failed {
			for i := range ot.pending {
				chunk := &ot.pending[i]
				if chunk.acked {
					continue
				}
				delay := chunk.sentAt.Add(ot.ackTimeout).Sub(now)
				if delay <= 0 {
					if chunk.retries >= ot.retryLimit {
						err := fmt.Errorf("ack timeout for transfer=%d chunk=%d", ot.id, chunk.chunkID)
						ot.failed = err
						ot.cond.Broadcast()
						events = append(events, ProtocolEvent{Code: EventAckTimeout, Message: err.Error(), TransferID: ot.id, ChunkID: chunk.chunkID})
						failed = true
						break
					}
					chunk.retries++
					chunk.sentAt = now
					frame := chunk.frame
					frame.Payload = append([]byte(nil), frame.Payload...)
					ot.monitorRefs++
					tasks = append(tasks, outgoingRetryTask{transfer: ot, frame: frame, chunkID: chunk.chunkID})
					delay = ot.ackTimeout
				}
				if next < 0 || delay < next {
					next = delay
				}
			}
		}
		ot.mu.Unlock()
	}
	s.mu.Unlock()

	for _, event := range events {
		s.emitEvent(event)
	}
	for _, task := range tasks {
		task.transfer.mu.Lock()
		active := task.transfer.failed == nil
		if active && task.commit {
			active = !task.transfer.completed
		} else if active {
			active = false
			for i := range task.transfer.pending {
				if task.transfer.pending[i].chunkID == task.chunkID {
					active = true
					break
				}
			}
		}
		task.transfer.mu.Unlock()
		if !active {
			releaseOutgoingMonitorRef(task.transfer)
			continue
		}
		if !task.commit {
			s.emitEvent(ProtocolEvent{Code: EventAckTimeout, Message: "resend chunk after ack timeout", TransferID: task.transfer.id, ChunkID: task.chunkID})
		}
		if err := s.send(task.frame); err != nil {
			task.transfer.fail(err)
		}
		releaseOutgoingMonitorRef(task.transfer)
	}
	return next
}

func releaseOutgoingMonitorRef(ot *outgoingTransfer) {
	ot.mu.Lock()
	ot.monitorRefs--
	ot.cond.Broadcast()
	ot.mu.Unlock()
}

func (s *Session) waitOutgoingAcked(ctx context.Context, transferID uint64) error {
	ot := s.acquireOutgoingTransfer(transferID)
	if ot == nil {
		return nil
	}
	defer ot.refs.Done()
	ot.mu.Lock()
	ot.doneSending = true
	for (len(ot.pending) > 0 || !ot.completed) && ot.failed == nil {
		s.scheduleOutgoingMonitor(nextOutgoingDeadlineLocked(ot))
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

func (s *Session) applyAck(ack Ack) error {
	ot := s.lockOutgoingTransfer(ack.TransferID)
	if ot == nil {
		return nil
	}
	if ack.ChunkTo >= ot.chunkCount || ack.ReceivedBytes < ot.ackedBytes || ack.ReceivedBytes > ot.totalBytes {
		ot.mu.Unlock()
		return errors.New("ack counters exceed outgoing transfer")
	}
	matched := false
	dst := ot.pending[:0]
	for _, chunk := range ot.pending {
		chunkID := chunk.chunkID
		if chunkID >= ack.ChunkFrom && chunkID <= ack.ChunkTo {
			matched = true
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
	if matched && ack.ReceivedBytes != ot.ackedBytes {
		err := errors.New("ack received byte counter does not match committed chunks")
		ot.failed = err
		ot.cond.Broadcast()
		ot.mu.Unlock()
		return err
	}
	ackedBytes := ot.ackedBytes
	ot.cond.Broadcast()
	ot.mu.Unlock()
	if !matched {
		if ack.ReceivedBytes <= ackedBytes {
			return nil
		}
		return errors.New("ack does not match pending chunks")
	}
	return nil
}

func (s *Session) applyNack(nack Nack) error {
	ot := s.acquireOutgoingTransfer(nack.TransferID)
	if ot == nil {
		return nil
	}
	defer ot.refs.Done()
	ot.mu.Lock()
	if nack.ReasonCode != NackMissingChunk && nack.ReasonCode != NackFlowControl {
		err := fmt.Errorf("remote rejected transfer=%d reason=%d", nack.TransferID, nack.ReasonCode)
		ot.failed = err
		ot.cond.Broadcast()
		ot.mu.Unlock()
		return nil
	}
	if nack.ReasonCode == NackFlowControl {
		ot.hasRemoteWindow = true
		ot.remoteWindowBytes = 0
		ot.remoteWindowChunks = 0
		ot.mu.Unlock()
		return nil
	}
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
			frame := chunk.frame
			frame.Payload = append([]byte(nil), frame.Payload...)
			frames = append(frames, frame)
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
	for ot.monitorRefs > 0 {
		ot.cond.Wait()
	}
	pending := ot.pending
	ot.pending = ot.pending[:0]
	ot.pendingBytes = 0
	ot.mu.Unlock()
	for i := range pending {
		putChunkBuffer(pending[i].pooledPayload)
	}
}

func (s *Session) cancelOutgoing(transferID uint64) {
	ot := s.acquireOutgoingTransfer(transferID)
	if ot == nil {
		return
	}
	defer ot.refs.Done()
	ot.mu.Lock()
	ot.cond.Broadcast()
	ot.mu.Unlock()
}

func (s *Session) failOutgoing(err error) {
	if err == nil {
		err = ErrSessionClosed
	}
	s.mu.Lock()
	transfers := make([]*outgoingTransfer, 0, len(s.outgoing))
	for _, transfer := range s.outgoing {
		transfer.refs.Add(1)
		transfers = append(transfers, transfer)
	}
	s.mu.Unlock()
	for _, transfer := range transfers {
		transfer.fail(err)
		transfer.refs.Done()
	}
}

func (s *Session) closeWriter() {
	if s.writer != nil {
		s.writer.close()
	}
}

func (s *Session) handleWriterFailure(err error) {
	state := s.State()
	if state == SessionClosing || state == SessionClosed || state == SessionAuthRejected {
		return
	}
	s.setState(SessionFailed)
	s.failOutgoing(err)
	_ = s.t.Close()
	s.emitEvent(ProtocolEvent{Code: EventWriteFailed, Message: err.Error()})
}

func (s *Session) startHandlerWorkers(ctx context.Context) {
	s.handlerOnce.Do(func() {
		for i := 0; i < s.config.Handlers.Workers; i++ {
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case job := <-s.handlerJobs:
						err := callSafely("application handler", job.run)
						if err != nil {
							s.emitEvent(ProtocolEvent{Code: EventHandlerFailed, Message: err.Error(), FrameType: job.frame.Header.FrameType, TransferID: job.frame.Header.TransferID})
							_ = s.SendError(ErrorInternal, job.frame, "application handler failed")
						}
						job.frame.Release()
					}
				}
			}()
		}
	})
}

func (s *Session) invokeApplicationHandler(frame Frame, run func() error) error {
	if !s.running.Load() {
		return callSafely("application handler", run)
	}
	frame.Retain()
	select {
	case s.handlerJobs <- handlerJob{frame: frame, run: run}:
		return nil
	default:
		frame.Release()
		_ = s.SendError(ErrorRateLimited, frame, "application handler queue is full")
		return nil
	}
}

func callSafely(scope string, call func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%s panic: %v", scope, recovered)
		}
	}()
	return call()
}

func callAuthHandlerSafely(handler AuthHandler, ctx context.Context, request AuthRequest) (result AuthResult, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("auth handler panic: %v", recovered)
		}
	}()
	return handler(ctx, request)
}

func (s *Session) remoteLimits() Hello {
	if limits := s.remoteHello.Load(); limits != nil {
		return *limits
	}
	return Hello{}
}

func (s *Session) remoteMaxFrameBytes() uint32 {
	return s.remoteFrame.Load()
}

func (s *Session) checkRemoteFrameSize(size int) error {
	if size < HeaderSize || size > MaxFrameBytes {
		return fmt.Errorf("frame exceeds local max size: %d", size)
	}
	if max := s.remoteMaxFrameBytes(); max > 0 && uint32(size) > max {
		return fmt.Errorf("frame exceeds remote max size: %d > %d", size, max)
	}
	return nil
}

func (s *Session) applyReceiveFrameLimit() {
	setter, ok := s.t.(FrameLimitTransport)
	if !ok {
		return
	}
	limit := s.config.Payload.PreHandshakeBytes
	if s.State() == SessionEstablished {
		limit = s.config.Payload.MaxFrameBytes
	}
	setter.SetMaxFrameBytes(limit)
}

func checksumFlag(mode ChecksumMode) uint32 {
	if mode == ChecksumTransferSHA256 {
		return TransferFlagChecksumSHA256
	}
	return 0
}

func initialPendingCapacity(maxInFlight int, chunkCount uint32) int {
	if maxInFlight <= 0 || chunkCount == 0 {
		return 0
	}
	capacity := maxInFlight
	if uint64(capacity) > uint64(chunkCount) {
		capacity = int(chunkCount)
	}
	if capacity > 4 {
		capacity = 4
	}
	return capacity
}

func cloneAuthAttributes(in []AuthAttribute) []AuthAttribute {
	if len(in) == 0 {
		return nil
	}
	out := make([]AuthAttribute, len(in))
	copy(out, in)
	return out
}
