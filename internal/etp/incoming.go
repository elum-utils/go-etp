package etp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"
	"time"
)

type incomingTransfer struct {
	meta              TransferBegin
	writer            IncomingTransferWriter
	receivedBytes     uint64
	acceptedBytes     uint64
	firstChunk        uint32
	lastChunk         uint32
	nextChunk         uint32
	acceptedNextChunk uint32
	hash              hash.Hash
	queue             chan incomingChunk
	done              chan struct{}
	endDone           chan struct{}
	failed            error
	accepting         bool
	ending            bool
	flowBlocked       bool
	suspendOnFailure  bool
	ctx               context.Context
	cancel            context.CancelFunc
	closeOnce         sync.Once
	mu                sync.Mutex
}

type incomingChunk struct {
	frame   Frame
	payload []byte
}

var (
	ErrRequestIDRequired = errors.New("request id is required")
	ErrRateLimitFrames   = errors.New("rate limit exceeded: frames")
	ErrRateLimitBytes    = errors.New("rate limit exceeded: bytes")
	ErrTooManyIncoming   = errors.New("too many incoming transfers")
	ErrDuplicateIncoming = errors.New("duplicate incoming transfer id")
	ErrIncomingCanceled  = errors.New("incoming transfer canceled")
	ErrReceiveBufferFull = errors.New("incoming receive buffer full")
)

func (s *Session) newIncomingTransfer(ctx context.Context, meta TransferBegin, writer IncomingTransferWriter, hash hash.Hash) *incomingTransfer {
	queueSize := s.config.FlowControl.MaxInFlightChunks
	if queueSize <= 0 {
		queueSize = 1
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	return &incomingTransfer{
		meta:      meta,
		writer:    writer,
		hash:      hash,
		queue:     make(chan incomingChunk, queueSize),
		done:      make(chan struct{}),
		endDone:   make(chan struct{}),
		accepting: true,
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (s *Session) HandleFrame(ctx context.Context, frame Frame) error {
	if ctx == nil {
		ctx = context.Background()
	}
	now := time.Now()
	s.lastReadNano.Store(now.UnixNano())
	if err := s.checkRateLimit(frame, now); err != nil {
		_ = s.SendError(ErrorRateLimited, frame, err.Error())
		return err
	}
	if err := validateFrameEnvelope(frame); err != nil {
		_ = s.markBadFrame(frame, err.Error())
		_ = s.SendError(ErrorProtocolViolation, frame, err.Error())
		return err
	}
	switch frame.Header.FrameType {
	case FrameAuth:
		if s.config.Role != RoleServer || (s.State() != SessionNew && s.State() != SessionAuthPending) {
			return errors.New("auth request is not allowed in current session state")
		}
		s.setState(SessionAuthPending)
		return s.HandleAuth(ctx, frame)
	case FrameAuthAccept:
		if s.config.Role != RoleClient || s.State() != SessionAuthPending {
			return errors.New("auth accept is not allowed in current session state")
		}
		return s.HandleAuthAccept(frame)
	case FrameAuthReject:
		if s.config.Role != RoleClient || s.State() != SessionAuthPending {
			return errors.New("auth reject is not allowed in current session state")
		}
		return s.HandleAuthReject(frame)
	}

	if err := s.EnsureAuthenticatedFor(frame); err != nil {
		_ = s.RejectAuth(AuthRejectUnauthorized, AuthRejectUnauthorized, "auth required")
		return err
	}
	if err := s.ensureIncomingState(frame.Header.FrameType); err != nil {
		_ = s.SendError(ErrorBadState, frame, err.Error())
		return err
	}
	if capability, name := frameCapability(frame); capability != 0 {
		if err := s.requireCapability(capability, name); err != nil {
			_ = s.SendError(ErrorUnsupportedFeature, frame, err.Error())
			return err
		}
	}

	switch frame.Header.FrameType {
	case FrameHello:
		return s.HandleHello(frame)
	case FrameHelloAck:
		return s.HandleHelloAck(frame)
	case FramePing:
		return s.HandlePing()
	case FramePong:
		s.HandlePong()
		return nil
	case FrameAck:
		return s.HandleAck(frame)
	case FrameNack:
		return s.HandleNack(frame)
	case FrameWindow:
		return s.HandleWindow(frame)
	case FrameData:
		return s.handleIncomingData(ctx, frame)
	case FrameRequest:
		return s.handleIncomingRequest(ctx, frame)
	case FrameResponse:
		return s.handleIncomingResponse(ctx, frame)
	case FrameError:
		return s.handleIncomingError(frame)
	case FrameGoAway:
		return s.handleIncomingGoAway(frame)
	case FrameTransferBegin:
		return s.handleIncomingTransferBegin(ctx, frame)
	case FrameTransferEnd:
		return s.handleIncomingTransferEnd(ctx, frame)
	case FrameTransferResume:
		return s.handleIncomingTransferResume(ctx, frame)
	case FrameTransferState:
		return s.handleIncomingTransferState(frame)
	case FrameCancel:
		return s.handleIncomingCancel(frame)
	case FrameCancelAck:
		return s.handleIncomingCancelAck(frame)
	case FrameClose:
		return s.handleIncomingClose(frame)
	case FrameCloseAck:
		return s.handleIncomingCloseAck(frame)
	default:
		s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: "unknown frame type", FrameType: frame.Header.FrameType})
		_ = s.markBadFrame(frame, "unknown frame type")
		_ = s.SendError(ErrorProtocolViolation, frame, "unknown frame type")
		return fmt.Errorf("unknown frame type: %d", frame.Header.FrameType)
	}
}

func frameCapability(frame Frame) (uint64, string) {
	switch frame.Header.FrameType {
	case FrameAck:
		return CapabilityAck, "ack"
	case FrameNack:
		return CapabilityNack, "nack"
	case FramePing, FramePong:
		return CapabilityHeartbeat, "heartbeat"
	case FrameWindow:
		return CapabilityFlowControl, "flow control"
	case FrameCancel, FrameCancelAck:
		return CapabilityCancel, "cancel"
	case FrameRequest, FrameResponse:
		return CapabilityRequestResponse, "request/response"
	case FrameTransferBegin, FrameTransferEnd, FrameTransferState:
		return CapabilityTransfers, "transfers"
	case FrameTransferResume:
		return CapabilityTransferResume, "transfer resume"
	case FrameData:
		if frame.Header.TransferID != 0 {
			return CapabilityTransfers, "transfers"
		}
	}
	return 0, ""
}

func (s *Session) ensureIncomingState(frameType uint8) error {
	state := s.State()
	switch frameType {
	case FrameHello:
		if s.config.Role == RoleServer && (state == SessionNew || state == SessionAuthAccepted) {
			return nil
		}
	case FrameHelloAck:
		if s.config.Role == RoleClient && state == SessionHelloSent {
			return nil
		}
	case FrameClose:
		return nil
	case FrameCloseAck:
		if state == SessionClosing || state == SessionDraining || state == SessionClosed {
			return nil
		}
	default:
		if state == SessionEstablished {
			return nil
		}
		if state == SessionDraining {
			switch frameType {
			case FrameData, FrameAck, FrameNack, FrameWindow, FrameTransferEnd, FrameTransferState, FrameCancel, FrameCancelAck, FramePing, FramePong, FrameError:
				return nil
			}
		}
	}
	return fmt.Errorf("frame type %d is not allowed in session state %s", frameType, state)
}

func validateFrameEnvelope(frame Frame) error {
	expectedSchema := uint32(0)
	switch frame.Header.FrameType {
	case FrameData:
		if frame.Header.TransferID == 0 {
			expectedSchema = SchemaTextMessage
		}
	case FrameAck:
		expectedSchema = SchemaAck
	case FrameNack:
		expectedSchema = SchemaNack
	case FrameWindow:
		expectedSchema = SchemaWindow
	case FrameCancel, FrameCancelAck:
		expectedSchema = SchemaCancel
	case FrameHello, FrameHelloAck:
		expectedSchema = SchemaHello
	case FrameTransferBegin:
		expectedSchema = SchemaTransferBegin
	case FrameTransferState, FrameTransferResume:
		expectedSchema = SchemaTransferState
	case FrameAuth:
		expectedSchema = SchemaAuth
	case FrameAuthAccept, FrameAuthReject:
		expectedSchema = SchemaAuthResult
	case FrameRequest, FrameResponse:
		expectedSchema = SchemaEvent
	case FrameError:
		expectedSchema = SchemaError
	case FrameGoAway:
		expectedSchema = SchemaGoAway
	case FrameClose, FrameCloseAck:
		expectedSchema = SchemaClose
	}
	if frame.Header.SchemaID != expectedSchema {
		return fmt.Errorf("invalid schema %d for frame type %d", frame.Header.SchemaID, frame.Header.FrameType)
	}
	if frame.Header.TransferID == 0 {
		switch frame.Header.FrameType {
		case FrameTransferBegin, FrameTransferEnd, FrameTransferState, FrameTransferResume, FrameAck, FrameNack, FrameCancel, FrameCancelAck:
			return errors.New("transfer id is required")
		}
	}
	if len(frame.Payload) != 0 {
		return nil
	}
	switch frame.Header.FrameType {
	case FramePing, FramePong, FrameTransferEnd:
		return nil
	case FrameData:
		if frame.Header.TransferID != 0 {
			return errors.New("transfer data payload is empty")
		}
	}
	return nil
}

func (s *Session) Run(ctx context.Context) error {
	if s.t == nil {
		return errors.New("frame transport is nil")
	}
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("session run cannot be started more than once")
	}
	s.running.Store(true)
	defer s.running.Store(false)
	runCtx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		for {
			select {
			case job := <-s.handlerJobs:
				job.frame.Release()
			default:
				return
			}
		}
	}()
	s.StartAuthTimeout(runCtx)
	s.StartHeartbeat(runCtx)
	s.startHandlerWorkers(runCtx)
	readDone := make(chan struct{})
	readWatcherDone := make(chan struct{})
	go func() {
		defer close(readWatcherDone)
		select {
		case <-runCtx.Done():
			_ = s.t.Close()
		case <-readDone:
		}
	}()
	defer func() {
		close(readDone)
		<-readWatcherDone
	}()
	for {
		frame, err := Read(s.t)
		if err != nil {
			state := s.State()
			if state == SessionClosed || state == SessionClosing {
				return nil
			}
			if errors.Is(err, ErrSlowloris) {
				s.emitEvent(ProtocolEvent{Code: EventSlowloris, Message: err.Error()})
			} else if errors.Is(err, ErrInvalidFrame) {
				s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: err.Error()})
			}
			s.cleanupIncoming("connection lost", true)
			s.failOutgoing(err)
			s.closeWriter()
			if s.State() != SessionClosed && s.State() != SessionAuthRejected {
				s.setState(SessionFailed)
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if s.State() == SessionClosed {
			frame.Release()
			return nil
		}
		err = s.HandleFrame(runCtx, frame)
		frame.Release()
		if err != nil {
			s.cleanupIncoming("protocol handling failed", false)
			s.failOutgoing(err)
			s.closeWriter()
			if s.State() != SessionAuthRejected {
				s.setState(SessionFailed)
			}
			_ = s.t.Close()
			return err
		}
		if s.State() == SessionClosed {
			return nil
		}
	}
}

func (s *Session) checkRateLimit(frame Frame, now time.Time) error {
	s.rate.mu.Lock()
	if s.rate.windowStart.IsZero() || now.Sub(s.rate.windowStart) >= time.Second {
		s.rate.windowStart = now
		s.rate.frames = 0
		s.rate.bytes = 0
	}
	s.rate.frames++
	s.rate.bytes += uint64(HeaderSize + len(frame.Payload))
	if s.config.RateLimit.MaxFramesPerSecond > 0 && s.rate.frames > s.config.RateLimit.MaxFramesPerSecond {
		s.rate.mu.Unlock()
		s.emitEvent(ProtocolEvent{Code: EventRateLimited, Message: "rate limit exceeded: frames", FrameType: frame.Header.FrameType})
		return ErrRateLimitFrames
	}
	if s.config.RateLimit.MaxBytesPerSecond > 0 && s.rate.bytes > s.config.RateLimit.MaxBytesPerSecond {
		s.rate.mu.Unlock()
		s.emitEvent(ProtocolEvent{Code: EventRateLimited, Message: "rate limit exceeded: bytes", FrameType: frame.Header.FrameType})
		return ErrRateLimitBytes
	}
	s.rate.mu.Unlock()
	return nil
}

func (s *Session) markBadFrame(frame Frame, message string) error {
	s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: message, FrameType: frame.Header.FrameType})
	return nil
}

func (s *Session) HandleHello(frame Frame) error {
	hello, err := DecodeHelloMessage(frame.Payload)
	if err != nil {
		s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: "invalid hello", FrameType: frame.Header.FrameType})
		_ = s.markBadFrame(frame, "invalid hello")
		return err
	}
	if err := s.acceptRemoteHello(hello, RoleClient); err != nil {
		return err
	}
	return s.SendHelloAck(s.config.Role)
}

func (s *Session) HandleHelloAck(frame Frame) error {
	hello, err := DecodeHelloMessage(frame.Payload)
	if err != nil {
		s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: "invalid hello ack", FrameType: frame.Header.FrameType})
		_ = s.markBadFrame(frame, "invalid hello ack")
		return err
	}
	if err := s.acceptRemoteHello(hello, RoleServer); err != nil {
		return err
	}
	return s.established()
}

func (s *Session) acceptRemoteHello(hello Hello, expectedRole string) error {
	if hello.Role != expectedRole {
		return fmt.Errorf("unexpected remote role %q", hello.Role)
	}
	if hello.Capabilities&^uint64(AllCapabilities) != 0 {
		return fmt.Errorf("remote advertised unknown capabilities: %#x", hello.Capabilities)
	}
	if hello.MaxFrameBytes < HeaderSize || hello.MaxFrameBytes > MaxFrameBytes || hello.MaxChunkSize == 0 || hello.MaxChunkSize > hello.MaxFrameBytes-HeaderSize || hello.MaxTransferBytes == 0 || hello.MaxInFlightChunks == 0 || hello.HeartbeatMillis == 0 {
		return errors.New("remote hello contains invalid limits")
	}
	limits := new(Hello)
	*limits = hello
	s.remoteHello.Store(limits)
	s.remoteFrame.Store(hello.MaxFrameBytes)
	s.remoteCaps.Store(hello.Capabilities & s.config.Capabilities)
	return nil
}

func (s *Session) handleIncomingData(ctx context.Context, frame Frame) error {
	if frame.Header.SchemaID == SchemaTextMessage {
		if len(frame.Payload) > int(s.config.Payload.MaxTextBytes) {
			_ = s.markBadFrame(frame, "text payload too large")
			_ = s.SendError(ErrorFrameTooLarge, frame, "text payload too large")
			return fmt.Errorf("text payload exceeds max size: %d > %d", len(frame.Payload), s.config.Payload.MaxTextBytes)
		}
		text, err := DecodeTextMessage(frame.Payload)
		if err != nil {
			s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: "invalid text payload", FrameType: frame.Header.FrameType})
			_ = s.markBadFrame(frame, "invalid text payload")
			return err
		}
		if s.config.Receive.TextHandler != nil {
			return s.invokeApplicationHandler(frame, func() error {
				return s.config.Receive.TextHandler(ctx, frame, text)
			})
		}
		return nil
	}
	return s.handleIncomingTransferData(frame)
}

func (s *Session) handleIncomingRequest(ctx context.Context, frame Frame) error {
	if frame.Header.RequestID == 0 {
		_ = s.markBadFrame(frame, "request id is required")
		_ = s.SendError(ErrorInvalidRequest, frame, "request id is required")
		return ErrRequestIDRequired
	}
	if len(frame.Payload) > int(s.config.Payload.MaxRequestBytes) {
		_ = s.markBadFrame(frame, "request payload too large")
		_ = s.SendError(ErrorFrameTooLarge, frame, "request payload too large")
		return fmt.Errorf("request payload exceeds max size: %d > %d", len(frame.Payload), s.config.Payload.MaxRequestBytes)
	}
	if frame.Header.SchemaID != SchemaEvent {
		s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: "invalid request schema", FrameType: frame.Header.FrameType})
		_ = s.markBadFrame(frame, "invalid request schema")
		_ = s.SendError(ErrorInvalidRequest, frame, "invalid request schema")
		return fmt.Errorf("invalid request schema: %d", frame.Header.SchemaID)
	}
	message, err := DecodeEventMessageView(frame.Payload)
	if err != nil {
		s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: "invalid request payload", FrameType: frame.Header.FrameType})
		_ = s.markBadFrame(frame, "invalid request payload")
		_ = s.SendError(ErrorInvalidRequest, frame, "invalid request payload")
		return err
	}
	if len(message.Event) == 0 {
		s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: "request event is required", FrameType: frame.Header.FrameType})
		_ = s.markBadFrame(frame, "request event is required")
		_ = s.SendError(ErrorInvalidRequest, frame, "request event is required")
		return errors.New("request event is required")
	}
	if s.config.Receive.RequestHandler != nil {
		return s.invokeApplicationHandler(frame, func() error {
			return s.config.Receive.RequestHandler(ctx, frame, message)
		})
	}
	return nil
}

func (s *Session) handleIncomingResponse(ctx context.Context, frame Frame) error {
	if frame.Header.RequestID == 0 {
		s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: "response request id is required", FrameType: frame.Header.FrameType})
		_ = s.markBadFrame(frame, "response request id is required")
		_ = s.SendError(ErrorInvalidRequest, frame, "response request id is required")
		return errors.New("response request id is required")
	}
	if len(frame.Payload) > int(s.config.Payload.MaxResponseBytes) {
		_ = s.markBadFrame(frame, "response payload too large")
		_ = s.SendError(ErrorFrameTooLarge, frame, "response payload too large")
		return fmt.Errorf("response payload exceeds max size: %d > %d", len(frame.Payload), s.config.Payload.MaxResponseBytes)
	}
	if frame.Header.SchemaID != SchemaEvent {
		s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: "invalid response schema", FrameType: frame.Header.FrameType})
		_ = s.markBadFrame(frame, "invalid response schema")
		_ = s.SendError(ErrorInvalidRequest, frame, "invalid response schema")
		return fmt.Errorf("invalid response schema: %d", frame.Header.SchemaID)
	}
	message, err := DecodeEventMessageView(frame.Payload)
	if err != nil {
		s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: "invalid response payload", FrameType: frame.Header.FrameType})
		_ = s.markBadFrame(frame, "invalid response payload")
		_ = s.SendError(ErrorInvalidRequest, frame, "invalid response payload")
		return err
	}
	if len(message.Event) == 0 {
		s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: "response event is required", FrameType: frame.Header.FrameType})
		_ = s.markBadFrame(frame, "response event is required")
		_ = s.SendError(ErrorInvalidRequest, frame, "response event is required")
		return errors.New("response event is required")
	}
	if s.config.Receive.ResponseHandler != nil {
		return s.invokeApplicationHandler(frame, func() error {
			return s.config.Receive.ResponseHandler(ctx, frame, message)
		})
	}
	return nil
}

func (s *Session) handleIncomingError(frame Frame) error {
	msg, err := DecodeErrorMessageView(frame.Payload)
	if err != nil {
		return err
	}
	s.emitEvent(ProtocolEvent{
		Code:       EventErrorReceived,
		Message:    string(msg.Message),
		FrameType:  msg.FrameType,
		TransferID: msg.TransferID,
	})
	if msg.Code == ErrorProtocolViolation || msg.Code == ErrorUnauthorized {
		s.setState(SessionFailed)
		return fmt.Errorf("fatal remote protocol error: code=%d message=%s", msg.Code, msg.Message)
	}
	return nil
}

func (s *Session) handleIncomingGoAway(frame Frame) error {
	msg, err := DecodeGoAwayView(frame.Payload)
	if err != nil {
		return err
	}
	if msg.Flags&CloseFlagDrain != 0 {
		if err := s.requireCapability(CapabilityGracefulClose, "graceful close"); err != nil {
			_ = s.SendError(ErrorUnsupportedFeature, frame, err.Error())
			return err
		}
	}
	s.emitEvent(ProtocolEvent{Code: EventGoAwayReceived, Message: string(msg.Message), FrameType: frame.Header.FrameType})
	if msg.Flags&CloseFlagDrain != 0 {
		s.setState(SessionDraining)
		return nil
	}
	s.cleanupIncoming("goaway received", false)
	s.setState(SessionClosing)
	return nil
}

func (s *Session) handleIncomingClose(frame Frame) error {
	closeMessage, err := DecodeCloseMessage(frame.Payload)
	if err != nil {
		return err
	}
	if closeMessage.Flags&CloseFlagDrain != 0 {
		if err := s.requireCapability(CapabilityGracefulClose, "graceful close"); err != nil {
			_ = s.SendError(ErrorUnsupportedFeature, frame, err.Error())
			return err
		}
	}
	if !s.peerCloseSeen.CompareAndSwap(false, true) {
		return nil
	}
	if closeMessage.Flags&CloseFlagDrain != 0 {
		closeCollision := s.beginPeerClose(SessionDraining)
		timeout := time.Duration(closeMessage.DrainTimeoutMillis) * time.Millisecond
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		go s.finishPeerDrain(closeMessage, timeout, closeCollision)
		return nil
	} else {
		closeCollision := s.beginPeerClose(SessionClosing)
		s.cleanupIncoming("peer closed session", false)
		s.failOutgoing(ErrSessionClosed)
		go s.finishPeerClose(closeMessage, closeCollision)
		return nil
	}
}

func (s *Session) beginPeerClose(target SessionState) bool {
	for {
		state := s.State()
		switch state {
		case SessionClosing:
			return true
		case SessionDraining:
			if target == SessionDraining {
				return true
			}
			if s.state.CompareAndSwap(uint32(state), uint32(target)) {
				return true
			}
			continue
		case SessionClosed:
			return false
		}
		if s.state.CompareAndSwap(uint32(state), uint32(target)) {
			return false
		}
	}
}

func (s *Session) finishPeerClose(closeMessage CloseMessage, closeCollision bool) {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.SendQueue.EnqueueTimeout+s.config.SendQueue.WriteTimeout)
	err := s.sendCloseAckAndWait(ctx, closeMessage)
	cancel()
	s.peerCloseDone <- err
	if err != nil {
		s.setState(SessionFailed)
		s.emitEvent(ProtocolEvent{Code: EventWriteFailed, Message: fmt.Sprintf("send close ack: %v", err), FrameType: FrameCloseAck})
		_ = s.t.Close()
		return
	}
	if closeCollision {
		return
	}
	if s.writer != nil {
		s.writer.close()
	}
	s.setState(SessionClosed)
	_ = s.t.Close()
}

func (s *Session) finishPeerDrain(closeMessage CloseMessage, timeout time.Duration, closeCollision bool) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	err := s.waitTransfersDrained(ctx)
	cancel()
	if err != nil {
		closeMessage.ReasonCode = CloseDrainTimeout
	}
	sendCtx, sendCancel := context.WithTimeout(context.Background(), s.config.SendQueue.EnqueueTimeout+s.config.SendQueue.WriteTimeout)
	sendErr := s.sendCloseAckAndWait(sendCtx, closeMessage)
	sendCancel()
	s.peerCloseDone <- sendErr
	if sendErr != nil {
		s.setState(SessionFailed)
		s.emitEvent(ProtocolEvent{Code: EventWriteFailed, Message: fmt.Sprintf("send drain close ack: %v", sendErr), FrameType: FrameCloseAck})
		_ = s.t.Close()
		return
	}
	if closeCollision {
		return
	}
	if s.writer != nil {
		s.writer.close()
	}
	s.cleanupIncoming("peer drain completed", false)
	s.failOutgoing(ErrSessionClosed)
	s.setState(SessionClosed)
	_ = s.t.Close()
}

func (s *Session) handleIncomingCloseAck(frame Frame) error {
	closeMessage, err := DecodeCloseMessage(frame.Payload)
	if err != nil {
		return err
	}
	s.emitEvent(ProtocolEvent{Code: EventCloseAckReceived, Message: fmt.Sprintf("close ack reason=%d", closeMessage.ReasonCode), FrameType: frame.Header.FrameType})
	select {
	case s.closeAck <- closeMessage:
	default:
	}
	return nil
}

func (s *Session) handleIncomingTransferBegin(ctx context.Context, frame Frame) error {
	if frame.Header.TransferID == 0 {
		return errors.New("incoming transfer id is required")
	}
	if s.config.Capabilities&CapabilityTransferCommit == 0 || !s.RemoteSupports(CapabilityTransferCommit) {
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
		return errors.New("transfer commit was not negotiated")
	}
	meta, err := DecodeTransferBegin(frame.Payload)
	if err != nil {
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
		return err
	}
	if meta.TotalSize > s.config.FlowControl.MaxTransferBytes {
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
		return fmt.Errorf("incoming transfer exceeds max size: %d > %d", meta.TotalSize, s.config.FlowControl.MaxTransferBytes)
	}
	if meta.ChunkSize == 0 || meta.ChunkSize > uint32(s.config.FlowControl.MaxChunkSize) {
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
		return fmt.Errorf("invalid incoming chunk size: %d", meta.ChunkSize)
	}
	expectedChunks := chunkCountFor(meta.TotalSize, meta.ChunkSize)
	if meta.ChunkCount != expectedChunks {
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
		return fmt.Errorf("invalid incoming chunk count: %d != %d", meta.ChunkCount, expectedChunks)
	}
	if meta.Flags & ^uint32(TransferFlagChecksumSHA256) != 0 {
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
		return fmt.Errorf("unsupported incoming transfer flags: %#x", meta.Flags)
	}
	if meta.Flags&TransferFlagChecksumSHA256 != 0 && !s.RemoteSupports(CapabilityTransferSHA256) {
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
		return errors.New("incoming checksum was not negotiated")
	}
	if len(meta.Parts) > 0 {
		var partsSize uint64
		for _, part := range meta.Parts {
			if part.TotalSize > meta.TotalSize-partsSize {
				_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
				return errors.New("transfer part sizes exceed total size")
			}
			partsSize += part.TotalSize
		}
		if partsSize != meta.TotalSize {
			_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
			return errors.New("transfer part sizes do not match total size")
		}
	}
	if (frame.Header.RequestID == 0) != (meta.Event == "") {
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
		return errors.New("transfer event and request id must be set together")
	}
	if s.config.Receive.TransferHandler == nil {
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
		return errors.New("incoming transfer handler is not configured")
	}

	s.mu.Lock()
	lastIncoming := s.lastIncoming
	if frame.Header.TransferID <= lastIncoming {
		s.mu.Unlock()
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
		return fmt.Errorf("incoming transfer id is not increasing: %d <= %d", frame.Header.TransferID, lastIncoming)
	}
	if len(s.incoming)+len(s.opening) >= s.config.FlowControl.MaxConcurrentTransfers {
		s.mu.Unlock()
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
		return fmt.Errorf("too many incoming transfers: %d", s.config.FlowControl.MaxConcurrentTransfers)
	}
	if s.incoming[frame.Header.TransferID] != nil {
		s.mu.Unlock()
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
		return fmt.Errorf("duplicate incoming transfer id: %d", frame.Header.TransferID)
	}
	if _, exists := s.opening[frame.Header.TransferID]; exists {
		s.mu.Unlock()
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
		return fmt.Errorf("duplicate incoming transfer id: %d", frame.Header.TransferID)
	}
	s.lastIncoming = frame.Header.TransferID
	s.opening[frame.Header.TransferID] = struct{}{}
	s.mu.Unlock()

	info := IncomingTransferInfo{
		TransferID: frame.Header.TransferID,
		RequestID:  frame.Header.RequestID,
		Meta:       meta,
	}
	if s.running.Load() && s.remoteLimits().MaxInFlightChunks > 0 {
		go func() {
			if err := s.openIncomingTransfer(ctx, info); err != nil {
				s.emitEvent(ProtocolEvent{Code: EventTransferFailed, Message: err.Error(), FrameType: FrameTransferBegin, TransferID: info.TransferID})
			}
		}()
		return nil
	}
	return s.openIncomingTransfer(ctx, info)
}

func (s *Session) openIncomingTransfer(ctx context.Context, info IncomingTransferInfo) error {
	openCtx, cancel := context.WithTimeout(ctx, s.config.FlowControl.TransferOpenTimeout)
	defer cancel()
	type openResult struct {
		writer IncomingTransferWriter
		err    error
	}
	result := make(chan openResult)
	go func() {
		writer, err := callTransferHandlerSafely(s.config.Receive.TransferHandler, openCtx, info)
		select {
		case result <- openResult{writer: writer, err: err}:
		case <-openCtx.Done():
			if writer != nil {
				abortIncomingWriter(writer)
			}
			s.removeOpening(info.TransferID)
		}
	}()

	var opened openResult
	select {
	case opened = <-result:
		if err := openCtx.Err(); err != nil {
			if opened.writer != nil {
				abortIncomingWriter(opened.writer)
			}
			s.removeOpening(info.TransferID)
			_ = s.sendNack(info.TransferID, 0, 0, NackWriteFailed)
			return fmt.Errorf("open incoming transfer: %w", err)
		}
	case <-openCtx.Done():
		_ = s.sendNack(info.TransferID, 0, 0, NackWriteFailed)
		return fmt.Errorf("open incoming transfer: %w", openCtx.Err())
	}
	writer, err := opened.writer, opened.err
	if err != nil {
		s.removeOpening(info.TransferID)
		_ = s.sendNack(info.TransferID, 0, 0, NackWriteFailed)
		return err
	}
	if writer == nil {
		s.removeOpening(info.TransferID)
		_ = s.sendNack(info.TransferID, 0, 0, NackWriteFailed)
		return errors.New("incoming transfer handler returned nil writer")
	}

	var transferHash hash.Hash
	if info.Meta.Flags&TransferFlagChecksumSHA256 != 0 {
		transferHash = sha256.New()
	}
	tr := s.newIncomingTransfer(ctx, info.Meta, writer, transferHash)

	s.mu.Lock()
	delete(s.opening, info.TransferID)
	state := s.State()
	if state != SessionEstablished && state != SessionDraining {
		s.mu.Unlock()
		abortIncomingWriter(writer)
		return fmt.Errorf("session closed while opening transfer: %s", state)
	}
	s.incoming[info.TransferID] = tr
	s.mu.Unlock()
	go s.runIncomingTransfer(info.TransferID, tr)
	if err := s.sendIncomingWindow(info.TransferID); err != nil {
		s.removeIncoming(info.TransferID)
		tr.stop(err)
		return err
	}
	return nil
}

func (s *Session) removeOpening(transferID uint64) {
	s.mu.Lock()
	delete(s.opening, transferID)
	s.mu.Unlock()
}

func (s *Session) handleIncomingTransferData(frame Frame) error {
	if frame.Header.TransferID == 0 || len(frame.Payload) == 0 {
		return s.sendNack(frame.Header.TransferID, frame.Header.ChunkID, frame.Header.ChunkID, NackInvalidChunk)
	}
	tr := s.lookupIncoming(frame.Header.TransferID)
	if tr == nil {
		return s.sendNack(frame.Header.TransferID, frame.Header.ChunkID, frame.Header.ChunkID, NackTransferUnknown)
	}
	tr.mu.Lock()
	if tr.failed != nil {
		err := tr.failed
		tr.mu.Unlock()
		return err
	}
	if !tr.accepting {
		tr.mu.Unlock()
		return s.sendNack(frame.Header.TransferID, frame.Header.ChunkID, frame.Header.ChunkID, NackTransferCanceled)
	}
	if frame.Header.ChunkID < tr.nextChunk {
		firstChunk, lastChunk, receivedBytes := tr.firstChunk, tr.lastChunk, tr.receivedBytes
		tr.mu.Unlock()
		return s.sendAck(frame.Header.TransferID, firstChunk, lastChunk, receivedBytes)
	}
	if frame.Header.ChunkID < tr.acceptedNextChunk {
		tr.mu.Unlock()
		return nil
	}
	if frame.Header.ChunkID > tr.acceptedNextChunk {
		nextChunk := tr.acceptedNextChunk
		tr.mu.Unlock()
		return s.sendNack(frame.Header.TransferID, nextChunk, frame.Header.ChunkID-1, NackMissingChunk)
	}
	if frame.Header.ChunkID >= tr.meta.ChunkCount || uint32(len(frame.Payload)) > tr.meta.ChunkSize {
		tr.mu.Unlock()
		return s.sendNack(frame.Header.TransferID, frame.Header.ChunkID, frame.Header.ChunkID, NackInvalidChunk)
	}
	expectedSize := tr.meta.ChunkSize
	if frame.Header.ChunkID+1 == tr.meta.ChunkCount {
		expectedSize = uint32(tr.meta.TotalSize - uint64(frame.Header.ChunkID)*uint64(tr.meta.ChunkSize))
	}
	if uint32(len(frame.Payload)) != expectedSize {
		tr.mu.Unlock()
		return s.sendNack(frame.Header.TransferID, frame.Header.ChunkID, frame.Header.ChunkID, NackInvalidChunk)
	}
	if !s.reserveReceiveBytes(uint64(len(frame.Payload))) {
		tr.flowBlocked = true
		tr.mu.Unlock()
		return s.sendNack(frame.Header.TransferID, frame.Header.ChunkID, frame.Header.ChunkID, NackFlowControl)
	}
	frame.Retain()
	select {
	case tr.queue <- incomingChunk{frame: frame, payload: frame.Payload}:
		tr.acceptedBytes += uint64(len(frame.Payload))
		tr.acceptedNextChunk++
		tr.mu.Unlock()
		return nil
	default:
		frame.Release()
		s.releaseReceiveBytes(uint64(len(frame.Payload)))
		tr.flowBlocked = true
		tr.mu.Unlock()
		return s.sendNack(frame.Header.TransferID, frame.Header.ChunkID, frame.Header.ChunkID, NackFlowControl)
	}
}

func (s *Session) runIncomingTransfer(transferID uint64, tr *incomingTransfer) {
	defer tr.cancel()
	for chunk := range tr.queue {
		tr.mu.Lock()
		failed := tr.failed != nil
		tr.mu.Unlock()
		if failed {
			s.releaseIncomingChunk(chunk)
			continue
		}
		s.writeIncomingChunk(transferID, tr, chunk)
	}
	tr.mu.Lock()
	err := tr.failed
	suspend := tr.suspendOnFailure
	tr.mu.Unlock()
	if err != nil {
		if suspend {
			suspendIncomingWriter(tr.writer)
		} else {
			abortIncomingWriter(tr.writer)
		}
	}
	close(tr.done)
}

func (s *Session) writeIncomingChunk(transferID uint64, tr *incomingTransfer, chunk incomingChunk) {
	frame := chunk.frame
	n, err := writeIncomingSafely(tr.ctx, tr.writer, chunk.payload)
	if err != nil {
		s.releaseIncomingChunk(chunk)
		s.notifyBlockedWindows(transferID)
		tr.stop(err)
		_ = s.sendNack(frame.Header.TransferID, frame.Header.ChunkID, frame.Header.ChunkID, NackWriteFailed)
		return
	}
	if n != len(chunk.payload) {
		err := io.ErrShortWrite
		s.releaseIncomingChunk(chunk)
		s.notifyBlockedWindows(transferID)
		tr.stop(err)
		_ = s.sendNack(frame.Header.TransferID, frame.Header.ChunkID, frame.Header.ChunkID, NackWriteFailed)
		return
	}
	tr.mu.Lock()
	if tr.failed != nil {
		tr.mu.Unlock()
		s.releaseIncomingChunk(chunk)
		s.notifyBlockedWindows(transferID)
		return
	}
	if tr.hash != nil {
		_, _ = tr.hash.Write(chunk.payload)
	}
	if tr.receivedBytes == 0 {
		tr.firstChunk = frame.Header.ChunkID
	}
	tr.lastChunk = frame.Header.ChunkID
	tr.nextChunk = frame.Header.ChunkID + 1
	tr.receivedBytes += uint64(len(chunk.payload))
	firstChunk, lastChunk, receivedBytes := tr.firstChunk, tr.lastChunk, tr.receivedBytes
	nextChunk, totalSize := tr.nextChunk, tr.meta.TotalSize
	tr.mu.Unlock()
	s.releaseIncomingChunk(chunk)
	if frame.Header.Flags&FlagAckRequest != 0 {
		_ = s.sendAck(frame.Header.TransferID, firstChunk, lastChunk, receivedBytes)
	}
	_ = s.sendIncomingWindow(transferID)
	s.notifyBlockedWindows(transferID)
	s.emitProgress(Progress{
		TransferID:        transferID,
		TotalBytes:        totalSize,
		LocalWrittenBytes: receivedBytes,
		SentChunks:        nextChunk,
		State:             TransferSending,
	})
}

func (tr *incomingTransfer) fail(err error) {
	tr.mu.Lock()
	if tr.failed == nil {
		tr.failed = err
	}
	tr.mu.Unlock()
}

func (tr *incomingTransfer) closeQueue() {
	tr.closeOnce.Do(func() {
		tr.mu.Lock()
		tr.accepting = false
		close(tr.queue)
		tr.mu.Unlock()
	})
}

func (tr *incomingTransfer) stop(err error) {
	tr.fail(err)
	tr.cancel()
	tr.closeQueue()
}

func (tr *incomingTransfer) suspend(err error) {
	tr.mu.Lock()
	tr.suspendOnFailure = true
	if tr.failed == nil {
		tr.failed = err
	}
	tr.mu.Unlock()
	tr.cancel()
	tr.closeQueue()
}

func (s *Session) releaseIncomingChunk(chunk incomingChunk) {
	s.releaseReceiveBytes(uint64(len(chunk.payload)))
	chunk.frame.Release()
}

func (s *Session) reserveReceiveBytes(n uint64) bool {
	limit := s.config.FlowControl.MaxReceiveBufferBytes
	for {
		current := s.receiveBytes.Load()
		if limit > 0 && (current > limit || n > limit-current) {
			return false
		}
		if s.receiveBytes.CompareAndSwap(current, current+n) {
			return true
		}
	}
}

func (s *Session) releaseReceiveBytes(n uint64) {
	for {
		current := s.receiveBytes.Load()
		next := uint64(0)
		if current > n {
			next = current - n
		}
		if s.receiveBytes.CompareAndSwap(current, next) {
			return
		}
	}
}

func (s *Session) sendIncomingWindow(transferID uint64) error {
	if !s.RemoteSupports(CapabilityFlowControl) {
		return nil
	}
	available := uint64(0)
	buffered := s.receiveBytes.Load()
	if buffered < s.config.FlowControl.MaxReceiveBufferBytes {
		available = s.config.FlowControl.MaxReceiveBufferBytes - buffered
	}
	windowBytes := s.config.FlowControl.MaxInFlightBytes
	if windowBytes == 0 || windowBytes > available {
		windowBytes = available
	}
	windowChunks := uint32(s.config.FlowControl.MaxInFlightChunks)
	if tr := s.lookupIncoming(transferID); tr != nil {
		tr.mu.Lock()
		queued := len(tr.queue)
		capacity := cap(tr.queue)
		tr.flowBlocked = windowBytes == 0 || queued == capacity
		tr.mu.Unlock()
		windowChunks = uint32(capacity - queued)
	}
	return s.SendWindow(Window{
		TransferID:   transferID,
		WindowBytes:  windowBytes,
		WindowChunks: windowChunks,
		Flags:        WindowFlagTransfer,
	})
}

func (s *Session) notifyBlockedWindows(excludeTransferID uint64) {
	s.mu.Lock()
	blocked := make([]uint64, 0, len(s.incoming))
	for transferID, tr := range s.incoming {
		if transferID == excludeTransferID {
			continue
		}
		tr.mu.Lock()
		isBlocked := tr.flowBlocked
		tr.mu.Unlock()
		if isBlocked {
			blocked = append(blocked, transferID)
		}
	}
	s.mu.Unlock()
	for _, transferID := range blocked {
		_ = s.sendIncomingWindow(transferID)
	}
}

func (s *Session) handleIncomingTransferEnd(ctx context.Context, frame Frame) error {
	tr := s.lookupIncoming(frame.Header.TransferID)
	if tr == nil {
		if state, ok := s.completedTransfer(frame.Header.TransferID); ok {
			return s.SendTransferState(state)
		}
		return s.sendNack(frame.Header.TransferID, frame.Header.ChunkID, frame.Header.ChunkID, NackTransferUnknown)
	}
	tr.mu.Lock()
	completeInput := tr.acceptedBytes == tr.meta.TotalSize && tr.acceptedNextChunk == tr.meta.ChunkCount
	firstChunk, lastChunk := tr.firstChunk, tr.lastChunk
	if completeInput && tr.ending {
		endDone := tr.endDone
		tr.mu.Unlock()
		if err := waitContextSignal(ctx, endDone); err != nil {
			return err
		}
		if state, ok := s.completedTransfer(frame.Header.TransferID); ok {
			return s.SendTransferState(state)
		}
		return s.sendNack(frame.Header.TransferID, firstChunk, lastChunk, NackTransferUnknown)
	}
	if completeInput {
		tr.ending = true
	}
	tr.mu.Unlock()
	if !completeInput {
		tr.mu.Lock()
		missingFrom := tr.acceptedNextChunk
		missingTo := tr.meta.ChunkCount - 1
		tr.mu.Unlock()
		return s.sendNack(frame.Header.TransferID, missingFrom, missingTo, NackMissingChunk)
	}
	defer close(tr.endDone)
	tr.closeQueue()
	if err := waitIncomingSignal(ctx, tr, tr.done); err != nil {
		tr.stop(err)
		tr.mu.Lock()
		receivedBytes, nextChunk := tr.receivedBytes, tr.nextChunk
		tr.mu.Unlock()
		s.rememberTransferState(TransferStateMessage{TransferID: frame.Header.TransferID, ReceivedBytes: receivedBytes, NextChunk: nextChunk, Flags: TransferStateFlagFailed, ReasonCode: NackWriteFailed})
		go func() {
			<-tr.done
			s.removeIncoming(frame.Header.TransferID)
		}()
		return err
	}
	tr.mu.Lock()
	writeErr := tr.failed
	tr.mu.Unlock()
	if writeErr != nil {
		s.removeIncoming(frame.Header.TransferID)
		s.rememberTransferState(TransferStateMessage{TransferID: frame.Header.TransferID, ReceivedBytes: tr.receivedBytes, NextChunk: tr.nextChunk, Flags: TransferStateFlagFailed, ReasonCode: NackWriteFailed})
		_ = s.sendTransferFailed(frame.Header.TransferID, tr.receivedBytes, tr.nextChunk, NackWriteFailed)
		return s.sendNack(frame.Header.TransferID, tr.firstChunk, tr.lastChunk, NackWriteFailed)
	}

	tr.mu.Lock()
	totalSize, receivedBytes := tr.meta.TotalSize, tr.receivedBytes
	firstChunk, lastChunk, nextChunk := tr.firstChunk, tr.lastChunk, tr.nextChunk
	var checksum []byte
	if tr.hash != nil {
		checksum = tr.hash.Sum(nil)
	}
	expectedChecksum := tr.meta.Checksum
	tr.mu.Unlock()

	if totalSize > 0 && receivedBytes != totalSize {
		s.removeIncoming(frame.Header.TransferID)
		abortIncomingWriter(tr.writer)
		s.rememberTransferState(TransferStateMessage{TransferID: frame.Header.TransferID, ReceivedBytes: receivedBytes, NextChunk: nextChunk, Flags: TransferStateFlagFailed, ReasonCode: NackInvalidChunk})
		_ = s.sendTransferFailed(frame.Header.TransferID, receivedBytes, nextChunk, NackInvalidChunk)
		return s.sendNack(frame.Header.TransferID, firstChunk, lastChunk, NackInvalidChunk)
	}
	if checksum != nil && !bytes.Equal(checksum, expectedChecksum[:]) {
		s.emitEvent(ProtocolEvent{Code: EventChecksumMismatch, Message: "transfer checksum mismatch", FrameType: frame.Header.FrameType, TransferID: frame.Header.TransferID})
		s.removeIncoming(frame.Header.TransferID)
		abortIncomingWriter(tr.writer)
		s.rememberTransferState(TransferStateMessage{TransferID: frame.Header.TransferID, ReceivedBytes: receivedBytes, NextChunk: nextChunk, Flags: TransferStateFlagFailed, ReasonCode: NackInvalidChunk})
		_ = s.sendTransferFailed(frame.Header.TransferID, receivedBytes, nextChunk, NackInvalidChunk)
		return s.sendNack(frame.Header.TransferID, firstChunk, lastChunk, NackInvalidChunk)
	}
	commitCtx, commitCancel := context.WithTimeout(ctx, s.config.FlowControl.TransferCommitTimeout)
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- closeIncomingWriterContext(commitCtx, tr.writer)
	}()
	var closeErr error
	timedOut := false
	select {
	case closeErr = <-closeDone:
	case <-commitCtx.Done():
		select {
		case closeErr = <-closeDone:
		default:
			closeErr = commitCtx.Err()
			timedOut = true
		}
	}
	commitCancel()
	if timedOut {
		tr.fail(closeErr)
		s.rememberTransferState(TransferStateMessage{TransferID: frame.Header.TransferID, ReceivedBytes: receivedBytes, NextChunk: nextChunk, Flags: TransferStateFlagFailed, ReasonCode: NackWriteFailed})
		go func() {
			<-closeDone
			abortIncomingWriter(tr.writer)
			s.removeIncoming(frame.Header.TransferID)
		}()
		_ = s.sendTransferFailed(frame.Header.TransferID, receivedBytes, nextChunk, NackWriteFailed)
		return s.sendNack(frame.Header.TransferID, firstChunk, lastChunk, NackWriteFailed)
	}
	if closeErr != nil {
		s.removeIncoming(frame.Header.TransferID)
		s.rememberTransferState(TransferStateMessage{TransferID: frame.Header.TransferID, ReceivedBytes: receivedBytes, NextChunk: nextChunk, Flags: TransferStateFlagFailed, ReasonCode: NackWriteFailed})
		_ = s.sendTransferFailed(frame.Header.TransferID, receivedBytes, nextChunk, NackWriteFailed)
		return s.sendNack(frame.Header.TransferID, firstChunk, lastChunk, NackWriteFailed)
	}
	s.removeIncoming(frame.Header.TransferID)
	state := TransferStateMessage{
		TransferID:    frame.Header.TransferID,
		ReceivedBytes: receivedBytes,
		NextChunk:     nextChunk,
		Flags:         TransferStateFlagCompleted,
	}
	s.rememberTransferState(state)
	s.emitProgress(Progress{
		TransferID:        frame.Header.TransferID,
		TotalBytes:        totalSize,
		LocalWrittenBytes: receivedBytes,
		SentChunks:        nextChunk,
		State:             TransferCompleted,
	})
	return s.SendTransferState(state)
}

func (s *Session) sendTransferFailed(transferID uint64, receivedBytes uint64, nextChunk uint32, reason uint16) error {
	return s.SendTransferState(TransferStateMessage{
		TransferID:    transferID,
		ReceivedBytes: receivedBytes,
		NextChunk:     nextChunk,
		Flags:         TransferStateFlagFailed,
		ReasonCode:    reason,
	})
}

func (s *Session) handleIncomingTransferResume(ctx context.Context, frame Frame) error {
	resume, err := DecodeTransferResumeView(frame.Payload)
	if err != nil {
		return err
	}
	if resume.TransferID == 0 || resume.TransferID != frame.Header.TransferID {
		return errors.New("transfer resume id does not match frame header")
	}
	if s.config.Capabilities&CapabilityTransferResume == 0 || !s.RemoteSupports(CapabilityTransferResume) {
		return s.SendTransferState(TransferStateMessage{TransferID: resume.TransferID, ReceivedBytes: resume.ReceivedBytes, NextChunk: resume.NextChunk, Flags: TransferStateFlagResumeRejected, ReasonCode: NackProtocolError})
	}
	s.mu.Lock()
	_, opening := s.opening[resume.TransferID]
	if s.incoming[resume.TransferID] != nil || opening {
		s.mu.Unlock()
		return s.SendTransferState(TransferStateMessage{
			TransferID:    resume.TransferID,
			ReceivedBytes: resume.ReceivedBytes,
			NextChunk:     resume.NextChunk,
			Flags:         TransferStateFlagResumeRejected,
			ReasonCode:    NackProtocolError,
		})
	}
	if len(s.incoming)+len(s.opening) >= s.config.FlowControl.MaxConcurrentTransfers {
		s.mu.Unlock()
		return s.SendTransferState(TransferStateMessage{TransferID: resume.TransferID, ReceivedBytes: resume.ReceivedBytes, NextChunk: resume.NextChunk, Flags: TransferStateFlagResumeRejected, ReasonCode: NackProtocolError})
	}
	s.opening[resume.TransferID] = struct{}{}
	s.mu.Unlock()
	openingOwned := true
	defer func() {
		if openingOwned {
			s.removeOpening(resume.TransferID)
		}
	}()
	if s.config.Resume.Store != nil {
		// The decoded token may reference a leased frame buffer. A store that
		// outlives TransferOpenTimeout must retain stable bytes.
		resume.Token = append([]byte(nil), resume.Token...)
		decision, timedOut, err := s.loadResumeDecision(ctx, resume)
		if timedOut {
			openingOwned = false
		}
		if err != nil {
			if decision.Writer != nil {
				abortIncomingWriter(decision.Writer)
			}
			s.emitEvent(ProtocolEvent{Code: EventTransferFailed, Message: err.Error(), FrameType: FrameTransferResume, TransferID: resume.TransferID})
			return s.SendTransferState(TransferStateMessage{TransferID: resume.TransferID, ReceivedBytes: resume.ReceivedBytes, NextChunk: resume.NextChunk, Flags: TransferStateFlagResumeRejected, ReasonCode: NackWriteFailed})
		}
		if decision.Accepted {
			if decision.Writer == nil {
				return s.SendTransferState(TransferStateMessage{
					TransferID:    resume.TransferID,
					ReceivedBytes: resume.ReceivedBytes,
					NextChunk:     resume.NextChunk,
					Flags:         TransferStateFlagResumeRejected,
					ReasonCode:    NackWriteFailed,
				})
			}
			meta := decision.Meta
			if meta.TotalSize > s.config.FlowControl.MaxTransferBytes || meta.ChunkSize == 0 || meta.ChunkSize > uint32(s.config.FlowControl.MaxChunkSize) || meta.ChunkCount != chunkCountFor(meta.TotalSize, meta.ChunkSize) || meta.Flags&^uint32(TransferFlagChecksumSHA256) != 0 || len(meta.Parts) > MaxTransferParts || len(meta.Fields) > MaxTransferFields {
				abortIncomingWriter(decision.Writer)
				return s.SendTransferState(TransferStateMessage{
					TransferID:    resume.TransferID,
					ReceivedBytes: decision.ReceivedBytes,
					NextChunk:     decision.NextChunk,
					Flags:         TransferStateFlagResumeRejected,
					ReasonCode:    NackProtocolError,
				})
			}
			if meta.Flags&TransferFlagChecksumSHA256 != 0 && (s.config.Capabilities&CapabilityTransferSHA256 == 0 || !s.RemoteSupports(CapabilityTransferSHA256)) {
				abortIncomingWriter(decision.Writer)
				return s.SendTransferState(TransferStateMessage{TransferID: resume.TransferID, ReceivedBytes: decision.ReceivedBytes, NextChunk: decision.NextChunk, Flags: TransferStateFlagResumeRejected, ReasonCode: NackProtocolError})
			}
			if len(meta.Parts) > 0 {
				var partsSize uint64
				for _, part := range meta.Parts {
					if part.TotalSize > meta.TotalSize-partsSize {
						abortIncomingWriter(decision.Writer)
						return s.SendTransferState(TransferStateMessage{TransferID: resume.TransferID, ReceivedBytes: decision.ReceivedBytes, NextChunk: decision.NextChunk, Flags: TransferStateFlagResumeRejected, ReasonCode: NackProtocolError})
					}
					partsSize += part.TotalSize
				}
				if partsSize != meta.TotalSize {
					abortIncomingWriter(decision.Writer)
					return s.SendTransferState(TransferStateMessage{TransferID: resume.TransferID, ReceivedBytes: decision.ReceivedBytes, NextChunk: decision.NextChunk, Flags: TransferStateFlagResumeRejected, ReasonCode: NackProtocolError})
				}
			}
			expectedBytes, validOffset := transferOffsetForChunk(meta.TotalSize, meta.ChunkSize, decision.NextChunk)
			if !validOffset || decision.ReceivedBytes != expectedBytes || (frame.Header.RequestID == 0) != (meta.Event == "") {
				abortIncomingWriter(decision.Writer)
				return s.SendTransferState(TransferStateMessage{
					TransferID:    resume.TransferID,
					ReceivedBytes: decision.ReceivedBytes,
					NextChunk:     decision.NextChunk,
					Flags:         TransferStateFlagResumeRejected,
					ReasonCode:    NackProtocolError,
				})
			}
			if decision.Meta.Flags&TransferFlagChecksumSHA256 != 0 && decision.Hash == nil {
				abortIncomingWriter(decision.Writer)
				return s.SendTransferState(TransferStateMessage{
					TransferID:    resume.TransferID,
					ReceivedBytes: resume.ReceivedBytes,
					NextChunk:     resume.NextChunk,
					Flags:         TransferStateFlagResumeRejected,
					ReasonCode:    NackProtocolError,
				})
			}
			tr := s.newIncomingTransfer(ctx, decision.Meta, decision.Writer, decision.Hash)
			tr.receivedBytes = decision.ReceivedBytes
			tr.acceptedBytes = decision.ReceivedBytes
			tr.nextChunk = decision.NextChunk
			tr.acceptedNextChunk = decision.NextChunk
			if decision.NextChunk > 0 {
				tr.firstChunk = 0
				tr.lastChunk = decision.NextChunk - 1
			}
			s.mu.Lock()
			delete(s.opening, resume.TransferID)
			openingOwned = false
			if resume.TransferID > s.lastIncoming {
				s.lastIncoming = resume.TransferID
			}
			s.incoming[resume.TransferID] = tr
			s.mu.Unlock()
			go s.runIncomingTransfer(resume.TransferID, tr)
			if err := s.SendTransferState(TransferStateMessage{
				TransferID:    resume.TransferID,
				ReceivedBytes: decision.ReceivedBytes,
				NextChunk:     decision.NextChunk,
				Flags:         TransferStateFlagResumeAccepted,
			}); err != nil {
				s.removeIncoming(resume.TransferID)
				tr.stop(err)
				return err
			}
			if err := s.sendIncomingWindow(resume.TransferID); err != nil {
				s.removeIncoming(resume.TransferID)
				tr.stop(err)
				return err
			}
			return nil
		}
		reason := decision.ReasonCode
		if decision.Writer != nil {
			abortIncomingWriter(decision.Writer)
		}
		if reason == 0 {
			reason = NackProtocolError
		}
		return s.SendTransferState(TransferStateMessage{
			TransferID:    resume.TransferID,
			ReceivedBytes: decision.ReceivedBytes,
			NextChunk:     decision.NextChunk,
			Flags:         TransferStateFlagResumeRejected,
			ReasonCode:    reason,
		})
	}
	return s.SendTransferState(TransferStateMessage{
		TransferID:    resume.TransferID,
		ReceivedBytes: resume.ReceivedBytes,
		NextChunk:     resume.NextChunk,
		Flags:         TransferStateFlagResumeRejected,
		ReasonCode:    NackProtocolError,
	})
}

func (s *Session) loadResumeDecision(ctx context.Context, resume TransferResumeView) (TransferResumeDecision, bool, error) {
	storeCtx, cancel := context.WithTimeout(ctx, s.config.FlowControl.TransferOpenTimeout)
	defer cancel()
	type storeResult struct {
		decision TransferResumeDecision
		err      error
	}
	result := make(chan storeResult)
	go func() {
		decision, err := callResumeStoreSafely(s.config.Resume.Store, storeCtx, resume)
		select {
		case result <- storeResult{decision: decision, err: err}:
		case <-storeCtx.Done():
			if decision.Writer != nil {
				abortIncomingWriter(decision.Writer)
			}
			s.removeOpening(resume.TransferID)
		}
	}()
	select {
	case opened := <-result:
		if err := storeCtx.Err(); err != nil {
			if opened.decision.Writer != nil {
				abortIncomingWriter(opened.decision.Writer)
			}
			s.removeOpening(resume.TransferID)
			return TransferResumeDecision{}, true, err
		}
		return opened.decision, false, opened.err
	case <-storeCtx.Done():
		return TransferResumeDecision{}, true, storeCtx.Err()
	}
}

func (s *Session) handleIncomingTransferState(frame Frame) error {
	state, err := DecodeTransferStateMessage(frame.Payload)
	if err != nil {
		return err
	}
	if state.TransferID == 0 || state.TransferID != frame.Header.TransferID {
		return errors.New("transfer state id does not match frame header")
	}
	const stateFlags = TransferStateFlagResumeAccepted | TransferStateFlagResumeRejected | TransferStateFlagCompleted | TransferStateFlagFailed
	if state.Flags == 0 || state.Flags&^uint16(stateFlags) != 0 || state.Flags&(state.Flags-1) != 0 {
		return errors.New("invalid transfer state flags")
	}
	if (state.Flags == TransferStateFlagCompleted || state.Flags == TransferStateFlagResumeAccepted) && state.ReasonCode != 0 {
		return errors.New("successful transfer state contains a reason code")
	}
	if (state.Flags == TransferStateFlagFailed || state.Flags == TransferStateFlagResumeRejected) && state.ReasonCode == 0 {
		return errors.New("failed transfer state is missing a reason code")
	}
	ot := s.acquireOutgoingTransfer(state.TransferID)
	if ot != nil {
		defer ot.refs.Done()
	}
	if state.Flags&TransferStateFlagCompleted != 0 {
		if ot == nil {
			return nil
		}
		ot.mu.Lock()
		if state.ReceivedBytes != ot.totalBytes || state.NextChunk != ot.chunkCount {
			err := errors.New("invalid transfer completion counters")
			ot.failed = err
			ot.cond.Broadcast()
			ot.mu.Unlock()
			return err
		}
		ot.completed = true
		ot.cond.Broadcast()
		ot.mu.Unlock()
		return nil
	}
	if state.Flags&TransferStateFlagFailed != 0 {
		err := fmt.Errorf("remote transfer commit failed: reason=%d", state.ReasonCode)
		if ot != nil {
			ot.fail(err)
		}
		s.emitEvent(ProtocolEvent{Code: EventTransferFailed, Message: err.Error(), FrameType: frame.Header.FrameType, TransferID: state.TransferID})
		return nil
	}
	if state.Flags&TransferStateFlagResumeRejected != 0 {
		if ot != nil {
			ot.fail(fmt.Errorf("remote rejected transfer resume: reason=%d", state.ReasonCode))
		}
		return nil
	}
	if state.Flags&TransferStateFlagResumeAccepted != 0 {
		if ot == nil {
			return nil
		}
		expectedBytes, ok := transferOffsetForChunk(ot.totalBytes, ot.chunkSize, state.NextChunk)
		if !ok || state.ReceivedBytes != expectedBytes {
			err := errors.New("invalid transfer resume counters")
			ot.fail(err)
			return err
		}
		ot.mu.Lock()
		ot.ackedBytes = state.ReceivedBytes
		ot.resumeBytes = state.ReceivedBytes
		ot.resumeChunk = state.NextChunk
		ot.resumeAccepted = true
		ot.resumeDecided = true
		ot.cond.Broadcast()
		ot.mu.Unlock()
		return nil
	}
	return errors.New("invalid transfer state flags")
}

func (s *Session) handleIncomingCancel(frame Frame) error {
	cancel, err := DecodeCancel(frame.Payload)
	if err != nil {
		return err
	}
	if cancel.TransferID != frame.Header.TransferID {
		return errors.New("cancel transfer id does not match frame header")
	}
	status := CancelAckNotFound
	if tr := s.lookupIncoming(cancel.TransferID); tr != nil {
		s.removeIncoming(cancel.TransferID)
		tr.stop(ErrIncomingCanceled)
		status = CancelAckOK
	}
	ack := NewFrame(FrameCancelAck, SchemaCancel, EncodeCancelAck(cancel.TransferID, status))
	ack.Header.Priority = PriorityCritical
	ack.Header.ChannelID = ChannelControl
	ack.Header.TransferID = cancel.TransferID
	ack.Header.Flags = FlagControl
	return s.send(ack)
}

func (s *Session) handleIncomingCancelAck(frame Frame) error {
	transferID, status, err := DecodeCancelAck(frame.Payload)
	if err != nil {
		return err
	}
	if transferID != frame.Header.TransferID {
		return errors.New("cancel ack transfer id does not match frame header")
	}
	if status < CancelAckOK || status > CancelAckCompleted {
		return fmt.Errorf("invalid cancel ack status: %d", status)
	}
	return nil
}

func (s *Session) sendAck(transferID uint64, chunkFrom uint32, chunkTo uint32, receivedBytes uint64) error {
	ack := NewFrame(FrameAck, SchemaAck, EncodeAck(Ack{
		TransferID:    transferID,
		ChunkFrom:     chunkFrom,
		ChunkTo:       chunkTo,
		ReceivedBytes: receivedBytes,
	}))
	ack.Header.Priority = PriorityHigh
	ack.Header.ChannelID = ChannelControl
	ack.Header.TransferID = transferID
	ack.Header.Flags = FlagControl
	return s.send(ack)
}

func (s *Session) sendNack(transferID uint64, chunkFrom uint32, chunkTo uint32, reason uint16) error {
	nack := NewFrame(FrameNack, SchemaNack, EncodeNack(Nack{
		TransferID: transferID,
		ChunkFrom:  chunkFrom,
		ChunkTo:    chunkTo,
		ReasonCode: reason,
	}))
	nack.Header.Priority = PriorityHigh
	nack.Header.ChannelID = ChannelControl
	nack.Header.TransferID = transferID
	nack.Header.Flags = FlagControl
	return s.send(nack)
}

func (s *Session) lookupIncoming(transferID uint64) *incomingTransfer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.incoming[transferID]
}

func (s *Session) removeIncoming(transferID uint64) {
	s.mu.Lock()
	delete(s.incoming, transferID)
	s.mu.Unlock()
}

func (s *Session) rememberTransferState(state TransferStateMessage) {
	now := time.Now()
	s.mu.Lock()
	if _, exists := s.completed[state.TransferID]; !exists {
		s.removeCompletedIDLocked(state.TransferID)
		s.completedIDs = append(s.completedIDs, state.TransferID)
	}
	s.completed[state.TransferID] = completedTransfer{state: state, expiresAt: now.Add(s.config.FlowControl.CompletedTransferTTL)}
	for len(s.completedIDs) > s.config.FlowControl.MaxCompletedTransfers {
		oldest := s.completedIDs[0]
		s.completedIDs = s.completedIDs[1:]
		delete(s.completed, oldest)
	}
	s.mu.Unlock()
}

func (s *Session) completedTransfer(transferID uint64) (TransferStateMessage, bool) {
	now := time.Now()
	s.mu.Lock()
	completed, ok := s.completed[transferID]
	if ok && now.After(completed.expiresAt) {
		delete(s.completed, transferID)
		s.removeCompletedIDLocked(transferID)
		ok = false
	}
	s.mu.Unlock()
	return completed.state, ok
}

func (s *Session) removeCompletedIDLocked(transferID uint64) {
	ids := s.completedIDs[:0]
	for _, id := range s.completedIDs {
		if id != transferID {
			ids = append(ids, id)
		}
	}
	s.completedIDs = ids
}

func (s *Session) cleanupIncoming(reason string, preserveForResume bool) {
	s.mu.Lock()
	transfers := s.incoming
	s.incoming = map[uint64]*incomingTransfer{}
	s.mu.Unlock()
	for transferID, tr := range transfers {
		if preserveForResume {
			tr.suspend(errors.New(reason))
		} else {
			tr.stop(errors.New(reason))
		}
		s.emitEvent(ProtocolEvent{Code: EventTransferFailed, Message: reason, TransferID: transferID})
	}
}

func suspendIncomingWriter(writer IncomingTransferWriter) {
	_ = callSafely("incoming transfer suspend", func() error {
		if suspender, ok := writer.(IncomingTransferSuspender); ok {
			return suspender.Suspend()
		}
		abortIncomingWriter(writer)
		return nil
	})
}

func abortIncomingWriter(writer IncomingTransferWriter) {
	_ = callSafely("incoming transfer abort", func() error {
		if aborter, ok := writer.(IncomingTransferAborter); ok {
			return aborter.Abort()
		}
		return writer.Close()
	})
}

func closeIncomingWriter(writer IncomingTransferWriter) error {
	return callSafely("incoming transfer close", writer.Close)
}

func closeIncomingWriterContext(ctx context.Context, writer IncomingTransferWriter) error {
	if closer, ok := writer.(IncomingTransferContextCloser); ok {
		return callSafely("incoming transfer close", func() error {
			return closer.CloseContext(ctx)
		})
	}
	return closeIncomingWriter(writer)
}

func waitIncomingSignal(ctx context.Context, tr *incomingTransfer, signal <-chan struct{}) error {
	select {
	case <-signal:
		return nil
	default:
	}
	select {
	case <-signal:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-tr.ctx.Done():
		select {
		case <-signal:
			return nil
		default:
			return tr.ctx.Err()
		}
	}
}

func waitContextSignal(ctx context.Context, signal <-chan struct{}) error {
	select {
	case <-signal:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func writeIncomingSafely(ctx context.Context, writer IncomingTransferWriter, payload []byte) (n int, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("incoming transfer writer panic: %v", recovered)
		}
	}()
	if contextWriter, ok := writer.(IncomingTransferContextWriter); ok {
		return contextWriter.WriteContext(ctx, payload)
	}
	return writer.Write(payload)
}

func callTransferHandlerSafely(handler TransferHandler, ctx context.Context, info IncomingTransferInfo) (writer IncomingTransferWriter, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("incoming transfer handler panic: %v", recovered)
		}
	}()
	return handler(ctx, info)
}

func callResumeStoreSafely(store TransferResumeStore, ctx context.Context, resume TransferResumeView) (decision TransferResumeDecision, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("transfer resume store panic: %v", recovered)
		}
	}()
	return store.ResumeIncoming(ctx, resume)
}

func chunkCountFor(totalSize uint64, chunkSize uint32) uint32 {
	if totalSize == 0 {
		return 0
	}
	chunks := 1 + (totalSize-1)/uint64(chunkSize)
	if chunks > uint64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(chunks)
}

func transferOffsetForChunk(totalSize uint64, chunkSize uint32, nextChunk uint32) (uint64, bool) {
	if chunkSize == 0 {
		return 0, false
	}
	chunkCount := chunkCountFor(totalSize, chunkSize)
	if nextChunk > chunkCount {
		return 0, false
	}
	if nextChunk == chunkCount {
		return totalSize, true
	}
	offset := uint64(nextChunk) * uint64(chunkSize)
	if offset >= totalSize {
		return 0, false
	}
	return offset, true
}

type discardIncomingWriter struct{}

func (discardIncomingWriter) Write(p []byte) (int, error) { return len(p), nil }
func (discardIncomingWriter) Close() error                { return nil }

var _ IncomingTransferWriter = discardIncomingWriter{}
var _ io.Writer = discardIncomingWriter{}
