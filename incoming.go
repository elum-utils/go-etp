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
	meta          TransferBegin
	writer        IncomingTransferWriter
	receivedBytes uint64
	firstChunk    uint32
	lastChunk     uint32
	nextChunk     uint32
	hash          hash.Hash
	queue         chan incomingChunk
	done          chan error
	failed        error
	closeOnce     sync.Once
	mu            sync.Mutex
}

type incomingChunk struct {
	frame   Frame
	payload []byte
}

var (
	ErrRequestIDRequired  = errors.New("request id is required")
	ErrRateLimitFrames    = errors.New("rate limit exceeded: frames")
	ErrRateLimitBytes     = errors.New("rate limit exceeded: bytes")
	ErrRateLimitAuth      = errors.New("rate limit exceeded: auth attempts")
	ErrRateLimitBadFrames = errors.New("rate limit exceeded: bad frames")
	ErrTooManyIncoming    = errors.New("too many incoming transfers")
	ErrDuplicateIncoming  = errors.New("duplicate incoming transfer id")
)

func (s *Session) newIncomingTransfer(meta TransferBegin, writer IncomingTransferWriter, hash hash.Hash) *incomingTransfer {
	queueSize := s.config.FlowControl.MaxInFlightChunks
	if queueSize <= 0 {
		queueSize = 1
	}
	return &incomingTransfer{
		meta:   meta,
		writer: writer,
		hash:   hash,
		queue:  make(chan incomingChunk, queueSize),
		done:   make(chan error, 1),
	}
}

func (s *Session) HandleFrame(ctx context.Context, frame Frame) error {
	s.handleMu.Lock()
	defer s.handleMu.Unlock()
	s.markRead()
	if err := s.checkRateLimit(frame); err != nil {
		_ = s.SendError(ErrorRateLimited, frame, err.Error())
		return err
	}
	switch frame.Header.FrameType {
	case FrameAuth:
		return s.HandleAuth(ctx, frame)
	case FrameAuthAccept:
		return s.HandleAuthAccept(frame)
	case FrameAuthReject:
		return s.HandleAuthReject(frame)
	}

	if err := s.EnsureAuthenticatedFor(frame); err != nil {
		_ = s.RejectAuth(AuthRejectUnauthorized, AuthRejectUnauthorized, "auth required")
		return err
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
		return s.handleIncomingTransferEnd(frame)
	case FrameTransferResume:
		return s.handleIncomingTransferResume(ctx, frame)
	case FrameTransferState:
		return s.handleIncomingTransferState(frame)
	case FrameCancel:
		return s.handleIncomingCancel(frame)
	case FrameCancelAck:
		return nil
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

func (s *Session) Run(ctx context.Context) error {
	for {
		frame, err := Read(s.t)
		if err != nil {
			s.cleanupIncoming("connection lost")
			return err
		}
		if err := s.HandleFrame(ctx, frame); err != nil {
			return err
		}
	}
}

func (s *Session) checkRateLimit(frame Frame) error {
	now := time.Now()
	s.rate.mu.Lock()
	if s.rate.windowStart.IsZero() || now.Sub(s.rate.windowStart) >= time.Second {
		s.rate.windowStart = now
		s.rate.frames = 0
		s.rate.bytes = 0
		s.rate.authAttempts = 0
		s.rate.badFrames = 0
	}
	s.rate.frames++
	s.rate.bytes += uint64(HeaderSize + len(frame.Payload))
	if frame.Header.FrameType == FrameAuth {
		s.rate.authAttempts++
	}
	if s.config.RateLimit.MaxFramesPerSecond > 0 && s.rate.frames > s.config.RateLimit.MaxFramesPerSecond {
		s.rate.mu.Unlock()
		s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: "rate limit exceeded: frames", FrameType: frame.Header.FrameType})
		return ErrRateLimitFrames
	}
	if s.config.RateLimit.MaxBytesPerSecond > 0 && s.rate.bytes > s.config.RateLimit.MaxBytesPerSecond {
		s.rate.mu.Unlock()
		s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: "rate limit exceeded: bytes", FrameType: frame.Header.FrameType})
		return ErrRateLimitBytes
	}
	if s.config.RateLimit.MaxAuthAttempts > 0 && s.rate.authAttempts > s.config.RateLimit.MaxAuthAttempts {
		s.rate.mu.Unlock()
		s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: "rate limit exceeded: auth attempts", FrameType: frame.Header.FrameType})
		return ErrRateLimitAuth
	}
	s.rate.mu.Unlock()
	return nil
}

func (s *Session) markBadFrame(frame Frame, message string) error {
	s.rate.mu.Lock()
	s.rate.badFrames++
	badFrames := s.rate.badFrames
	limit := s.config.RateLimit.MaxBadFramesPerSecond
	s.rate.mu.Unlock()
	s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: message, FrameType: frame.Header.FrameType})
	if limit > 0 && badFrames > limit {
		_ = s.SendError(ErrorRateLimited, frame, "rate limit exceeded: bad frames")
		return ErrRateLimitBadFrames
	}
	return nil
}

func (s *Session) HandleHello(frame Frame) error {
	hello, err := DecodeHelloMessage(frame.Payload)
	if err != nil {
		s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: "invalid hello", FrameType: frame.Header.FrameType})
		_ = s.markBadFrame(frame, "invalid hello")
		return err
	}
	s.remoteCaps.Store(hello.Capabilities)
	return s.SendHelloAck(s.config.Role)
}

func (s *Session) HandleHelloAck(frame Frame) error {
	hello, err := DecodeHelloMessage(frame.Payload)
	if err != nil {
		s.emitEvent(ProtocolEvent{Code: EventProtocolViolation, Message: "invalid hello ack", FrameType: frame.Header.FrameType})
		_ = s.markBadFrame(frame, "invalid hello ack")
		return err
	}
	s.remoteCaps.Store(hello.Capabilities)
	s.setState(SessionEstablished)
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
			return s.config.Receive.TextHandler(ctx, frame, text)
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
		return s.config.Receive.RequestHandler(ctx, frame, message)
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
		return s.config.Receive.ResponseHandler(ctx, frame, message)
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
	}
	return nil
}

func (s *Session) handleIncomingGoAway(frame Frame) error {
	msg, err := DecodeGoAwayView(frame.Payload)
	if err != nil {
		return err
	}
	s.emitEvent(ProtocolEvent{Code: EventGoAwayReceived, Message: string(msg.Message), FrameType: frame.Header.FrameType})
	if msg.Flags&CloseFlagDrain != 0 {
		s.setState(SessionDraining)
		return nil
	}
	s.cleanupIncoming("goaway received")
	s.setState(SessionClosing)
	return nil
}

func (s *Session) handleIncomingClose(frame Frame) error {
	closeMessage, err := DecodeCloseMessage(frame.Payload)
	if err != nil {
		closeMessage = CloseMessage{ReasonCode: CloseNormal, Flags: CloseFlagImmediate}
	}
	if closeMessage.Flags&CloseFlagDrain != 0 {
		s.setState(SessionDraining)
	} else {
		s.cleanupIncoming("peer closed session")
		s.setState(SessionClosing)
	}
	if err := s.SendCloseAck(closeMessage); err != nil {
		return err
	}
	s.setState(SessionClosed)
	return s.t.Close()
}

func (s *Session) handleIncomingCloseAck(frame Frame) error {
	closeMessage, err := DecodeCloseMessage(frame.Payload)
	if err != nil {
		return err
	}
	s.emitEvent(ProtocolEvent{Code: EventCloseAckReceived, Message: fmt.Sprintf("close ack reason=%d", closeMessage.ReasonCode), FrameType: frame.Header.FrameType})
	s.cleanupIncoming("close ack received")
	s.setState(SessionClosed)
	return s.t.Close()
}

func (s *Session) handleIncomingTransferBegin(ctx context.Context, frame Frame) error {
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
	if meta.TotalSize > s.config.FlowControl.MaxReceiveBufferBytes {
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
		return fmt.Errorf("incoming transfer exceeds receive buffer: %d > %d", meta.TotalSize, s.config.FlowControl.MaxReceiveBufferBytes)
	}
	if s.config.Receive.TransferHandler == nil {
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
		return errors.New("incoming transfer handler is not configured")
	}

	s.mu.Lock()
	if len(s.incoming) >= s.config.FlowControl.MaxConcurrentTransfers {
		s.mu.Unlock()
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
		return fmt.Errorf("too many incoming transfers: %d", s.config.FlowControl.MaxConcurrentTransfers)
	}
	if s.incoming[frame.Header.TransferID] != nil {
		s.mu.Unlock()
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackProtocolError)
		return fmt.Errorf("duplicate incoming transfer id: %d", frame.Header.TransferID)
	}
	s.mu.Unlock()

	writer, err := s.config.Receive.TransferHandler(ctx, IncomingTransferInfo{
		TransferID: frame.Header.TransferID,
		RequestID:  frame.Header.RequestID,
		Meta:       meta,
	})
	if err != nil {
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackWriteFailed)
		return err
	}
	if writer == nil {
		_ = s.sendNack(frame.Header.TransferID, 0, 0, NackWriteFailed)
		return errors.New("incoming transfer handler returned nil writer")
	}

	var transferHash hash.Hash
	if meta.Flags&TransferFlagChecksumSHA256 != 0 {
		transferHash = sha256.New()
	}
	tr := s.newIncomingTransfer(meta, writer, transferHash)

	s.mu.Lock()
	s.incoming[frame.Header.TransferID] = tr
	s.mu.Unlock()
	go s.runIncomingTransfer(frame.Header.TransferID, tr)
	if err := s.sendIncomingWindow(frame.Header.TransferID); err != nil {
		return err
	}
	return nil
}

func (s *Session) handleIncomingTransferData(frame Frame) error {
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
	if frame.Header.ChunkID < tr.nextChunk {
		firstChunk, lastChunk, receivedBytes := tr.firstChunk, tr.lastChunk, tr.receivedBytes
		tr.mu.Unlock()
		return s.sendAck(frame.Header.TransferID, firstChunk, lastChunk, receivedBytes)
	}
	if frame.Header.ChunkID > tr.nextChunk {
		nextChunk := tr.nextChunk
		tr.mu.Unlock()
		return s.sendNack(frame.Header.TransferID, nextChunk, frame.Header.ChunkID-1, NackMissingChunk)
	}
	nextReceived := tr.receivedBytes + uint64(len(frame.Payload))
	if tr.meta.TotalSize > 0 && nextReceived > tr.meta.TotalSize {
		tr.mu.Unlock()
		s.removeIncoming(frame.Header.TransferID)
		abortIncoming(tr)
		return s.sendNack(frame.Header.TransferID, frame.Header.ChunkID, frame.Header.ChunkID, NackInvalidChunk)
	}
	if tr.receivedBytes == 0 {
		tr.firstChunk = frame.Header.ChunkID
	}
	tr.lastChunk = frame.Header.ChunkID
	tr.nextChunk = frame.Header.ChunkID + 1
	tr.receivedBytes = nextReceived
	tr.mu.Unlock()

	select {
	case tr.queue <- incomingChunk{frame: frame, payload: frame.Payload}:
		return nil
	default:
		return s.sendNack(frame.Header.TransferID, frame.Header.ChunkID, frame.Header.ChunkID, NackWriteFailed)
	}
}

func (s *Session) runIncomingTransfer(transferID uint64, tr *incomingTransfer) {
	for chunk := range tr.queue {
		frame := chunk.frame
		n, err := tr.writer.Write(chunk.payload)
		if err != nil {
			tr.fail(err)
			_ = s.sendNack(frame.Header.TransferID, frame.Header.ChunkID, frame.Header.ChunkID, NackWriteFailed)
			continue
		}
		if n != len(chunk.payload) {
			err := io.ErrShortWrite
			tr.fail(err)
			_ = s.sendNack(frame.Header.TransferID, frame.Header.ChunkID, frame.Header.ChunkID, NackWriteFailed)
			continue
		}
		tr.mu.Lock()
		if tr.hash != nil {
			_, _ = tr.hash.Write(chunk.payload)
		}
		firstChunk, lastChunk, receivedBytes := tr.firstChunk, tr.lastChunk, tr.receivedBytes
		nextChunk, totalSize := tr.nextChunk, tr.meta.TotalSize
		tr.mu.Unlock()
		if frame.Header.Flags&FlagAckRequest != 0 {
			_ = s.sendAck(frame.Header.TransferID, firstChunk, lastChunk, receivedBytes)
		}
		_ = s.sendIncomingWindow(transferID)
		s.emitProgress(Progress{
			TransferID:        transferID,
			TotalBytes:        totalSize,
			LocalWrittenBytes: receivedBytes,
			SentChunks:        nextChunk,
			State:             TransferSending,
		})
	}
	tr.mu.Lock()
	err := tr.failed
	tr.mu.Unlock()
	tr.done <- err
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
		close(tr.queue)
	})
}

func (s *Session) sendIncomingWindow(transferID uint64) error {
	if !s.RemoteSupports(CapabilityFlowControl) {
		return nil
	}
	windowBytes := s.config.FlowControl.MaxInFlightBytes
	if windowBytes == 0 || windowBytes > s.config.FlowControl.MaxReceiveBufferBytes {
		windowBytes = s.config.FlowControl.MaxReceiveBufferBytes
	}
	windowChunks := uint32(s.config.FlowControl.MaxInFlightChunks)
	if windowChunks == 0 {
		windowChunks = 1
	}
	return s.SendWindow(Window{
		TransferID:   transferID,
		WindowBytes:  windowBytes,
		WindowChunks: windowChunks,
		Flags:        WindowFlagTransfer,
	})
}

func (s *Session) handleIncomingTransferEnd(frame Frame) error {
	tr := s.lookupIncoming(frame.Header.TransferID)
	if tr == nil {
		return s.sendNack(frame.Header.TransferID, frame.Header.ChunkID, frame.Header.ChunkID, NackTransferUnknown)
	}
	tr.closeQueue()
	if err := <-tr.done; err != nil {
		s.removeIncoming(frame.Header.TransferID)
		abortIncoming(tr)
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
		abortIncoming(tr)
		return s.sendNack(frame.Header.TransferID, firstChunk, lastChunk, NackInvalidChunk)
	}
	if checksum != nil && !bytes.Equal(checksum, expectedChecksum[:]) {
		s.removeIncoming(frame.Header.TransferID)
		abortIncoming(tr)
		return s.sendNack(frame.Header.TransferID, firstChunk, lastChunk, NackInvalidChunk)
	}
	if err := tr.writer.Close(); err != nil {
		s.removeIncoming(frame.Header.TransferID)
		return s.sendNack(frame.Header.TransferID, firstChunk, lastChunk, NackWriteFailed)
	}
	s.removeIncoming(frame.Header.TransferID)
	s.emitProgress(Progress{
		TransferID:        frame.Header.TransferID,
		TotalBytes:        totalSize,
		LocalWrittenBytes: receivedBytes,
		SentChunks:        nextChunk,
		State:             TransferCompleted,
	})
	return nil
}

func (s *Session) handleIncomingTransferResume(ctx context.Context, frame Frame) error {
	resume, err := DecodeTransferResumeView(frame.Payload)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.incoming[resume.TransferID] != nil {
		s.mu.Unlock()
		return s.SendTransferState(TransferStateMessage{
			TransferID:    resume.TransferID,
			ReceivedBytes: resume.ReceivedBytes,
			NextChunk:     resume.NextChunk,
			Flags:         TransferStateFlagResumeRejected,
			ReasonCode:    NackProtocolError,
		})
	}
	s.mu.Unlock()
	if s.config.Resume.Store != nil {
		decision, err := s.config.Resume.Store.ResumeIncoming(ctx, resume)
		if err != nil {
			return err
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
			if decision.Meta.Flags&TransferFlagChecksumSHA256 != 0 && decision.Hash == nil {
				return s.SendTransferState(TransferStateMessage{
					TransferID:    resume.TransferID,
					ReceivedBytes: resume.ReceivedBytes,
					NextChunk:     resume.NextChunk,
					Flags:         TransferStateFlagResumeRejected,
					ReasonCode:    NackProtocolError,
				})
			}
			s.mu.Lock()
			if len(s.incoming) >= s.config.FlowControl.MaxConcurrentTransfers {
				s.mu.Unlock()
				return s.SendTransferState(TransferStateMessage{
					TransferID:    resume.TransferID,
					ReceivedBytes: resume.ReceivedBytes,
					NextChunk:     resume.NextChunk,
					Flags:         TransferStateFlagResumeRejected,
					ReasonCode:    NackProtocolError,
				})
			}
			s.mu.Unlock()
			tr := s.newIncomingTransfer(decision.Meta, decision.Writer, decision.Hash)
			tr.receivedBytes = decision.ReceivedBytes
			tr.nextChunk = decision.NextChunk
			if decision.NextChunk > 0 {
				tr.firstChunk = 0
				tr.lastChunk = decision.NextChunk - 1
			}
			s.mu.Lock()
			s.incoming[resume.TransferID] = tr
			s.mu.Unlock()
			go s.runIncomingTransfer(resume.TransferID, tr)
			if err := s.SendTransferState(TransferStateMessage{
				TransferID:    resume.TransferID,
				ReceivedBytes: decision.ReceivedBytes,
				NextChunk:     decision.NextChunk,
				Flags:         TransferStateFlagResumeAccepted,
			}); err != nil {
				return err
			}
			return s.sendIncomingWindow(resume.TransferID)
		}
		reason := decision.ReasonCode
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

func (s *Session) handleIncomingTransferState(frame Frame) error {
	state, err := DecodeTransferStateMessage(frame.Payload)
	if err != nil {
		return err
	}
	s.emitEvent(ProtocolEvent{
		Code:       EventTransferFailed,
		Message:    fmt.Sprintf("transfer state flags=%d reason=%d", state.Flags, state.ReasonCode),
		FrameType:  frame.Header.FrameType,
		TransferID: state.TransferID,
	})
	return nil
}

func (s *Session) handleIncomingCancel(frame Frame) error {
	cancel, err := DecodeCancel(frame.Payload)
	if err != nil {
		return err
	}
	status := CancelAckNotFound
	if tr := s.lookupIncoming(cancel.TransferID); tr != nil {
		s.removeIncoming(cancel.TransferID)
		abortIncoming(tr)
		status = CancelAckOK
	}
	ack := NewFrame(FrameCancelAck, SchemaCancel, EncodeCancelAck(cancel.TransferID, status))
	ack.Header.Priority = PriorityCritical
	ack.Header.ChannelID = ChannelControl
	ack.Header.TransferID = cancel.TransferID
	ack.Header.Flags = FlagControl
	return s.send(ack)
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

func (s *Session) cleanupIncoming(reason string) {
	s.mu.Lock()
	transfers := s.incoming
	s.incoming = map[uint64]*incomingTransfer{}
	s.mu.Unlock()
	for transferID, tr := range transfers {
		abortIncoming(tr)
		s.emitEvent(ProtocolEvent{Code: EventTransferFailed, Message: reason, TransferID: transferID})
	}
}

func abortIncoming(tr *incomingTransfer) {
	tr.closeQueue()
	if aborter, ok := tr.writer.(IncomingTransferAborter); ok {
		_ = aborter.Abort()
		return
	}
	_ = tr.writer.Close()
}

type discardIncomingWriter struct{}

func (discardIncomingWriter) Write(p []byte) (int, error) { return len(p), nil }
func (discardIncomingWriter) Close() error                { return nil }

var _ IncomingTransferWriter = discardIncomingWriter{}
var _ io.Writer = discardIncomingWriter{}
