package etp

import (
	"encoding/binary"
	"errors"
)

const (
	MaxTransferFields = 1024
	MaxTransferParts  = 1024
)

const (
	CapabilityTransfers       uint64 = 1 << 0
	CapabilityCancel          uint64 = 1 << 1
	CapabilityAck             uint64 = 1 << 2
	CapabilityNack            uint64 = 1 << 3
	CapabilityHeartbeat       uint64 = 1 << 4
	CapabilityTransferSHA256  uint64 = 1 << 5
	CapabilityFlowControl     uint64 = 1 << 6
	CapabilitySlowlorisGuard  uint64 = 1 << 7
	CapabilityProtocolEvents  uint64 = 1 << 8
	CapabilityRequestResponse uint64 = 1 << 9
	CapabilityGracefulClose   uint64 = 1 << 10
	CapabilityTransferResume  uint64 = 1 << 11
	CapabilityTransferCommit  uint64 = 1 << 12
)

const AllCapabilities = CapabilityTransfers |
	CapabilityCancel |
	CapabilityAck |
	CapabilityNack |
	CapabilityHeartbeat |
	CapabilityTransferSHA256 |
	CapabilityFlowControl |
	CapabilitySlowlorisGuard |
	CapabilityProtocolEvents |
	CapabilityRequestResponse |
	CapabilityGracefulClose |
	CapabilityTransferResume |
	CapabilityTransferCommit

const DefaultCapabilities = CapabilityTransfers |
	CapabilityCancel |
	CapabilityAck |
	CapabilityNack |
	CapabilityHeartbeat |
	CapabilityTransferSHA256 |
	CapabilityFlowControl |
	CapabilitySlowlorisGuard |
	CapabilityProtocolEvents |
	CapabilityRequestResponse |
	CapabilityGracefulClose |
	CapabilityTransferCommit

const (
	ErrorProtocolViolation  uint32 = 1
	ErrorUnauthorized       uint32 = 2
	ErrorUnsupportedFeature uint32 = 3
	ErrorFrameTooLarge      uint32 = 4
	ErrorBadState           uint32 = 5
	ErrorRateLimited        uint32 = 6
	ErrorServerShutdown     uint32 = 7
	ErrorDrainTimeout       uint32 = 8
	ErrorInvalidRequest     uint32 = 9
	ErrorInternal           uint32 = 10
)

const (
	CloseNormal            uint32 = 0
	CloseProtocolViolation uint32 = 1
	CloseAuthFailed        uint32 = 2
	CloseServerShutdown    uint32 = 3
	CloseClientShutdown    uint32 = 4
	CloseDrainTimeout      uint32 = 5
)

const (
	CloseFlagImmediate      uint16 = 1 << 0
	CloseFlagDrain          uint16 = 1 << 1
	CloseFlagNoNewRequests  uint16 = 1 << 2
	CloseFlagNoNewTransfers uint16 = 1 << 3
)

type Hello struct {
	Role              string
	Capabilities      uint64
	MaxFrameBytes     uint32
	MaxChunkSize      uint32
	MaxTransferBytes  uint64
	MaxInFlightChunks uint32
	HeartbeatMillis   uint32
}

func EncodeHello(role string) []byte {
	return EncodeHelloMessage(Hello{Role: role, Capabilities: DefaultCapabilities})
}

func DecodeHello(payload []byte) (string, error) {
	hello, err := DecodeHelloMessage(payload)
	return hello.Role, err
}

func EncodeHelloMessage(v Hello) []byte {
	role := []byte(v.Role)
	if len(role) > MaxFrameBytes-HeaderSize-40 {
		return nil
	}
	out := make([]byte, 40+len(role))
	binary.BigEndian.PutUint64(out[0:8], v.Capabilities)
	binary.BigEndian.PutUint32(out[8:12], v.MaxFrameBytes)
	binary.BigEndian.PutUint32(out[12:16], v.MaxChunkSize)
	binary.BigEndian.PutUint64(out[16:24], v.MaxTransferBytes)
	binary.BigEndian.PutUint32(out[24:28], v.MaxInFlightChunks)
	binary.BigEndian.PutUint32(out[28:32], v.HeartbeatMillis)
	binary.BigEndian.PutUint32(out[36:40], uint32(len(role)))
	copy(out[40:], role)
	return out
}

func DecodeHelloMessage(payload []byte) (Hello, error) {
	if len(payload) < 40 {
		return Hello{}, errors.New("hello payload too small")
	}
	roleLen := binary.BigEndian.Uint32(payload[36:40])
	roleEnd, ok := checkedPayloadEnd(payload, 40, roleLen)
	if !ok || roleEnd != len(payload) {
		return Hello{}, errors.New("invalid hello role length")
	}
	if !zeroBytes(payload[32:36]) {
		return Hello{}, errors.New("hello reserved bytes are not zero")
	}
	return Hello{
		Capabilities:      binary.BigEndian.Uint64(payload[0:8]),
		MaxFrameBytes:     binary.BigEndian.Uint32(payload[8:12]),
		MaxChunkSize:      binary.BigEndian.Uint32(payload[12:16]),
		MaxTransferBytes:  binary.BigEndian.Uint64(payload[16:24]),
		MaxInFlightChunks: binary.BigEndian.Uint32(payload[24:28]),
		HeartbeatMillis:   binary.BigEndian.Uint32(payload[28:32]),
		Role:              string(payload[40:roleEnd]),
	}, nil
}

func EncodeTextMessage(text string) []byte {
	return encodeString(text)
}

func EncodeTextMessageInto(dst []byte, text string) []byte {
	if len(text) > MaxFrameBytes-HeaderSize-4 {
		return nil
	}
	size := 4 + len(text)
	if cap(dst) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}
	binary.BigEndian.PutUint32(dst[0:4], uint32(len(text)))
	copy(dst[4:], text)
	return dst
}

func DecodeTextMessage(payload []byte) (string, error) {
	return decodeString(payload)
}

func transferFieldsPayloadSize(fields []TransferField) int {
	return saturatedInt(transferFieldsPayloadSize64(fields))
}

func transferFieldsPayloadSize64(fields []TransferField) uint64 {
	if len(fields) == 0 {
		return 0
	}
	size := uint64(4)
	for _, field := range fields {
		size += 8 + uint64(len(field.Key)) + uint64(len(field.Value))
	}
	return size
}

func encodeTransferFieldsInto(dst []byte, fields []TransferField) {
	if len(fields) == 0 {
		return
	}
	binary.BigEndian.PutUint32(dst[0:4], uint32(len(fields)))
	pos := 4
	for _, field := range fields {
		key := []byte(field.Key)
		value := []byte(field.Value)
		binary.BigEndian.PutUint32(dst[pos:pos+4], uint32(len(key)))
		pos += 4
		copy(dst[pos:], key)
		pos += len(key)
		binary.BigEndian.PutUint32(dst[pos:pos+4], uint32(len(value)))
		pos += 4
		copy(dst[pos:], value)
		pos += len(value)
	}
}

func decodeTransferFields(payload []byte) ([]TransferField, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	if len(payload) < 4 {
		return nil, errors.New("invalid transfer fields length")
	}
	fieldCount := binary.BigEndian.Uint32(payload[0:4])
	if fieldCount > MaxTransferFields || uint64(fieldCount)*8 > uint64(len(payload)-4) {
		return nil, errors.New("invalid transfer field count")
	}
	pos := 4
	fields := make([]TransferField, 0, fieldCount)
	for i := uint32(0); i < fieldCount; i++ {
		if pos+4 > len(payload) {
			return nil, errors.New("invalid transfer field key length")
		}
		keyLen := binary.BigEndian.Uint32(payload[pos : pos+4])
		pos += 4
		keyEnd, ok := checkedPayloadEnd(payload, pos, keyLen)
		if !ok {
			return nil, errors.New("invalid transfer field key length")
		}
		key := string(payload[pos:keyEnd])
		pos = keyEnd
		if pos+4 > len(payload) {
			return nil, errors.New("invalid transfer field value length")
		}
		valueLen := binary.BigEndian.Uint32(payload[pos : pos+4])
		pos += 4
		valueEnd, ok := checkedPayloadEnd(payload, pos, valueLen)
		if !ok {
			return nil, errors.New("invalid transfer field value length")
		}
		value := string(payload[pos:valueEnd])
		pos = valueEnd
		fields = append(fields, TransferField{Key: key, Value: value})
	}
	if pos != len(payload) {
		return nil, errors.New("invalid transfer fields trailing data")
	}
	return fields, nil
}

type EventMessage struct {
	Event  string
	Data   []byte
	Fields []TransferField
}

type EventMessageView struct {
	Event  []byte
	Data   []byte
	Fields []TransferField
}

func EventMessagePayloadSize(event []byte, data []byte, fields []TransferField) int {
	return saturatedInt(eventMessagePayloadSize64(len(event), len(data), fields))
}

func EventMessagePayloadSizeString(event string, data []byte, fields []TransferField) int {
	return saturatedInt(eventMessagePayloadSize64(len(event), len(data), fields))
}

func eventMessagePayloadSize64(eventBytes int, dataBytes int, fields []TransferField) uint64 {
	return 8 + uint64(eventBytes) + uint64(dataBytes) + transferFieldsPayloadSize64(fields)
}

func EncodeEventMessage(v EventMessage) []byte {
	out, _ := EncodeEventMessageStringWithFieldsInto(nil, v.Event, v.Data, v.Fields)
	return out
}

func EncodeEventMessageInto(dst []byte, event []byte, data []byte) ([]byte, error) {
	return EncodeEventMessageWithFieldsInto(dst, event, data, nil)
}

func EncodeEventMessageWithFieldsInto(dst []byte, event []byte, data []byte, fields []TransferField) ([]byte, error) {
	size64 := eventMessagePayloadSize64(len(event), len(data), fields)
	if size64 > uint64(MaxFrameBytes-HeaderSize) {
		return nil, errors.New("event payload exceeds max frame size")
	}
	size := int(size64)
	if cap(dst) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}
	binary.BigEndian.PutUint32(dst[0:4], uint32(len(event)))
	copy(dst[4:4+len(event)], event)
	dataOffset := 4 + len(event)
	binary.BigEndian.PutUint32(dst[dataOffset:dataOffset+4], uint32(len(data)))
	fieldsOffset := dataOffset + 4 + len(data)
	copy(dst[dataOffset+4:fieldsOffset], data)
	encodeTransferFieldsInto(dst[fieldsOffset:], fields)
	return dst, nil
}

func EncodeEventMessageStringWithFieldsInto(dst []byte, event string, data []byte, fields []TransferField) ([]byte, error) {
	size64 := eventMessagePayloadSize64(len(event), len(data), fields)
	if size64 > uint64(MaxFrameBytes-HeaderSize) {
		return nil, errors.New("event payload exceeds max frame size")
	}
	return encodeEventMessageStringWithFieldsSizedInto(dst, event, data, fields, int(size64)), nil
}

func encodeEventMessageStringWithFieldsSizedInto(dst []byte, event string, data []byte, fields []TransferField, size int) []byte {
	if cap(dst) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}
	binary.BigEndian.PutUint32(dst[0:4], uint32(len(event)))
	copy(dst[4:4+len(event)], event)
	dataOffset := 4 + len(event)
	binary.BigEndian.PutUint32(dst[dataOffset:dataOffset+4], uint32(len(data)))
	fieldsOffset := dataOffset + 4 + len(data)
	copy(dst[dataOffset+4:fieldsOffset], data)
	encodeTransferFieldsInto(dst[fieldsOffset:], fields)
	return dst
}

func DecodeEventMessageView(payload []byte) (EventMessageView, error) {
	if len(payload) < 8 {
		return EventMessageView{}, errors.New("event payload too small")
	}
	eventLen := binary.BigEndian.Uint32(payload[0:4])
	dataLenOffset, ok := checkedPayloadEnd(payload, 4, eventLen)
	if !ok || len(payload)-dataLenOffset < 4 {
		return EventMessageView{}, errors.New("invalid event name length")
	}
	dataLen := binary.BigEndian.Uint32(payload[dataLenOffset : dataLenOffset+4])
	end, ok := checkedPayloadEnd(payload, dataLenOffset+4, dataLen)
	if !ok {
		return EventMessageView{}, errors.New("invalid event data length")
	}
	fields, err := decodeTransferFields(payload[end:])
	if err != nil {
		return EventMessageView{}, err
	}
	return EventMessageView{
		Event:  payload[4:dataLenOffset],
		Data:   payload[dataLenOffset+4 : end],
		Fields: fields,
	}, nil
}

func DecodeEventMessage(payload []byte) (EventMessage, error) {
	view, err := DecodeEventMessageView(payload)
	if err != nil {
		return EventMessage{}, err
	}
	return EventMessage{
		Event:  string(view.Event),
		Data:   view.Data,
		Fields: view.Fields,
	}, nil
}

type ErrorMessage struct {
	Code       uint32
	FrameType  uint8
	SchemaID   uint32
	RequestID  uint64
	TransferID uint64
	Message    string
}

type ErrorMessageView struct {
	Code       uint32
	FrameType  uint8
	SchemaID   uint32
	RequestID  uint64
	TransferID uint64
	Message    []byte
}

func ErrorMessagePayloadSize(message []byte) int {
	return saturatedInt(32 + uint64(len(message)))
}

func ErrorMessagePayloadSizeString(message string) int {
	return saturatedInt(32 + uint64(len(message)))
}

func EncodeErrorMessage(v ErrorMessage) []byte {
	out, _ := EncodeErrorMessageStringInto(nil, v.Code, v.FrameType, v.SchemaID, v.RequestID, v.TransferID, v.Message)
	return out
}

func EncodeErrorMessageInto(dst []byte, code uint32, frameType uint8, schemaID uint32, requestID uint64, transferID uint64, message []byte) ([]byte, error) {
	if len(message) > MaxFrameBytes-HeaderSize-32 {
		return nil, errors.New("error message too large")
	}
	size := ErrorMessagePayloadSize(message)
	if cap(dst) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}
	binary.BigEndian.PutUint32(dst[0:4], code)
	dst[4] = frameType
	clear(dst[5:8])
	binary.BigEndian.PutUint32(dst[8:12], schemaID)
	binary.BigEndian.PutUint64(dst[12:20], requestID)
	binary.BigEndian.PutUint64(dst[20:28], transferID)
	binary.BigEndian.PutUint32(dst[28:32], uint32(len(message)))
	copy(dst[32:], message)
	return dst, nil
}

func EncodeErrorMessageStringInto(dst []byte, code uint32, frameType uint8, schemaID uint32, requestID uint64, transferID uint64, message string) ([]byte, error) {
	if len(message) > MaxFrameBytes-HeaderSize-32 {
		return nil, errors.New("error message too large")
	}
	size := 32 + len(message)
	if cap(dst) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}
	binary.BigEndian.PutUint32(dst[0:4], code)
	dst[4] = frameType
	clear(dst[5:8])
	binary.BigEndian.PutUint32(dst[8:12], schemaID)
	binary.BigEndian.PutUint64(dst[12:20], requestID)
	binary.BigEndian.PutUint64(dst[20:28], transferID)
	binary.BigEndian.PutUint32(dst[28:32], uint32(len(message)))
	copy(dst[32:], message)
	return dst, nil
}

func DecodeErrorMessageView(payload []byte) (ErrorMessageView, error) {
	if len(payload) < 32 {
		return ErrorMessageView{}, errors.New("error payload too small")
	}
	messageLen := binary.BigEndian.Uint32(payload[28:32])
	end, ok := checkedPayloadEnd(payload, 32, messageLen)
	if !ok || end != len(payload) {
		return ErrorMessageView{}, errors.New("invalid error message length")
	}
	if !zeroBytes(payload[5:8]) {
		return ErrorMessageView{}, errors.New("error reserved bytes are not zero")
	}
	return ErrorMessageView{
		Code:       binary.BigEndian.Uint32(payload[0:4]),
		FrameType:  payload[4],
		SchemaID:   binary.BigEndian.Uint32(payload[8:12]),
		RequestID:  binary.BigEndian.Uint64(payload[12:20]),
		TransferID: binary.BigEndian.Uint64(payload[20:28]),
		Message:    payload[32:end],
	}, nil
}

func DecodeErrorMessage(payload []byte) (ErrorMessage, error) {
	view, err := DecodeErrorMessageView(payload)
	if err != nil {
		return ErrorMessage{}, err
	}
	return ErrorMessage{
		Code:       view.Code,
		FrameType:  view.FrameType,
		SchemaID:   view.SchemaID,
		RequestID:  view.RequestID,
		TransferID: view.TransferID,
		Message:    string(view.Message),
	}, nil
}

type GoAway struct {
	ReasonCode             uint32
	Flags                  uint16
	DrainTimeoutMillis     uint32
	LastAcceptedRequestID  uint64
	LastAcceptedTransferID uint64
	Message                string
}

type GoAwayView struct {
	ReasonCode             uint32
	Flags                  uint16
	DrainTimeoutMillis     uint32
	LastAcceptedRequestID  uint64
	LastAcceptedTransferID uint64
	Message                []byte
}

func GoAwayPayloadSize(message []byte) int {
	return saturatedInt(32 + uint64(len(message)))
}

func EncodeGoAway(v GoAway) []byte {
	out, _ := EncodeGoAwayInto(nil, v.ReasonCode, v.Flags, v.DrainTimeoutMillis, v.LastAcceptedRequestID, v.LastAcceptedTransferID, []byte(v.Message))
	return out
}

func EncodeGoAwayInto(dst []byte, reasonCode uint32, flags uint16, drainTimeoutMillis uint32, lastAcceptedRequestID uint64, lastAcceptedTransferID uint64, message []byte) ([]byte, error) {
	if len(message) > MaxFrameBytes-HeaderSize-32 {
		return nil, errors.New("goaway message too large")
	}
	size := GoAwayPayloadSize(message)
	if cap(dst) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}
	binary.BigEndian.PutUint32(dst[0:4], reasonCode)
	binary.BigEndian.PutUint16(dst[4:6], flags)
	clear(dst[6:8])
	binary.BigEndian.PutUint32(dst[8:12], drainTimeoutMillis)
	binary.BigEndian.PutUint64(dst[12:20], lastAcceptedRequestID)
	binary.BigEndian.PutUint64(dst[20:28], lastAcceptedTransferID)
	binary.BigEndian.PutUint32(dst[28:32], uint32(len(message)))
	copy(dst[32:], message)
	return dst, nil
}

func DecodeGoAwayView(payload []byte) (GoAwayView, error) {
	if len(payload) < 32 {
		return GoAwayView{}, errors.New("goaway payload too small")
	}
	messageLen := binary.BigEndian.Uint32(payload[28:32])
	end, ok := checkedPayloadEnd(payload, 32, messageLen)
	if !ok || end != len(payload) {
		return GoAwayView{}, errors.New("invalid goaway message length")
	}
	flags := binary.BigEndian.Uint16(payload[4:6])
	if flags&^uint16(CloseFlagImmediate|CloseFlagDrain|CloseFlagNoNewRequests|CloseFlagNoNewTransfers) != 0 || flags&(CloseFlagImmediate|CloseFlagDrain) == CloseFlagImmediate|CloseFlagDrain || !zeroBytes(payload[6:8]) {
		return GoAwayView{}, errors.New("invalid goaway flags or reserved bytes")
	}
	return GoAwayView{
		ReasonCode:             binary.BigEndian.Uint32(payload[0:4]),
		Flags:                  flags,
		DrainTimeoutMillis:     binary.BigEndian.Uint32(payload[8:12]),
		LastAcceptedRequestID:  binary.BigEndian.Uint64(payload[12:20]),
		LastAcceptedTransferID: binary.BigEndian.Uint64(payload[20:28]),
		Message:                payload[32:end],
	}, nil
}

func DecodeGoAway(payload []byte) (GoAway, error) {
	view, err := DecodeGoAwayView(payload)
	if err != nil {
		return GoAway{}, err
	}
	return GoAway{
		ReasonCode:             view.ReasonCode,
		Flags:                  view.Flags,
		DrainTimeoutMillis:     view.DrainTimeoutMillis,
		LastAcceptedRequestID:  view.LastAcceptedRequestID,
		LastAcceptedTransferID: view.LastAcceptedTransferID,
		Message:                string(view.Message),
	}, nil
}

type CloseMessage struct {
	ReasonCode         uint32
	Flags              uint16
	DrainTimeoutMillis uint32
}

func EncodeCloseMessage(v CloseMessage) []byte {
	out := make([]byte, 12)
	binary.BigEndian.PutUint32(out[0:4], v.ReasonCode)
	binary.BigEndian.PutUint16(out[4:6], v.Flags)
	binary.BigEndian.PutUint32(out[8:12], v.DrainTimeoutMillis)
	return out
}

func DecodeCloseMessage(payload []byte) (CloseMessage, error) {
	if len(payload) != 12 {
		return CloseMessage{}, errors.New("invalid close payload length")
	}
	flags := binary.BigEndian.Uint16(payload[4:6])
	if flags&^uint16(CloseFlagImmediate|CloseFlagDrain|CloseFlagNoNewRequests|CloseFlagNoNewTransfers) != 0 || flags&(CloseFlagImmediate|CloseFlagDrain) == CloseFlagImmediate|CloseFlagDrain || !zeroBytes(payload[6:8]) {
		return CloseMessage{}, errors.New("invalid close flags or reserved bytes")
	}
	return CloseMessage{
		ReasonCode:         binary.BigEndian.Uint32(payload[0:4]),
		Flags:              flags,
		DrainTimeoutMillis: binary.BigEndian.Uint32(payload[8:12]),
	}, nil
}

const (
	WindowFlagConnection uint16 = 1 << 0
	WindowFlagTransfer   uint16 = 1 << 1
)

type Window struct {
	TransferID   uint64
	WindowBytes  uint64
	WindowChunks uint32
	Flags        uint16
}

func EncodeWindow(v Window) []byte {
	return EncodeWindowInto(nil, v)
}

func EncodeWindowInto(dst []byte, v Window) []byte {
	if cap(dst) < 24 {
		dst = make([]byte, 24)
	} else {
		dst = dst[:24]
	}
	out := dst
	binary.BigEndian.PutUint64(out[0:8], v.TransferID)
	binary.BigEndian.PutUint64(out[8:16], v.WindowBytes)
	binary.BigEndian.PutUint32(out[16:20], v.WindowChunks)
	binary.BigEndian.PutUint16(out[20:22], v.Flags)
	clear(out[22:24])
	return out
}

func DecodeWindow(payload []byte) (Window, error) {
	if len(payload) != 24 {
		return Window{}, errors.New("invalid window payload length")
	}
	flags := binary.BigEndian.Uint16(payload[20:22])
	if flags != WindowFlagConnection && flags != WindowFlagTransfer || !zeroBytes(payload[22:24]) {
		return Window{}, errors.New("invalid window flags or reserved bytes")
	}
	return Window{
		TransferID:   binary.BigEndian.Uint64(payload[0:8]),
		WindowBytes:  binary.BigEndian.Uint64(payload[8:16]),
		WindowChunks: binary.BigEndian.Uint32(payload[16:20]),
		Flags:        flags,
	}, nil
}

const (
	TransferStateFlagResumeAccepted uint16 = 1 << 0
	TransferStateFlagResumeRejected uint16 = 1 << 1
	TransferStateFlagCompleted      uint16 = 1 << 2
	TransferStateFlagFailed         uint16 = 1 << 3
)

type TransferResume struct {
	TransferID    uint64
	ReceivedBytes uint64
	NextChunk     uint32
	Token         []byte
}

type TransferResumeView struct {
	TransferID    uint64
	ReceivedBytes uint64
	NextChunk     uint32
	Token         []byte
}

func TransferResumePayloadSize(token []byte) int {
	return saturatedInt(24 + uint64(len(token)))
}

func EncodeTransferResume(v TransferResume) []byte {
	out, _ := EncodeTransferResumeInto(nil, v.TransferID, v.ReceivedBytes, v.NextChunk, v.Token)
	return out
}

func EncodeTransferResumeInto(dst []byte, transferID uint64, receivedBytes uint64, nextChunk uint32, token []byte) ([]byte, error) {
	if len(token) > MaxFrameBytes-HeaderSize-24 {
		return nil, errors.New("resume token too large")
	}
	size := TransferResumePayloadSize(token)
	if cap(dst) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}
	binary.BigEndian.PutUint64(dst[0:8], transferID)
	binary.BigEndian.PutUint64(dst[8:16], receivedBytes)
	binary.BigEndian.PutUint32(dst[16:20], nextChunk)
	binary.BigEndian.PutUint32(dst[20:24], uint32(len(token)))
	copy(dst[24:], token)
	return dst, nil
}

func DecodeTransferResumeView(payload []byte) (TransferResumeView, error) {
	if len(payload) < 24 {
		return TransferResumeView{}, errors.New("transfer resume payload too small")
	}
	tokenLen := binary.BigEndian.Uint32(payload[20:24])
	end, ok := checkedPayloadEnd(payload, 24, tokenLen)
	if !ok || end != len(payload) {
		return TransferResumeView{}, errors.New("invalid transfer resume token length")
	}
	return TransferResumeView{
		TransferID:    binary.BigEndian.Uint64(payload[0:8]),
		ReceivedBytes: binary.BigEndian.Uint64(payload[8:16]),
		NextChunk:     binary.BigEndian.Uint32(payload[16:20]),
		Token:         payload[24:end],
	}, nil
}

func DecodeTransferResume(payload []byte) (TransferResume, error) {
	view, err := DecodeTransferResumeView(payload)
	if err != nil {
		return TransferResume{}, err
	}
	return TransferResume{
		TransferID:    view.TransferID,
		ReceivedBytes: view.ReceivedBytes,
		NextChunk:     view.NextChunk,
		Token:         view.Token,
	}, nil
}

type TransferStateMessage struct {
	TransferID    uint64
	ReceivedBytes uint64
	NextChunk     uint32
	Flags         uint16
	ReasonCode    uint16
}

func EncodeTransferStateMessage(v TransferStateMessage) []byte {
	return EncodeTransferStateMessageInto(nil, v)
}

func EncodeTransferStateMessageInto(dst []byte, v TransferStateMessage) []byte {
	if cap(dst) < 24 {
		dst = make([]byte, 24)
	} else {
		dst = dst[:24]
	}
	out := dst
	binary.BigEndian.PutUint64(out[0:8], v.TransferID)
	binary.BigEndian.PutUint64(out[8:16], v.ReceivedBytes)
	binary.BigEndian.PutUint32(out[16:20], v.NextChunk)
	binary.BigEndian.PutUint16(out[20:22], v.Flags)
	binary.BigEndian.PutUint16(out[22:24], v.ReasonCode)
	return out
}

func DecodeTransferStateMessage(payload []byte) (TransferStateMessage, error) {
	if len(payload) != 24 {
		return TransferStateMessage{}, errors.New("invalid transfer state payload length")
	}
	return TransferStateMessage{
		TransferID:    binary.BigEndian.Uint64(payload[0:8]),
		ReceivedBytes: binary.BigEndian.Uint64(payload[8:16]),
		NextChunk:     binary.BigEndian.Uint32(payload[16:20]),
		Flags:         binary.BigEndian.Uint16(payload[20:22]),
		ReasonCode:    binary.BigEndian.Uint16(payload[22:24]),
	}, nil
}

const (
	AuthMethodBearer  uint16 = 1
	AuthMethodAPIKey  uint16 = 2
	AuthMethodSession uint16 = 3
	AuthMethodCustom  uint16 = 255
)

const (
	AuthRejectUnauthorized uint16 = 401
	AuthRejectForbidden    uint16 = 403
	AuthRejectTimeout      uint16 = 408
	AuthRejectTooLarge     uint16 = 413
	AuthRejectProtocol     uint16 = 440
)

type AuthRequest struct {
	Method       uint16
	Flags        uint16
	AuthSchemaID uint32
	Payload      []byte
}

type AuthAccept struct {
	UserID string
}

type AuthAcceptView struct {
	UserID []byte
}

type AuthReject struct {
	StatusCode uint16
	ReasonCode uint16
	Message    string
}

type AuthRejectView struct {
	StatusCode uint16
	ReasonCode uint16
	Message    []byte
}

func AuthRequestPayloadSize(v AuthRequest) int {
	return saturatedInt(12 + uint64(len(v.Payload)))
}

func EncodeAuthRequest(v AuthRequest) []byte {
	out, _ := EncodeAuthRequestInto(nil, v)
	return out
}

func EncodeAuthRequestInto(dst []byte, v AuthRequest) ([]byte, error) {
	if len(v.Payload) > MaxFrameBytes-HeaderSize-12 {
		return nil, errors.New("auth payload too large")
	}
	size := AuthRequestPayloadSize(v)
	if cap(dst) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}
	binary.BigEndian.PutUint16(dst[0:2], v.Method)
	binary.BigEndian.PutUint16(dst[2:4], v.Flags)
	binary.BigEndian.PutUint32(dst[4:8], v.AuthSchemaID)
	binary.BigEndian.PutUint32(dst[8:12], uint32(len(v.Payload)))
	copy(dst[12:], v.Payload)
	return dst, nil
}

func DecodeAuthRequest(payload []byte) (AuthRequest, error) {
	return DecodeAuthRequestView(payload)
}

func DecodeAuthRequestView(payload []byte) (AuthRequest, error) {
	if len(payload) < 12 {
		return AuthRequest{}, errors.New("auth request payload too small")
	}
	payloadLen := binary.BigEndian.Uint32(payload[8:12])
	end, ok := checkedPayloadEnd(payload, 12, payloadLen)
	if !ok || end != len(payload) {
		return AuthRequest{}, errors.New("invalid auth request payload length")
	}
	return AuthRequest{
		Method:       binary.BigEndian.Uint16(payload[0:2]),
		Flags:        binary.BigEndian.Uint16(payload[2:4]),
		AuthSchemaID: binary.BigEndian.Uint32(payload[4:8]),
		Payload:      payload[12:end],
	}, nil
}

func EncodeAuthAccept(v AuthAccept) []byte {
	userID := []byte(v.UserID)
	out, _ := EncodeAuthAcceptInto(nil, userID)
	return out
}

func AuthAcceptPayloadSize(userID []byte) int {
	return saturatedInt(4 + uint64(len(userID)))
}

func EncodeAuthAcceptInto(dst []byte, userID []byte) ([]byte, error) {
	if len(userID) > MaxFrameBytes-HeaderSize-4 {
		return nil, errors.New("auth accept user id too large")
	}
	size := AuthAcceptPayloadSize(userID)
	if cap(dst) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}
	binary.BigEndian.PutUint32(dst[0:4], uint32(len(userID)))
	copy(dst[4:], userID)
	return dst, nil
}

func DecodeAuthAcceptView(payload []byte) (AuthAcceptView, error) {
	if len(payload) < 4 {
		return AuthAcceptView{}, errors.New("auth accept payload too small")
	}
	userLen := binary.BigEndian.Uint32(payload[0:4])
	end, ok := checkedPayloadEnd(payload, 4, userLen)
	if !ok || end != len(payload) {
		return AuthAcceptView{}, errors.New("invalid auth accept user length")
	}
	return AuthAcceptView{UserID: payload[4:end]}, nil
}

func DecodeAuthAccept(payload []byte) (AuthAccept, error) {
	view, err := DecodeAuthAcceptView(payload)
	if err != nil {
		return AuthAccept{}, err
	}
	return AuthAccept{UserID: string(view.UserID)}, nil
}

func EncodeAuthReject(v AuthReject) []byte {
	msg := []byte(v.Message)
	out, _ := EncodeAuthRejectInto(nil, v.StatusCode, v.ReasonCode, msg)
	return out
}

func AuthRejectPayloadSize(message []byte) int {
	return saturatedInt(8 + uint64(len(message)))
}

func EncodeAuthRejectInto(dst []byte, statusCode uint16, reasonCode uint16, message []byte) ([]byte, error) {
	if len(message) > MaxFrameBytes-HeaderSize-8 {
		return nil, errors.New("auth reject message too large")
	}
	size := AuthRejectPayloadSize(message)
	if cap(dst) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}
	binary.BigEndian.PutUint16(dst[0:2], statusCode)
	binary.BigEndian.PutUint16(dst[2:4], reasonCode)
	binary.BigEndian.PutUint32(dst[4:8], uint32(len(message)))
	copy(dst[8:], message)
	return dst, nil
}

func DecodeAuthRejectView(payload []byte) (AuthRejectView, error) {
	if len(payload) < 8 {
		return AuthRejectView{}, errors.New("auth reject payload too small")
	}
	msgLen := binary.BigEndian.Uint32(payload[4:8])
	end, ok := checkedPayloadEnd(payload, 8, msgLen)
	if !ok || end != len(payload) {
		return AuthRejectView{}, errors.New("invalid auth reject message length")
	}
	return AuthRejectView{
		StatusCode: binary.BigEndian.Uint16(payload[0:2]),
		ReasonCode: binary.BigEndian.Uint16(payload[2:4]),
		Message:    payload[8:end],
	}, nil
}

func DecodeAuthReject(payload []byte) (AuthReject, error) {
	view, err := DecodeAuthRejectView(payload)
	if err != nil {
		return AuthReject{}, err
	}
	return AuthReject{
		StatusCode: view.StatusCode,
		ReasonCode: view.ReasonCode,
		Message:    string(view.Message),
	}, nil
}

type TransferBegin struct {
	TotalSize   uint64
	ChunkSize   uint32
	ChunkCount  uint32
	ContentType uint32
	Flags       uint32
	Checksum    [32]byte
	Name        string
	Event       string
	Field       string
	Index       uint32
	Parts       []TransferPart
	Fields      []TransferField
}

type TransferPart struct {
	Field       string
	Index       uint32
	Name        string
	TotalSize   uint64
	ContentType uint32
}

func EncodeTransferBegin(v TransferBegin) []byte {
	return EncodeTransferBeginInto(nil, v)
}

func EncodeTransferBeginInto(dst []byte, v TransferBegin) []byte {
	size64 := transferBeginPayloadSize64(v.Name, v.Event, v.Field, v.Parts, v.Fields)
	if size64 > uint64(MaxFrameBytes-HeaderSize) {
		return nil
	}
	partsSize := int(transferPartsPayloadSize64(v.Parts))
	size := int(size64)
	if cap(dst) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}
	out := dst
	binary.BigEndian.PutUint64(out[0:8], v.TotalSize)
	binary.BigEndian.PutUint32(out[8:12], v.ChunkSize)
	binary.BigEndian.PutUint32(out[12:16], v.ChunkCount)
	binary.BigEndian.PutUint32(out[16:20], v.ContentType)
	binary.BigEndian.PutUint32(out[20:24], v.Flags)
	copy(out[24:56], v.Checksum[:])
	binary.BigEndian.PutUint32(out[56:60], uint32(len(v.Name)))
	copy(out[60:], v.Name)
	pos := 60 + len(v.Name)
	binary.BigEndian.PutUint32(out[pos:pos+4], uint32(len(v.Event)))
	pos += 4
	copy(out[pos:], v.Event)
	pos += len(v.Event)
	binary.BigEndian.PutUint32(out[pos:pos+4], uint32(len(v.Field)))
	pos += 4
	copy(out[pos:], v.Field)
	pos += len(v.Field)
	binary.BigEndian.PutUint32(out[pos:pos+4], v.Index)
	pos += 4
	encodeTransferPartsInto(out[pos:], v.Parts)
	pos += partsSize
	encodeTransferFieldsInto(out[pos:], v.Fields)
	return out
}

func DecodeTransferBegin(payload []byte) (TransferBegin, error) {
	if len(payload) < 72 {
		return TransferBegin{}, errors.New("transfer begin payload too small")
	}
	nameLen := binary.BigEndian.Uint32(payload[56:60])
	nameEnd, ok := checkedPayloadEnd(payload, 60, nameLen)
	if !ok {
		return TransferBegin{}, errors.New("invalid transfer name length")
	}
	var checksum [32]byte
	copy(checksum[:], payload[24:56])
	pos := nameEnd
	event := ""
	fieldName := ""
	index := uint32(0)
	parts := []TransferPart(nil)
	fields := []TransferField(nil)
	if pos+4 > len(payload) {
		return TransferBegin{}, errors.New("invalid transfer event length")
	}
	eventLen := binary.BigEndian.Uint32(payload[pos : pos+4])
	pos += 4
	eventEnd, ok := checkedPayloadEnd(payload, pos, eventLen)
	if !ok {
		return TransferBegin{}, errors.New("invalid transfer event length")
	}
	event = string(payload[pos:eventEnd])
	pos = eventEnd
	if pos+4 > len(payload) {
		return TransferBegin{}, errors.New("invalid transfer field name length")
	}
	fieldNameLen := binary.BigEndian.Uint32(payload[pos : pos+4])
	pos += 4
	fieldNameEnd, ok := checkedPayloadEnd(payload, pos, fieldNameLen)
	if !ok {
		return TransferBegin{}, errors.New("invalid transfer field name length")
	}
	fieldName = string(payload[pos:fieldNameEnd])
	pos = fieldNameEnd
	if pos+4 > len(payload) {
		return TransferBegin{}, errors.New("invalid transfer field index")
	}
	index = binary.BigEndian.Uint32(payload[pos : pos+4])
	pos += 4
	var partsBytes int
	var err error
	parts, partsBytes, err = decodeTransferParts(payload[pos:])
	if err != nil {
		return TransferBegin{}, err
	}
	pos += partsBytes
	fields, err = decodeTransferFields(payload[pos:])
	if err != nil {
		return TransferBegin{}, err
	}
	return TransferBegin{
		TotalSize:   binary.BigEndian.Uint64(payload[0:8]),
		ChunkSize:   binary.BigEndian.Uint32(payload[8:12]),
		ChunkCount:  binary.BigEndian.Uint32(payload[12:16]),
		ContentType: binary.BigEndian.Uint32(payload[16:20]),
		Flags:       binary.BigEndian.Uint32(payload[20:24]),
		Checksum:    checksum,
		Name:        string(payload[60:nameEnd]),
		Event:       event,
		Field:       fieldName,
		Index:       index,
		Parts:       parts,
		Fields:      fields,
	}, nil
}

func transferPartsPayloadSize(parts []TransferPart) int {
	return saturatedInt(transferPartsPayloadSize64(parts))
}

func transferPartsPayloadSize64(parts []TransferPart) uint64 {
	size := uint64(4)
	for _, part := range parts {
		size += 24 + uint64(len(part.Field)) + uint64(len(part.Name))
	}
	return size
}

func transferBeginPayloadSize64(name string, event string, field string, parts []TransferPart, fields []TransferField) uint64 {
	return 72 + uint64(len(name)) + uint64(len(event)) + uint64(len(field)) + transferPartsPayloadSize64(parts) + transferFieldsPayloadSize64(fields)
}

func encodeTransferPartsInto(dst []byte, parts []TransferPart) {
	binary.BigEndian.PutUint32(dst[0:4], uint32(len(parts)))
	pos := 4
	for _, part := range parts {
		field := []byte(part.Field)
		name := []byte(part.Name)
		binary.BigEndian.PutUint32(dst[pos:pos+4], uint32(len(field)))
		pos += 4
		copy(dst[pos:], field)
		pos += len(field)
		binary.BigEndian.PutUint32(dst[pos:pos+4], part.Index)
		pos += 4
		binary.BigEndian.PutUint32(dst[pos:pos+4], uint32(len(name)))
		pos += 4
		copy(dst[pos:], name)
		pos += len(name)
		binary.BigEndian.PutUint64(dst[pos:pos+8], part.TotalSize)
		pos += 8
		binary.BigEndian.PutUint32(dst[pos:pos+4], part.ContentType)
		pos += 4
	}
}

func decodeTransferParts(payload []byte) ([]TransferPart, int, error) {
	if len(payload) < 4 {
		return nil, 0, errors.New("invalid transfer parts length")
	}
	partCount := binary.BigEndian.Uint32(payload[0:4])
	if partCount > MaxTransferParts || uint64(partCount)*24 > uint64(len(payload)-4) {
		return nil, 0, errors.New("invalid transfer part count")
	}
	pos := 4
	parts := make([]TransferPart, 0, partCount)
	for i := uint32(0); i < partCount; i++ {
		if pos+4 > len(payload) {
			return nil, 0, errors.New("invalid transfer part field length")
		}
		fieldLen := binary.BigEndian.Uint32(payload[pos : pos+4])
		pos += 4
		fieldEnd, ok := checkedPayloadEnd(payload, pos, fieldLen)
		if !ok {
			return nil, 0, errors.New("invalid transfer part field length")
		}
		field := string(payload[pos:fieldEnd])
		pos = fieldEnd
		if pos+4 > len(payload) {
			return nil, 0, errors.New("invalid transfer part index")
		}
		index := binary.BigEndian.Uint32(payload[pos : pos+4])
		pos += 4
		if pos+4 > len(payload) {
			return nil, 0, errors.New("invalid transfer part name length")
		}
		nameLen := binary.BigEndian.Uint32(payload[pos : pos+4])
		pos += 4
		nameEnd, ok := checkedPayloadEnd(payload, pos, nameLen)
		if !ok {
			return nil, 0, errors.New("invalid transfer part name length")
		}
		name := string(payload[pos:nameEnd])
		pos = nameEnd
		if pos+12 > len(payload) {
			return nil, 0, errors.New("invalid transfer part metadata")
		}
		totalSize := binary.BigEndian.Uint64(payload[pos : pos+8])
		pos += 8
		contentType := binary.BigEndian.Uint32(payload[pos : pos+4])
		pos += 4
		parts = append(parts, TransferPart{
			Field:       field,
			Index:       index,
			Name:        name,
			TotalSize:   totalSize,
			ContentType: contentType,
		})
	}
	return parts, pos, nil
}

const (
	TransferFlagChecksumSHA256 uint32 = 1 << 0
)

type Ack struct {
	TransferID    uint64
	ChunkFrom     uint32
	ChunkTo       uint32
	ReceivedBytes uint64
}

type Nack struct {
	TransferID uint64
	ChunkFrom  uint32
	ChunkTo    uint32
	ReasonCode uint16
	Flags      uint16
}

const (
	NackMissingChunk     uint16 = 1
	NackInvalidChunk     uint16 = 2
	NackTransferUnknown  uint16 = 3
	NackTransferCanceled uint16 = 4
	NackWriteFailed      uint16 = 5
	NackProtocolError    uint16 = 6
	NackFlowControl      uint16 = 7
)

func EncodeNack(v Nack) []byte {
	out := make([]byte, 20)
	binary.BigEndian.PutUint64(out[0:8], v.TransferID)
	binary.BigEndian.PutUint32(out[8:12], v.ChunkFrom)
	binary.BigEndian.PutUint32(out[12:16], v.ChunkTo)
	binary.BigEndian.PutUint16(out[16:18], v.ReasonCode)
	binary.BigEndian.PutUint16(out[18:20], v.Flags)
	return out
}

func DecodeNack(payload []byte) (Nack, error) {
	if len(payload) != 20 {
		return Nack{}, errors.New("invalid nack payload length")
	}
	reason := binary.BigEndian.Uint16(payload[16:18])
	if reason < NackMissingChunk || reason > NackFlowControl || binary.BigEndian.Uint16(payload[18:20]) != 0 {
		return Nack{}, errors.New("invalid nack reason or flags")
	}
	return Nack{
		TransferID: binary.BigEndian.Uint64(payload[0:8]),
		ChunkFrom:  binary.BigEndian.Uint32(payload[8:12]),
		ChunkTo:    binary.BigEndian.Uint32(payload[12:16]),
		ReasonCode: reason,
	}, nil
}

const (
	CancelUser     uint16 = 1
	CancelTimeout  uint16 = 2
	CancelNetwork  uint16 = 3
	CancelRejected uint16 = 4
	CancelProtocol uint16 = 5
)

const (
	CancelDeletePartial uint16 = 1 << 0
	CancelSilent        uint16 = 1 << 1
)

const (
	CancelAckOK        uint8 = 1
	CancelAckNotFound  uint8 = 2
	CancelAckCompleted uint8 = 3
)

type Cancel struct {
	TransferID uint64
	ReasonCode uint16
	Flags      uint16
}

func EncodeCancel(v Cancel) []byte {
	out := make([]byte, 12)
	binary.BigEndian.PutUint64(out[0:8], v.TransferID)
	binary.BigEndian.PutUint16(out[8:10], v.ReasonCode)
	binary.BigEndian.PutUint16(out[10:12], v.Flags)
	return out
}

func DecodeCancel(payload []byte) (Cancel, error) {
	if len(payload) != 12 {
		return Cancel{}, errors.New("invalid cancel payload length")
	}
	reason := binary.BigEndian.Uint16(payload[8:10])
	flags := binary.BigEndian.Uint16(payload[10:12])
	if reason < CancelUser || reason > CancelProtocol || flags&^uint16(CancelDeletePartial|CancelSilent) != 0 {
		return Cancel{}, errors.New("invalid cancel reason or flags")
	}
	return Cancel{
		TransferID: binary.BigEndian.Uint64(payload[0:8]),
		ReasonCode: reason,
		Flags:      flags,
	}, nil
}

func EncodeCancelAck(transferID uint64, status uint8) []byte {
	out := make([]byte, 16)
	binary.BigEndian.PutUint64(out[0:8], transferID)
	out[8] = status
	return out
}

func DecodeCancelAck(payload []byte) (uint64, uint8, error) {
	if len(payload) != 16 {
		return 0, 0, errors.New("invalid cancel ack payload length")
	}
	if payload[8] < CancelAckOK || payload[8] > CancelAckCompleted || !zeroBytes(payload[9:16]) {
		return 0, 0, errors.New("invalid cancel ack status or reserved bytes")
	}
	return binary.BigEndian.Uint64(payload[0:8]), payload[8], nil
}

func EncodeAck(v Ack) []byte {
	out := make([]byte, 24)
	binary.BigEndian.PutUint64(out[0:8], v.TransferID)
	binary.BigEndian.PutUint32(out[8:12], v.ChunkFrom)
	binary.BigEndian.PutUint32(out[12:16], v.ChunkTo)
	binary.BigEndian.PutUint64(out[16:24], v.ReceivedBytes)
	return out
}

func DecodeAck(payload []byte) (Ack, error) {
	if len(payload) != 24 {
		return Ack{}, errors.New("invalid ack payload length")
	}
	return Ack{
		TransferID:    binary.BigEndian.Uint64(payload[0:8]),
		ChunkFrom:     binary.BigEndian.Uint32(payload[8:12]),
		ChunkTo:       binary.BigEndian.Uint32(payload[12:16]),
		ReceivedBytes: binary.BigEndian.Uint64(payload[16:24]),
	}, nil
}

func encodeString(s string) []byte {
	b := []byte(s)
	if len(b) > MaxFrameBytes-HeaderSize-4 {
		return nil
	}
	out := make([]byte, 4+len(b))
	binary.BigEndian.PutUint32(out[0:4], uint32(len(b)))
	copy(out[4:], b)
	return out
}

func decodeString(payload []byte) (string, error) {
	if len(payload) < 4 {
		return "", errors.New("string payload too small")
	}
	n := binary.BigEndian.Uint32(payload[0:4])
	end, ok := checkedPayloadEnd(payload, 4, n)
	if !ok || end != len(payload) {
		return "", errors.New("invalid string length")
	}
	return string(payload[4:end]), nil
}

func checkedPayloadEnd(payload []byte, start int, length uint32) (int, bool) {
	if start < 0 || start > len(payload) || uint64(length) > uint64(len(payload)-start) {
		return 0, false
	}
	return start + int(length), true
}

func saturatedInt(value uint64) int {
	maxInt := uint64(^uint(0) >> 1)
	if value > maxInt {
		return int(maxInt)
	}
	return int(value)
}

func zeroBytes(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}
