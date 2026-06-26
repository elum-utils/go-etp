package etp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)

const (
	ProtocolName = "elum-protocol"
	WireVersion  = uint8(1)
	HeaderSize   = 40
)

const (
	FrameData uint8 = 1

	FrameAck  uint8 = 2
	FrameNack uint8 = 3

	FramePing uint8 = 4
	FramePong uint8 = 5

	FrameWindow uint8 = 6

	FrameCancel    uint8 = 7
	FrameCancelAck uint8 = 8

	FrameHello    uint8 = 9
	FrameHelloAck uint8 = 10

	FrameClose uint8 = 11

	FrameTransferBegin uint8 = 12
	FrameTransferEnd   uint8 = 13
	FrameTransferState uint8 = 14

	FrameAuth       uint8 = 15
	FrameAuthAccept uint8 = 16
	FrameAuthReject uint8 = 17

	FrameRequest  uint8 = 18
	FrameResponse uint8 = 19

	FrameError    uint8 = 20
	FrameGoAway   uint8 = 21
	FrameCloseAck uint8 = 22

	FrameTransferResume uint8 = 23
)

const (
	FlagFirst      uint16 = 1 << 0
	FlagLast       uint16 = 1 << 1
	FlagAckRequest uint16 = 1 << 2
	FlagEncrypted  uint16 = 1 << 3
	FlagCompressed uint16 = 1 << 4
	FlagControl    uint16 = 1 << 5
)

const (
	PriorityCritical uint8 = 0
	PriorityHigh     uint8 = 1
	PriorityNormal   uint8 = 2
	PriorityLow      uint8 = 3
	PriorityIdle     uint8 = 4
)

const (
	ChannelControl    uint16 = 0
	ChannelRealtime   uint16 = 1
	ChannelBulk       uint16 = 2
	ChannelSync       uint16 = 3
	ChannelBackground uint16 = 4
)

const (
	SchemaHello         uint32 = 1
	SchemaTextMessage   uint32 = 100
	SchemaTransferBegin uint32 = 200
	SchemaAck           uint32 = 201
	SchemaCancel        uint32 = 202
	SchemaNack          uint32 = 203
	SchemaAuth          uint32 = 204
	SchemaAuthResult    uint32 = 205
	SchemaEvent         uint32 = 300
	SchemaError         uint32 = 400
	SchemaGoAway        uint32 = 401
	SchemaClose         uint32 = 402
	SchemaWindow        uint32 = 500
	SchemaTransferState uint32 = 501
)

const (
	ContentFile  uint32 = 1
	ContentMedia uint32 = 2
)

type Header struct {
	Version       uint8
	FrameType     uint8
	Flags         uint16
	Priority      uint8
	HeaderLength  uint8
	ChannelID     uint16
	PayloadOffset uint16
	HeaderFlags   uint16
	PayloadLength uint32
	SchemaID      uint32
	RequestID     uint64
	TransferID    uint64
	ChunkID       uint32
}

type Frame struct {
	Header  Header
	Payload []byte
}

var frameBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, HeaderSize+DefaultChunkSize)
		return &buf
	},
}

var chunkBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, DefaultChunkSize)
		return &buf
	},
}

func NewFrame(frameType uint8, schemaID uint32, payload []byte) Frame {
	return Frame{
		Header: Header{
			Version:       WireVersion,
			FrameType:     frameType,
			HeaderLength:  HeaderSize,
			PayloadOffset: HeaderSize,
			PayloadLength: uint32(len(payload)),
			SchemaID:      schemaID,
		},
		Payload: payload,
	}
}

func getChunkBuffer(size int) *[]byte {
	bufp := chunkBufferPool.Get().(*[]byte)
	if cap(*bufp) < size {
		buf := make([]byte, size)
		return &buf
	}
	*bufp = (*bufp)[:size]
	return bufp
}

func putChunkBuffer(bufp *[]byte) {
	if bufp == nil {
		return
	}
	if cap(*bufp) > MaxFrameBytes {
		return
	}
	*bufp = (*bufp)[:cap(*bufp)]
	chunkBufferPool.Put(bufp)
}

func EncodedFrameSize(frame Frame) int {
	return HeaderSize + len(frame.Payload)
}

func EncodeFrame(frame Frame) ([]byte, error) {
	return EncodeFrameInto(nil, frame)
}

func EncodeFrameInto(dst []byte, frame Frame) ([]byte, error) {
	if len(frame.Payload) > int(^uint32(0)) {
		return nil, errors.New("payload too large")
	}
	frame.Header.Version = WireVersion
	frame.Header.HeaderLength = HeaderSize
	frame.Header.PayloadOffset = HeaderSize
	frame.Header.PayloadLength = uint32(len(frame.Payload))

	size := EncodedFrameSize(frame)
	if cap(dst) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}
	out := dst
	h := frame.Header
	out[0] = h.Version
	out[1] = h.FrameType
	binary.BigEndian.PutUint16(out[2:4], h.Flags)
	out[4] = h.Priority
	out[5] = h.HeaderLength
	binary.BigEndian.PutUint16(out[6:8], h.ChannelID)
	binary.BigEndian.PutUint16(out[8:10], h.PayloadOffset)
	binary.BigEndian.PutUint16(out[10:12], h.HeaderFlags)
	binary.BigEndian.PutUint32(out[12:16], h.PayloadLength)
	binary.BigEndian.PutUint32(out[16:20], h.SchemaID)
	binary.BigEndian.PutUint64(out[20:28], h.RequestID)
	binary.BigEndian.PutUint64(out[28:36], h.TransferID)
	binary.BigEndian.PutUint32(out[36:40], h.ChunkID)
	copy(out[HeaderSize:], frame.Payload)
	return out, nil
}

func encodeFrameHeaderInto(out []byte, h Header, payloadLength int) {
	h.Version = WireVersion
	h.HeaderLength = HeaderSize
	h.PayloadOffset = HeaderSize
	h.PayloadLength = uint32(payloadLength)
	out[0] = h.Version
	out[1] = h.FrameType
	binary.BigEndian.PutUint16(out[2:4], h.Flags)
	out[4] = h.Priority
	out[5] = h.HeaderLength
	binary.BigEndian.PutUint16(out[6:8], h.ChannelID)
	binary.BigEndian.PutUint16(out[8:10], h.PayloadOffset)
	binary.BigEndian.PutUint16(out[10:12], h.HeaderFlags)
	binary.BigEndian.PutUint32(out[12:16], h.PayloadLength)
	binary.BigEndian.PutUint32(out[16:20], h.SchemaID)
	binary.BigEndian.PutUint64(out[20:28], h.RequestID)
	binary.BigEndian.PutUint64(out[28:36], h.TransferID)
	binary.BigEndian.PutUint32(out[36:40], h.ChunkID)
}

func getFrameBuffer(size int) *[]byte {
	bufp := frameBufferPool.Get().(*[]byte)
	if cap(*bufp) < size {
		*bufp = make([]byte, 0, size)
	}
	return bufp
}

func putFrameBuffer(bufp *[]byte) {
	*bufp = (*bufp)[:0]
	frameBufferPool.Put(bufp)
}

func DecodeFrame(data []byte) (Frame, error) {
	return DecodeFrameView(data)
}

func DecodeHeader(data []byte) (Header, error) {
	if len(data) < HeaderSize {
		return Header{}, fmt.Errorf("frame header too small: %d", len(data))
	}
	h := Header{
		Version:       data[0],
		FrameType:     data[1],
		Flags:         binary.BigEndian.Uint16(data[2:4]),
		Priority:      data[4],
		HeaderLength:  data[5],
		ChannelID:     binary.BigEndian.Uint16(data[6:8]),
		PayloadOffset: binary.BigEndian.Uint16(data[8:10]),
		HeaderFlags:   binary.BigEndian.Uint16(data[10:12]),
		PayloadLength: binary.BigEndian.Uint32(data[12:16]),
		SchemaID:      binary.BigEndian.Uint32(data[16:20]),
		RequestID:     binary.BigEndian.Uint64(data[20:28]),
		TransferID:    binary.BigEndian.Uint64(data[28:36]),
		ChunkID:       binary.BigEndian.Uint32(data[36:40]),
	}
	if h.Version != WireVersion {
		return Header{}, fmt.Errorf("unsupported wire version: %d", h.Version)
	}
	if h.HeaderLength != HeaderSize || h.PayloadOffset != HeaderSize {
		return Header{}, errors.New("invalid fixed header length or payload offset")
	}
	return h, nil
}

func DecodeFrameView(data []byte) (Frame, error) {
	h, err := DecodeHeader(data)
	if err != nil {
		return Frame{}, err
	}
	end := int(h.PayloadOffset) + int(h.PayloadLength)
	if end > len(data) {
		return Frame{}, errors.New("payload exceeds frame length")
	}
	if end != len(data) {
		return Frame{}, errors.New("frame has trailing bytes")
	}
	return Frame{Header: h, Payload: data[h.PayloadOffset:end]}, nil
}

func DecodeFrameCopy(data []byte) (Frame, error) {
	frame, err := DecodeFrameView(data)
	if err != nil {
		return Frame{}, err
	}
	payload := make([]byte, len(frame.Payload))
	copy(payload, frame.Payload)
	frame.Payload = payload
	return frame, nil
}
