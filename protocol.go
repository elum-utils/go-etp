package etp

import (
	"net"

	protocol "github.com/elum-utils/go-etp/internal/etp"
)

const (
	RoleClient          = protocol.RoleClient
	RoleServer          = protocol.RoleServer
	DefaultChunkSize    = protocol.DefaultChunkSize
	HeaderSize          = protocol.HeaderSize
	MaxFrameBytes       = protocol.MaxFrameBytes
	MaxPooledFrameBytes = protocol.MaxPooledFrameBytes
	WireVersion         = protocol.WireVersion

	FrameData     = protocol.FrameData
	FrameRequest  = protocol.FrameRequest
	FrameResponse = protocol.FrameResponse
	FrameHello    = protocol.FrameHello
	FrameHelloAck = protocol.FrameHelloAck

	FlagFirst   = protocol.FlagFirst
	FlagLast    = protocol.FlagLast
	FlagControl = protocol.FlagControl

	SchemaTextMessage = protocol.SchemaTextMessage
	SchemaEvent       = protocol.SchemaEvent
	SchemaHello       = protocol.SchemaHello

	PriorityCritical = protocol.PriorityCritical
	ChannelControl   = protocol.ChannelControl

	ContentFile  = protocol.ContentFile
	ContentMedia = protocol.ContentMedia

	DefaultCapabilities = protocol.DefaultCapabilities
	AllCapabilities     = protocol.AllCapabilities

	CapabilityTransfers       = protocol.CapabilityTransfers
	CapabilityCancel          = protocol.CapabilityCancel
	CapabilityAck             = protocol.CapabilityAck
	CapabilityNack            = protocol.CapabilityNack
	CapabilityHeartbeat       = protocol.CapabilityHeartbeat
	CapabilityTransferSHA256  = protocol.CapabilityTransferSHA256
	CapabilityFlowControl     = protocol.CapabilityFlowControl
	CapabilitySlowlorisGuard  = protocol.CapabilitySlowlorisGuard
	CapabilityProtocolEvents  = protocol.CapabilityProtocolEvents
	CapabilityRequestResponse = protocol.CapabilityRequestResponse
	CapabilityGracefulClose   = protocol.CapabilityGracefulClose
	CapabilityTransferResume  = protocol.CapabilityTransferResume
	CapabilityTransferCommit  = protocol.CapabilityTransferCommit

	ChecksumOff            = protocol.ChecksumOff
	ChecksumTransferSHA256 = protocol.ChecksumTransferSHA256

	SessionNew         = protocol.SessionNew
	SessionEstablished = protocol.SessionEstablished
	SessionClosed      = protocol.SessionClosed
	SessionFailed      = protocol.SessionFailed

	EventProtocolViolation = protocol.EventProtocolViolation
	EventSlowloris         = protocol.EventSlowloris
	EventTransferFailed    = protocol.EventTransferFailed
	EventTransferUnknown   = protocol.EventTransferUnknown
	EventTransferCanceled  = protocol.EventTransferCanceled
	EventWriteFailed       = protocol.EventWriteFailed
	EventNackReceived      = protocol.EventNackReceived
	EventAckTimeout        = protocol.EventAckTimeout
	EventAuthAccepted      = protocol.EventAuthAccepted
	EventAuthRejected      = protocol.EventAuthRejected
	EventAuthTimeout       = protocol.EventAuthTimeout
	EventAuthRequired      = protocol.EventAuthRequired
	EventErrorReceived     = protocol.EventErrorReceived
	EventGoAwayReceived    = protocol.EventGoAwayReceived
	EventCloseAckReceived  = protocol.EventCloseAckReceived
	EventHandlerFailed     = protocol.EventHandlerFailed
	EventRateLimited       = protocol.EventRateLimited
	EventChecksumMismatch  = protocol.EventChecksumMismatch
)

type (
	Session                       = protocol.Session
	SessionState                  = protocol.SessionState
	SessionConfig                 = protocol.SessionConfig
	FlowControlConfig             = protocol.FlowControlConfig
	HeartbeatConfig               = protocol.HeartbeatConfig
	AuthConfig                    = protocol.AuthConfig
	ReceiveConfig                 = protocol.ReceiveConfig
	RateLimitConfig               = protocol.RateLimitConfig
	ResumeConfig                  = protocol.ResumeConfig
	SchedulerConfig               = protocol.SchedulerConfig
	PayloadLimitConfig            = protocol.PayloadLimitConfig
	SendQueueConfig               = protocol.SendQueueConfig
	HandlerConfig                 = protocol.HandlerConfig
	CloseConfig                   = protocol.CloseConfig
	AuthHandler                   = protocol.AuthHandler
	AuthRequest                   = protocol.AuthRequest
	AuthAccept                    = protocol.AuthAccept
	AuthReject                    = protocol.AuthReject
	AuthResult                    = protocol.AuthResult
	AuthAttribute                 = protocol.AuthAttribute
	SessionIdentity               = protocol.SessionIdentity
	ProtocolEvent                 = protocol.ProtocolEvent
	Progress                      = protocol.Progress
	ChecksumMode                  = protocol.ChecksumMode
	MessageOptions                = protocol.MessageOptions
	TransferOptions               = protocol.TransferOptions
	TransferHandle                = protocol.TransferHandle
	ResumeTransferOptions         = protocol.ResumeTransferOptions
	TransferField                 = protocol.TransferField
	TransferPart                  = protocol.TransferPart
	TransferBegin                 = protocol.TransferBegin
	TransferResume                = protocol.TransferResume
	TransferResumeView            = protocol.TransferResumeView
	TransferResumeDecision        = protocol.TransferResumeDecision
	TransferResumeStore           = protocol.TransferResumeStore
	TransferState                 = protocol.TransferState
	TransferStateMessage          = protocol.TransferStateMessage
	Window                        = protocol.Window
	Ack                           = protocol.Ack
	Nack                          = protocol.Nack
	Cancel                        = protocol.Cancel
	CloseMessage                  = protocol.CloseMessage
	GoAway                        = protocol.GoAway
	ErrorMessage                  = protocol.ErrorMessage
	Frame                         = protocol.Frame
	Header                        = protocol.Header
	FrameLease                    = protocol.FrameLease
	FrameLeaseReleaser            = protocol.FrameLeaseReleaser
	FrameTransport                = protocol.FrameTransport
	LeasedFrameTransport          = protocol.LeasedFrameTransport
	NegotiatedFrameTransport      = protocol.NegotiatedFrameTransport
	FrameLimitTransport           = protocol.FrameLimitTransport
	DeadlineStream                = protocol.DeadlineStream
	StreamTransport               = protocol.StreamTransport
	SlowlorisConfig               = protocol.SlowlorisConfig
	MultiStreamTransport          = protocol.MultiStreamTransport
	MultiStreamTransportConfig    = protocol.MultiStreamTransportConfig
	EventMessage                  = protocol.EventMessage
	EventMessageView              = protocol.EventMessageView
	Hello                         = protocol.Hello
	IncomingTransferInfo          = protocol.IncomingTransferInfo
	IncomingTransferWriter        = protocol.IncomingTransferWriter
	IncomingTransferContextWriter = protocol.IncomingTransferContextWriter
	IncomingTransferContextCloser = protocol.IncomingTransferContextCloser
	IncomingTransferAborter       = protocol.IncomingTransferAborter
	IncomingTransferSuspender     = protocol.IncomingTransferSuspender
	TextHandler                   = protocol.TextHandler
	RequestHandler                = protocol.RequestHandler
	ResponseHandler               = protocol.ResponseHandler
	TransferHandler               = protocol.TransferHandler
)

func NewSession(t FrameTransport) *Session {
	return protocol.NewSession(t)
}

func NewSessionWithConfig(t FrameTransport, config SessionConfig) *Session {
	return protocol.NewSessionWithConfig(t, config)
}

func DefaultClientConfig() SessionConfig {
	return protocol.DefaultClientConfig()
}

func DefaultServerConfig() SessionConfig {
	return protocol.DefaultServerConfig()
}

func DefaultSessionConfig(role string) SessionConfig {
	return protocol.DefaultSessionConfig(role)
}

func NormalizeSessionConfig(config SessionConfig) SessionConfig {
	return protocol.NormalizeSessionConfig(config)
}

func DefaultSlowlorisConfig() SlowlorisConfig {
	return protocol.DefaultSlowlorisConfig()
}

func NewStreamTransport(conn net.Conn) *StreamTransport {
	return protocol.NewStreamTransport(conn)
}

func NewStreamTransportWithSlowlorisGuard(conn net.Conn, guard SlowlorisConfig) *StreamTransport {
	return protocol.NewStreamTransportWithSlowlorisGuard(conn, guard)
}

func NewStreamTransportForStream(stream DeadlineStream, guard SlowlorisConfig) *StreamTransport {
	return protocol.NewStreamTransportForStream(stream, guard)
}

func NewMultiStreamTransport(config MultiStreamTransportConfig) *MultiStreamTransport {
	return protocol.NewMultiStreamTransport(config)
}

func NewFrame(frameType uint8, schemaID uint32, payload []byte) Frame {
	return protocol.NewFrame(frameType, schemaID, payload)
}

func NewFrameLease(data []byte, release func([]byte)) *FrameLease {
	return protocol.NewFrameLease(data, release)
}

func InitFrameLease(lease *FrameLease, data []byte, releaser FrameLeaseReleaser) *FrameLease {
	return protocol.InitFrameLease(lease, data, releaser)
}

func DecodeHeader(data []byte) (Header, error) {
	return protocol.DecodeHeader(data)
}

func DecodeFrameView(data []byte) (Frame, error) {
	return protocol.DecodeFrameView(data)
}

func EncodeFrame(frame Frame) ([]byte, error) {
	return protocol.EncodeFrame(frame)
}

func EncodeFrameInto(dst []byte, frame Frame) ([]byte, error) {
	return protocol.EncodeFrameInto(dst, frame)
}

func DecodeEventMessageView(payload []byte) (EventMessageView, error) {
	return protocol.DecodeEventMessageView(payload)
}

func EncodeEventMessageStringWithFieldsInto(dst []byte, event string, data []byte, fields []TransferField) ([]byte, error) {
	return protocol.EncodeEventMessageStringWithFieldsInto(dst, event, data, fields)
}

func EncodeHelloMessage(hello Hello) []byte {
	return protocol.EncodeHelloMessage(hello)
}
