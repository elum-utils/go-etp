package etp

type SessionState uint8

const (
	SessionNew SessionState = iota
	SessionAuthPending
	SessionAuthAccepted
	SessionAuthRejected
	SessionHelloSent
	SessionEstablished
	SessionDraining
	SessionClosing
	SessionClosed
	SessionFailed
)

func (s SessionState) String() string {
	switch s {
	case SessionNew:
		return "NEW"
	case SessionAuthPending:
		return "AUTH_PENDING"
	case SessionAuthAccepted:
		return "AUTH_ACCEPTED"
	case SessionAuthRejected:
		return "AUTH_REJECTED"
	case SessionHelloSent:
		return "HELLO_SENT"
	case SessionEstablished:
		return "ESTABLISHED"
	case SessionDraining:
		return "DRAINING"
	case SessionClosing:
		return "CLOSING"
	case SessionClosed:
		return "CLOSED"
	case SessionFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

type TransferState uint8

const (
	TransferInit TransferState = iota
	TransferSending
	TransferCanceling
	TransferCanceled
	TransferCompleted
	TransferFailed
	TransferAcked
)

func (s TransferState) String() string {
	switch s {
	case TransferInit:
		return "INIT"
	case TransferSending:
		return "SENDING"
	case TransferCanceling:
		return "CANCELING"
	case TransferCanceled:
		return "CANCELED"
	case TransferCompleted:
		return "COMPLETED"
	case TransferFailed:
		return "FAILED"
	case TransferAcked:
		return "ACKED"
	default:
		return "UNKNOWN"
	}
}

type ProtocolEvent struct {
	Code       uint16
	Message    string
	FrameType  uint8
	TransferID uint64
	ChunkID    uint32
}

const (
	EventProtocolViolation uint16 = 1
	EventSlowloris         uint16 = 2
	EventTransferFailed    uint16 = 3
	EventTransferUnknown   uint16 = 4
	EventTransferCanceled  uint16 = 5
	EventWriteFailed       uint16 = 6
	EventNackReceived      uint16 = 7
	EventAckTimeout        uint16 = 8
	EventAuthAccepted      uint16 = 9
	EventAuthRejected      uint16 = 10
	EventAuthTimeout       uint16 = 11
	EventAuthRequired      uint16 = 12
	EventErrorReceived     uint16 = 13
	EventGoAwayReceived    uint16 = 14
	EventCloseAckReceived  uint16 = 15
	EventHandlerFailed     uint16 = 16
	EventRateLimited       uint16 = 17
	EventChecksumMismatch  uint16 = 18
)
