package etp_test

import (
	"context"
	"testing"

	etp "github.com/elum-utils/go-etp"
)

type resumeStore struct{}

func (resumeStore) ResumeIncoming(context.Context, etp.TransferResumeView) (etp.TransferResumeDecision, error) {
	return etp.TransferResumeDecision{}, nil
}

func TestPublicProtocolConfigurationTypes(t *testing.T) {
	config := etp.DefaultServerConfig()
	config.Auth.Handler = func(context.Context, etp.AuthRequest) (etp.AuthResult, error) {
		return etp.AuthResult{OK: true}, nil
	}
	config.Resume.Store = resumeStore{}
	config = etp.NormalizeSessionConfig(config)
	if config.Role != etp.RoleServer {
		t.Fatalf("role = %q", config.Role)
	}
}
