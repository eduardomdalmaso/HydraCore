package ports

import (
	"context"

	"hydrastream/internal/domain"
)

// WebRTCGateway defines the driven port for native WebRTC WHEP media negotiation and session lifecycle.
type WebRTCGateway interface {
	NegotiateWHEP(ctx context.Context, stream *domain.Stream, sdpOffer string, onDisconnect func(sessionID string)) (sessionID, sdpAnswer string, err error)
	CloseSession(sessionID string) error
	CloseAll()
}
