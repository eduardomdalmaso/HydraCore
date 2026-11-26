package webrtc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"hydrastream/internal/domain"
	"hydrastream/internal/ports"
)

type activeSession struct {
	pc     *webrtc.PeerConnection
	cancel context.CancelFunc
}

// PionWHEPGateway implements native WebRTC WHEP egress using Pion WebRTC v4 in pure pass-through mode.
type PionWHEPGateway struct {
	rtpSource ports.RTPStreamSource
	mu        sync.Mutex
	sessions  map[string]*activeSession
}

// NewPionWHEPGateway creates a native WebRTC WHEP gateway backed by an RTP stream source.
func NewPionWHEPGateway(rtpSource ports.RTPStreamSource) *PionWHEPGateway {
	return &PionWHEPGateway{
		rtpSource: rtpSource,
		sessions:  make(map[string]*activeSession),
	}
}

func isH265Codec(codec string) bool {
	c := strings.ToUpper(strings.TrimSpace(codec))
	return c == "H265" || c == "HEVC" || c == "H.265"
}

func randomID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// NegotiateWHEP creates a PeerConnection, attaches an H264 or H265 RTP track in pass-through, and returns an SDP answer.
func (g *PionWHEPGateway) NegotiateWHEP(
	ctx context.Context,
	stream *domain.Stream,
	sdpOffer string,
	onDisconnect func(sessionID string),
) (string, string, error) {
	streamID := ""
	mimeType := webrtc.MimeTypeH264
	if stream != nil {
		streamID = stream.StreamID
		if isH265Codec(stream.Codec) {
			mimeType = webrtc.MimeTypeH265
		}
	}

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return "", "", fmt.Errorf("create peer connection: %w", err)
	}

	videoTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: mimeType, ClockRate: 90000},
		"video",
		streamID,
	)
	if err != nil {
		_ = pc.Close()
		return "", "", fmt.Errorf("create video track: %w", err)
	}

	rtpSender, err := pc.AddTrack(videoTrack)
	if err != nil {
		_ = pc.Close()
		return "", "", fmt.Errorf("add video track: %w", err)
	}

	sessionID := randomID()
	sessCtx, cancel := context.WithCancel(context.Background())

	g.mu.Lock()
	g.sessions[sessionID] = &activeSession{pc: pc, cancel: cancel}
	g.mu.Unlock()

	go func() {
		rtcpBuf := make([]byte, 1500)
		for {
			if _, _, err := rtpSender.Read(rtcpBuf); err != nil {
				return
			}
		}
	}()

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateFailed ||
			state == webrtc.PeerConnectionStateClosed ||
			state == webrtc.PeerConnectionStateDisconnected {
			_ = g.CloseSession(sessionID)
			if onDisconnect != nil {
				onDisconnect(sessionID)
			}
		}
	})

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  sdpOffer,
	}); err != nil {
		_ = g.CloseSession(sessionID)
		return "", "", fmt.Errorf("set remote description: %w", err)
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		_ = g.CloseSession(sessionID)
		return "", "", fmt.Errorf("create answer: %w", err)
	}

	gatherComplete := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		_ = g.CloseSession(sessionID)
		return "", "", fmt.Errorf("set local description: %w", err)
	}

	select {
	case <-gatherComplete:
	case <-time.After(3 * time.Second):
	case <-ctx.Done():
		_ = g.CloseSession(sessionID)
		return "", "", ctx.Err()
	}

	localDesc := pc.LocalDescription()
	if localDesc == nil {
		_ = g.CloseSession(sessionID)
		return "", "", fmt.Errorf("empty local description")
	}

	if g.rtpSource != nil {
		go g.pumpRTP(sessCtx, streamID, videoTrack)
	}

	log.Printf("📡 [WebRTC WHEP] Pass-through session '%s' (%s) active for stream '%s'\n", sessionID, mimeType, streamID)
	return sessionID, localDesc.SDP, nil
}

func (g *PionWHEPGateway) pumpRTP(ctx context.Context, streamID string, track *webrtc.TrackLocalStaticRTP) {
	ch, unsub := g.rtpSource.SubscribeRTP(streamID)
	defer unsub()

	for {
		select {
		case <-ctx.Done():
			return
		case pkt, ok := <-ch:
			if !ok {
				return
			}
			if _, err := track.Write(pkt); err != nil {
				return
			}
		}
	}
}

// CloseSession closes an active WebRTC PeerConnection and stops its RTP pump.
func (g *PionWHEPGateway) CloseSession(sessionID string) error {
	g.mu.Lock()
	sess, ok := g.sessions[sessionID]
	if ok {
		delete(g.sessions, sessionID)
	}
	g.mu.Unlock()

	if !ok {
		return nil
	}
	sess.cancel()
	return sess.pc.Close()
}

// CloseAll terminates every active WebRTC session.
func (g *PionWHEPGateway) CloseAll() {
	g.mu.Lock()
	ids := make([]string, 0, len(g.sessions))
	for id := range g.sessions {
		ids = append(ids, id)
	}
	g.mu.Unlock()

	for _, id := range ids {
		_ = g.CloseSession(id)
	}
}

var _ ports.WebRTCGateway = (*PionWHEPGateway)(nil)
