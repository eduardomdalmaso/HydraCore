package ingest

import (
	"context"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"hydrastream/internal/domain"
	"hydrastream/internal/ports"
)

type ingestSession struct {
	streamID           string
	sourceURL          string
	cancel             context.CancelFunc
	status             string
	rtpPacketsTotal    uint64
	rtpLossTotal       uint64
	framesTotal        uint64
	framesDecodedTotal uint64
	bytesTotal         uint64
	startedAt          time.Time
	firstFrameAt       time.Time
	ttffMs             float64
	lastFrameTime      time.Time
	mu                 sync.RWMutex
	conn               net.Conn
	err                error
}

// StreamEventPublisher defines the callback to publish CloudEvents to NATS.
type StreamEventPublisher interface {
	PublishCameraOffline(ctx context.Context, tenantID, streamID, errorMsg string) error
	PublishCameraOnline(ctx context.Context, tenantID, streamID string) error
}

// RTSPIngestor manages concurrent RTSP stream ingestion sessions.
type RTSPIngestor struct {
	mu        sync.RWMutex
	sessions  map[string]*ingestSession
	publisher StreamEventPublisher
}

// NewRTSPIngestor creates a new RTSPIngestor adapter.
func NewRTSPIngestor() *RTSPIngestor {
	return &RTSPIngestor{
		sessions: make(map[string]*ingestSession),
	}
}

// SetPublisher configures the event publisher for instant status emission.
func (r *RTSPIngestor) SetPublisher(pub StreamEventPublisher) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.publisher = pub
}

// StartIngest starts a background worker ingesting from the stream's source URL.
func (r *RTSPIngestor) StartIngest(ctx context.Context, stream *domain.Stream) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.sessions[stream.StreamID]; exists {
		return nil
	}

	sessionCtx, cancel := context.WithCancel(context.Background())
	now := time.Now()
	sess := &ingestSession{
		streamID:      stream.StreamID,
		sourceURL:     stream.SourceURL,
		cancel:        cancel,
		status:        "connecting",
		startedAt:     now,
		lastFrameTime: now,
	}
	r.sessions[stream.StreamID] = sess

	go r.runWorker(sessionCtx, sess, stream)
	return nil
}

// StopIngest cancels and removes an active stream ingestion session.
func (r *RTSPIngestor) StopIngest(ctx context.Context, streamID string) error {
	r.mu.Lock()
	sess, ok := r.sessions[streamID]
	if ok {
		delete(r.sessions, streamID)
	}
	r.mu.Unlock()

	if !ok {
		return domain.ErrStreamNotFound
	}

	sess.cancel()
	sess.mu.Lock()
	if sess.conn != nil {
		_ = sess.conn.Close()
	}
	sess.status = "stopped"
	sess.mu.Unlock()

	return nil
}

// GetIngestStats queries real-time performance and metrics for a specific stream.
func (r *RTSPIngestor) GetIngestStats(ctx context.Context, streamID string) (*domain.IngestStats, error) {
	r.mu.RLock()
	sess, ok := r.sessions[streamID]
	r.mu.RUnlock()

	if !ok {
		return nil, domain.ErrStreamNotFound
	}

	sess.mu.RLock()
	defer sess.mu.RUnlock()

	rtpPackets := atomic.LoadUint64(&sess.rtpPacketsTotal)
	rtpLoss := atomic.LoadUint64(&sess.rtpLossTotal)
	frames := atomic.LoadUint64(&sess.framesTotal)
	framesDecoded := atomic.LoadUint64(&sess.framesDecodedTotal)
	bytes := atomic.LoadUint64(&sess.bytesTotal)
	elapsed := time.Since(sess.startedAt).Seconds()
	sinceLast := time.Since(sess.lastFrameTime).Seconds()

	var fps, decodeFPS, bitrateKbps float64
	if elapsed > 0 && (frames > 0 || rtpPackets > 0) {
		if sinceLast <= 5.0 || sess.status == "streaming" {
			fps = float64(frames) / elapsed
			decodeFPS = float64(framesDecoded) / elapsed
		}
		bitrateKbps = (float64(bytes) * 8.0 / 1024.0) / elapsed
	}

	var errMsg string
	if sess.err != nil {
		errMsg = sess.err.Error()
	}

	return &domain.IngestStats{
		StreamID:           sess.streamID,
		Status:             sess.status,
		IngestFPS:          math.Round(fps*100) / 100,
		DecodeFPS:          math.Round(decodeFPS*100) / 100,
		BitrateKbps:        math.Round(bitrateKbps*100) / 100,
		RTPPacketsTotal:    rtpPackets,
		RTPBytesTotal:      bytes,
		RTPLossTotal:       rtpLoss,
		FramesTotal:        frames,
		FramesDecodedTotal: framesDecoded,
		BytesTotal:         bytes,
		StartedAt:          sess.startedAt,
		FirstFrameAt:       sess.firstFrameAt,
		TTFFMs:             sess.ttffMs,
		LastFrameTime:      sess.lastFrameTime,
		ErrorMsg:           errMsg,
	}, nil
}

// ListActiveIngests returns all active ingestion sessions.
func (r *RTSPIngestor) ListActiveIngests(ctx context.Context) ([]*domain.IngestStats, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var list []*domain.IngestStats
	for id := range r.sessions {
		if stat, err := r.GetIngestStats(ctx, id); err == nil {
			list = append(list, stat)
		}
	}
	return list, nil
}

var _ ports.StreamIngestor = (*RTSPIngestor)(nil)
