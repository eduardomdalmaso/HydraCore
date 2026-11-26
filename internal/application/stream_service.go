package application

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"hydrastream/internal/adapters/secondary/logger"
	"hydrastream/internal/adapters/secondary/memory"
	"hydrastream/internal/domain"
	"hydrastream/internal/ports"
)

// StreamService is the application service handling stream use cases.
type StreamService struct {
	repo         ports.StreamRepository
	fragRepo     ports.FragmentRepository
	ingestor     ports.StreamIngestor
	onvif        ports.ONVIFDiscoverer
	logCol       ports.LogCollector
	recPublisher ports.RecordingEventPublisher
	balancer     *BalancerService
	webrtcGW     ports.WebRTCGateway
	startTime    time.Time
	mu           sync.Mutex
	history      []float64
	latHistory   []float64
	lastTick     time.Time
	whepMu          sync.RWMutex
	whepSessions    map[string]*domain.WHEPSession
	whepBaseURL     string
	consumerMu      sync.Mutex
	activeConsumers map[string]map[string]struct{}
	idleTimers      map[string]*time.Timer
	idleStopDelay   time.Duration
}

// NewStreamService creates a new StreamService application instance.
func NewStreamService(repo ports.StreamRepository, ingestor ports.StreamIngestor, onvif ports.ONVIFDiscoverer, logCollector ...ports.LogCollector) *StreamService {
	var logCol ports.LogCollector
	if len(logCollector) > 0 && logCollector[0] != nil {
		logCol = logCollector[0]
	} else {
		logCol = logger.NewRingLogger(1000)
	}

	whepURL := os.Getenv("MEDIAMTX_WHEP_URL")
	if whepURL == "" {
		whepURL = "http://localhost:8889"
	}

	s := &StreamService{
		repo:         repo,
		fragRepo:     memory.NewFragmentRepository("recordings"),
		ingestor:     ingestor,
		onvif:        onvif,
		logCol:       logCol,
		startTime:    time.Now().UTC(),
		history:      []float64{38.2, 44.5, 52.1, 48.0, 62.4, 58.9, 61.2},
		latHistory:   []float64{1.2, 1.4, 1.35, 1.42, 1.48, 1.39, 1.42},
		lastTick:     time.Now(),
		whepSessions:    make(map[string]*domain.WHEPSession),
		whepBaseURL:     strings.TrimSuffix(whepURL, "/"),
		activeConsumers: make(map[string]map[string]struct{}),
		idleTimers:      make(map[string]*time.Timer),
		idleStopDelay:   defaultIdleStopDelay,
	}

	go func() {
		ticker := time.NewTicker(2 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			s.pruneExpiredWHEPSessions()
		}
	}()

	return s
}

// SetWHEPBaseURL overrides the MediaMTX WebRTC URL.
func (s *StreamService) SetWHEPBaseURL(url string) {
	s.whepMu.Lock()
	defer s.whepMu.Unlock()
	s.whepBaseURL = strings.TrimSuffix(url, "/")
}

// SetClusterBalancer attaches a cluster balancer for Master-Slave stream routing.
func (s *StreamService) SetClusterBalancer(b *BalancerService) {
	s.balancer = b
}

// SetWebRTCGateway attaches a native WebRTC WHEP gateway (e.g., Pion WebRTC).
func (s *StreamService) SetWebRTCGateway(gw ports.WebRTCGateway) {
	s.whepMu.Lock()
	defer s.whepMu.Unlock()
	s.webrtcGW = gw
}

// RegisterStream routes through the cluster balancer on Master nodes, or registers locally.
func (s *StreamService) RegisterStream(ctx context.Context, stream *domain.Stream) error {
	if s.balancer != nil && s.balancer.Role() == domain.NodeRoleMaster {
		return s.balancer.DistributeStream(ctx, stream)
	}
	return s.RegisterStreamLocal(ctx, stream)
}

// RegisterStreamLocal persists stream metadata in on-demand standby without opening upstream ingest until a consumer connects.
func (s *StreamService) RegisterStreamLocal(ctx context.Context, stream *domain.Stream) error {
	if err := stream.Validate(); err != nil {
		s.RecordLog(domain.LogLevelWarn, "validation", fmt.Sprintf("Stream validation failed: %v", err), nil)
		return err
	}
	if stream.Status == "" {
		stream.Status = "idle"
	}
	if err := s.repo.Save(ctx, stream); err != nil {
		s.RecordLog(domain.LogLevelError, "storage", fmt.Sprintf("Failed to save stream '%s': %v", stream.StreamID, err), nil)
		return err
	}
	s.RecordLog(domain.LogLevelInfo, "stream", fmt.Sprintf("Stream '%s' registered in on-demand standby (Codec: %s, Ingest FPS: %.1f)", stream.StreamID, stream.Codec, stream.IngestFPS), nil)
	return nil
}

func (s *StreamService) GetStream(ctx context.Context, streamID string) (*domain.Stream, error) {
	if streamID == "" {
		return nil, domain.ErrInvalidStream
	}
	return s.repo.FindByID(ctx, streamID)
}

func (s *StreamService) ListStreams(ctx context.Context, searchQuery, tenantFilter, sortBy string, page, limit int) ([]*domain.Stream, int, error) {
	return s.repo.ListFiltered(ctx, searchQuery, tenantFilter, sortBy, page, limit)
}

func (s *StreamService) DeleteStream(ctx context.Context, streamID string) error {
	if streamID == "" {
		return domain.ErrInvalidStream
	}
	if s.balancer != nil && s.balancer.Role() == domain.NodeRoleMaster {
		return s.balancer.RemoveStream(ctx, streamID)
	}
	return s.DeleteStreamLocal(ctx, streamID)
}

// DeleteStreamLocal stops ingest and removes stream state on this node directly.
func (s *StreamService) DeleteStreamLocal(ctx context.Context, streamID string) error {
	if streamID == "" {
		return domain.ErrInvalidStream
	}
	if s.ingestor != nil {
		_ = s.ingestor.StopIngest(ctx, streamID)
	}
	cleanBase := filepath.Base(streamID)
	_ = os.Remove(filepath.Join("samples", fmt.Sprintf("%s.jpg", cleanBase)))
	altMatches, _ := filepath.Glob(fmt.Sprintf("samples/*%s*.jpg", cleanBase))
	for _, alt := range altMatches {
		_ = os.Remove(alt)
	}
	go deleteEngineStream(streamID)
	s.RecordLog(domain.LogLevelInfo, "stream", fmt.Sprintf("Stream '%s' unregistered and ingest stopped", streamID), nil)
	return s.repo.Delete(ctx, streamID)
}

func (s *StreamService) GetIngestStats(ctx context.Context, streamID string) (*domain.IngestStats, error) {
	if s.ingestor == nil {
		return nil, domain.ErrStreamNotFound
	}
	return s.ingestor.GetIngestStats(ctx, streamID)
}

func (s *StreamService) ListActiveIngests(ctx context.Context) ([]*domain.IngestStats, error) {
	if s.ingestor == nil {
		return nil, nil
	}
	return s.ingestor.ListActiveIngests(ctx)
}

func (s *StreamService) UpdateConsumer(ctx context.Context, streamID, analyticType string, targetFPS float64, format string) error {
	if streamID == "" || analyticType == "" {
		return domain.ErrInvalidStream
	}
	if targetFPS > 0 {
		_, _ = s.AcquireStream(ctx, streamID, "ai:"+analyticType)
	} else {
		_, _ = s.ReleaseStream(ctx, streamID, "ai:"+analyticType)
	}
	s.RecordLog(domain.LogLevelInfo, "consumer", fmt.Sprintf("Updated consumer '%s' on stream '%s' (Target FPS: %.1f, Format: %s)", analyticType, streamID, targetFPS, format), nil)
	return s.repo.UpdateConsumerFPS(ctx, streamID, analyticType, targetFPS, format)
}

var _ ports.StreamUseCase = (*StreamService)(nil)
