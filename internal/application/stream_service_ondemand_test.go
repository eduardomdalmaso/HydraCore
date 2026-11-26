package application

import (
	"context"
	"sync"
	"testing"
	"time"

	"hydrastream/internal/adapters/secondary/memory"
	"hydrastream/internal/domain"
	"hydrastream/internal/ports"
)

type spyIngestor struct {
	mu      sync.Mutex
	starts  int
	stops   int
	running map[string]bool
}

func newSpyIngestor() *spyIngestor {
	return &spyIngestor{running: make(map[string]bool)}
}

func (s *spyIngestor) StartIngest(_ context.Context, st *domain.Stream) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.starts++
	s.running[st.StreamID] = true
	return nil
}

func (s *spyIngestor) StopIngest(_ context.Context, streamID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stops++
	delete(s.running, streamID)
	return nil
}

func (s *spyIngestor) GetIngestStats(_ context.Context, _ string) (*domain.IngestStats, error) {
	return nil, domain.ErrStreamNotFound
}

func (s *spyIngestor) ListActiveIngests(_ context.Context) ([]*domain.IngestStats, error) {
	return nil, nil
}

var _ ports.StreamIngestor = (*spyIngestor)(nil)

func TestStreamServiceOnDemandAndOneToNFanOut(t *testing.T) {
	repo := memory.NewStreamRepository()
	ing := newSpyIngestor()
	svc := NewStreamService(repo, ing, nil)
	svc.idleStopDelay = 20 * time.Millisecond

	ctx := context.Background()
	st := &domain.Stream{
		StreamID:  "cam_ondemand_01",
		SourceURL: "rtsp://127.0.0.1:554/stream1",
		Codec:     "H264",
		IngestFPS: 15,
	}

	if err := svc.RegisterStream(ctx, st); err != nil {
		t.Fatalf("RegisterStream failed: %v", err)
	}
	if ing.starts != 0 {
		t.Fatalf("expected 0 ingest starts on register (on-demand), got %d", ing.starts)
	}

	// 1st consumer (mosaic) -> opens 1 upstream ingest
	c1, err := svc.AcquireStream(ctx, "cam_ondemand_01", "mjpeg:1")
	if err != nil || c1 != 1 {
		t.Fatalf("expected 1 consumer, got %d (err=%v)", c1, err)
	}
	if ing.starts != 1 {
		t.Fatalf("expected 1 ingest start on 1st consumer, got %d", ing.starts)
	}

	// 2nd consumer (AI analytics) -> reuses same upstream ingest (1-to-N)
	c2, err := svc.AcquireStream(ctx, "cam_ondemand_01", "ai:yolo")
	if err != nil || c2 != 2 {
		t.Fatalf("expected 2 consumers, got %d (err=%v)", c2, err)
	}
	if ing.starts != 1 {
		t.Fatalf("expected still 1 upstream ingest for 2 consumers, got %d", ing.starts)
	}

	// Release mosaic, AI still active -> must keep ingest alive
	rem, _ := svc.ReleaseStream(ctx, "cam_ondemand_01", "mjpeg:1")
	if rem != 1 {
		t.Fatalf("expected 1 remaining consumer, got %d", rem)
	}
	time.Sleep(40 * time.Millisecond)
	if ing.stops != 0 {
		t.Fatalf("expected 0 stops while AI consumer is active, got %d", ing.stops)
	}

	// Release last consumer -> stops after idle grace period
	rem, _ = svc.ReleaseStream(ctx, "cam_ondemand_01", "ai:yolo")
	if rem != 0 {
		t.Fatalf("expected 0 remaining consumers, got %d", rem)
	}
	time.Sleep(50 * time.Millisecond)
	if ing.stops != 1 {
		t.Fatalf("expected 1 stop after last consumer released, got %d", ing.stops)
	}
}
