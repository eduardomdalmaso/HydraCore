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

type fakeClusterMesh struct {
	mu          sync.Mutex
	assigned    map[string][]*domain.Stream
	unassigned  map[string][]string
	savedKV     map[string]domain.StreamAssignment
	heartbeats  []domain.ClusterNodeMetrics
}

func newFakeClusterMesh() *fakeClusterMesh {
	return &fakeClusterMesh{
		assigned:   make(map[string][]*domain.Stream),
		unassigned: make(map[string][]string),
		savedKV:    make(map[string]domain.StreamAssignment),
	}
}

func (f *fakeClusterMesh) PublishNodeHeartbeat(_ context.Context, m domain.ClusterNodeMetrics) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heartbeats = append(f.heartbeats, m)
	return nil
}

func (f *fakeClusterMesh) SubscribeHeartbeats(func(domain.ClusterNodeMetrics)) error { return nil }

func (f *fakeClusterMesh) AssignStream(_ context.Context, nodeID string, st *domain.Stream) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.assigned[nodeID] = append(f.assigned[nodeID], st)
	return nil
}

func (f *fakeClusterMesh) UnassignStream(_ context.Context, nodeID, streamID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unassigned[nodeID] = append(f.unassigned[nodeID], streamID)
	return nil
}

func (f *fakeClusterMesh) SubscribeNodeCommands(string, func(context.Context, *domain.Stream) error, func(context.Context, string) error) error {
	return nil
}

func (f *fakeClusterMesh) SaveClusterAssignment(_ context.Context, a domain.StreamAssignment) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.savedKV[a.StreamID] = a
	return nil
}

func (f *fakeClusterMesh) DeleteClusterAssignment(_ context.Context, streamID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.savedKV, streamID)
	return nil
}

func (f *fakeClusterMesh) ListClusterAssignments(_ context.Context) ([]domain.StreamAssignment, error) {
	return nil, nil
}

func (f *fakeClusterMesh) Close() {}

var _ ports.ClusterMesh = (*fakeClusterMesh)(nil)

func TestBalancerDistributesAndRebalancesOffMaster(t *testing.T) {
	repo := memory.NewStreamRepository()
	svc := NewStreamService(repo, nil, nil)
	mesh := newFakeClusterMesh()

	cfg := domain.DefaultBalancerConfig()
	cfg.MigrationCooldown = 0
	balancer := NewBalancerService("master-1", domain.NodeRoleMaster, "http://localhost:8080", svc, mesh, cfg)
	svc.SetClusterBalancer(balancer)

	ctx := context.Background()
	st := &domain.Stream{
		StreamID:  "cam_rtsp_01",
		SourceURL: "rtsp://admin:secret@192.168.1.10:554/stream1",
		Codec:     "H265",
		IngestFPS: 30,
	}

	// Register initially with only master online -> lands on master-1
	balancer.recordNodeHeartbeat(domain.ClusterNodeMetrics{
		NodeID:        "master-1",
		Role:          domain.NodeRoleMaster,
		Status:        domain.NodeStatusOnline,
		CPUUsagePct:   88,
		ActiveStreams: 1,
		LoadScore:     85,
		LastHeartbeat: time.Now().UTC(),
	})

	if err := svc.RegisterStream(ctx, st); err != nil {
		t.Fatalf("RegisterStream failed: %v", err)
	}

	// Now a third-party app spins up a fresh idle Slave
	balancer.recordNodeHeartbeat(domain.ClusterNodeMetrics{
		NodeID:        "slave-02",
		Role:          domain.NodeRoleSlave,
		Status:        domain.NodeStatusOnline,
		CPUUsagePct:   8,
		ActiveStreams: 0,
		LoadScore:     5,
		LastHeartbeat: time.Now().UTC(),
	})

	// Force master-1 overloaded state and trigger rebalance
	balancer.recordNodeHeartbeat(domain.ClusterNodeMetrics{
		NodeID:        "master-1",
		Role:          domain.NodeRoleMaster,
		Status:        domain.NodeStatusOnline,
		CPUUsagePct:   88,
		ActiveStreams: 1,
		LoadScore:     85,
		LastHeartbeat: time.Now().UTC(),
	})
	balancer.RunRebalanceCycle(ctx)

	snap, err := svc.GetClusterMetrics(ctx)
	if err != nil {
		t.Fatalf("GetClusterMetrics failed: %v", err)
	}
	if snap.Balancer.TotalMigrations != 1 {
		t.Fatalf("expected 1 migration, got %d", snap.Balancer.TotalMigrations)
	}
	if len(mesh.assigned["slave-02"]) != 1 || mesh.assigned["slave-02"][0].StreamID != "cam_rtsp_01" {
		t.Fatalf("expected cam_rtsp_01 assigned to slave-02, got %+v", mesh.assigned)
	}
	if snap.Assignments[0].SourceURL != "rtsp://%2A%2A%2A:%2A%2A%2A@192.168.1.10:554/stream1" &&
		snap.Assignments[0].SourceURL != "rtsp://***:***@192.168.1.10:554/stream1" {
		t.Fatalf("expected masked source URL, got %s", snap.Assignments[0].SourceURL)
	}
}
