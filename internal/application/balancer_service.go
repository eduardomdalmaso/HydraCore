package application

import (
	"context"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"hydrastream/internal/domain"
	"hydrastream/internal/ports"
)

// BalancerService orchestrates Master-Slave node heartbeats, hardware-aware stream placement, and auto-rebalancing.
type BalancerService struct {
	nodeID     string
	role       domain.NodeRole
	localIP    string
	apiBaseURL string
	streamSvc  *StreamService
	mesh       ports.ClusterMesh
	cfg        domain.BalancerConfig

	mu               sync.RWMutex
	nodes            map[string]domain.ClusterNodeMetrics
	assignments      map[string]domain.StreamAssignment
	streamSpecs      map[string]*domain.Stream
	lastMigratedAt   map[string]time.Time
	recentMigrations []domain.MigrationEvent
	lastRebalanceAt  *time.Time

	totalMigrations atomic.Uint64
	rebalancing     atomic.Bool
	cancel          context.CancelFunc
}

// NewBalancerService creates a cluster balancer for either Master or Slave role.
func NewBalancerService(
	nodeID string,
	role domain.NodeRole,
	apiBaseURL string,
	streamSvc *StreamService,
	mesh ports.ClusterMesh,
	cfg domain.BalancerConfig,
) *BalancerService {
	if nodeID == "" {
		h, _ := os.Hostname()
		if h == "" {
			h = "hydracore-node"
		}
		nodeID = h
	}
	if role != domain.NodeRoleSlave {
		role = domain.NodeRoleMaster
	}

	return &BalancerService{
		nodeID:         nodeID,
		role:           role,
		localIP:        detectLocalIP(),
		apiBaseURL:     apiBaseURL,
		streamSvc:      streamSvc,
		mesh:           mesh,
		cfg:            cfg,
		nodes:          make(map[string]domain.ClusterNodeMetrics),
		assignments:    make(map[string]domain.StreamAssignment),
		streamSpecs:    make(map[string]*domain.Stream),
		lastMigratedAt: make(map[string]time.Time),
	}
}

// Role returns the configured cluster role (master or slave).
func (b *BalancerService) Role() domain.NodeRole {
	return b.role
}

// Start launches heartbeat publishing, NATS mesh subscriptions, and (on Master) the rebalance loop.
func (b *BalancerService) Start(parentCtx context.Context) {
	ctx, cancel := context.WithCancel(parentCtx)
	b.cancel = cancel

	if b.mesh != nil {
		if err := b.mesh.SubscribeNodeCommands(b.nodeID, b.handleRemoteAssign, b.handleRemoteUnassign); err != nil {
			log.Printf("⚠️ [Cluster] Failed to subscribe node commands for %s: %v\n", b.nodeID, err)
		}
		if b.role == domain.NodeRoleMaster {
			if err := b.mesh.SubscribeHeartbeats(b.recordNodeHeartbeat); err != nil {
				log.Printf("⚠️ [Cluster] Failed to subscribe heartbeats: %v\n", err)
			}
			b.restoreAssignmentsFromKV(ctx)
		}
	}

	b.publishLocalHeartbeat(ctx)

	go b.heartbeatLoop(ctx)
	if b.role == domain.NodeRoleMaster {
		go b.rebalanceLoop(ctx)
	}
	log.Printf("⚖️ [Cluster] Balancer started | NodeID=%s | Role=%s | AutoRebalance=%v\n", b.nodeID, b.role, b.cfg.Enabled)
}

// Stop halts background loops gracefully.
func (b *BalancerService) Stop() {
	if b.cancel != nil {
		b.cancel()
	}
	if b.mesh != nil {
		b.mesh.Close()
	}
}

// DistributeStream routes a new stream registration (RTSP/ONVIF/RTMP) to the least-loaded node.
func (b *BalancerService) DistributeStream(ctx context.Context, stream *domain.Stream) error {
	if stream == nil {
		return domain.ErrInvalidStream
	}
	if err := stream.Validate(); err != nil {
		return err
	}

	targetID := b.nodeID
	if b.role == domain.NodeRoleMaster && b.mesh != nil {
		b.refreshNodeStatusesLocked()
		b.mu.RLock()
		nodes := b.snapshotNodesLocked()
		b.mu.RUnlock()
		if picked, ok := domain.SelectLeastLoadedNode(nodes, true); ok {
			targetID = picked.NodeID
		}
	}

	if targetID != b.nodeID && b.mesh != nil {
		if err := b.mesh.AssignStream(ctx, targetID, stream); err != nil {
			log.Printf("⚠️ [Balancer] Remote assign to %s failed (%v); falling back to local node %s\n", targetID, err, b.nodeID)
			targetID = b.nodeID
		}
	}

	if targetID == b.nodeID {
		if err := b.streamSvc.RegisterStreamLocal(ctx, stream); err != nil {
			return err
		}
	}

	assignment := domain.StreamAssignment{
		StreamID:     stream.StreamID,
		NodeID:       targetID,
		TenantID:     stream.TenantID,
		Protocol:     detectProtocol(stream.SourceURL),
		SourceURL:    maskSourceURL(stream.SourceURL),
		SubstreamURL: maskSourceURL(stream.SubstreamURL),
		Codec:        stream.Codec,
		IngestFPS:    stream.IngestFPS,
		Status:       "online",
		AssignedAt:   time.Now().UTC(),
	}

	b.mu.Lock()
	b.streamSpecs[stream.StreamID] = cloneStream(stream)
	b.assignments[stream.StreamID] = assignment
	b.mu.Unlock()

	if b.mesh != nil {
		_ = b.mesh.SaveClusterAssignment(ctx, assignment)
	}
	return nil
}

// RemoveStream unassigns a stream from whichever cluster node currently owns it.
func (b *BalancerService) RemoveStream(ctx context.Context, streamID string) error {
	b.mu.RLock()
	assignment, exists := b.assignments[streamID]
	b.mu.RUnlock()

	if exists && assignment.NodeID != "" && assignment.NodeID != b.nodeID && b.mesh != nil {
		_ = b.mesh.UnassignStream(ctx, assignment.NodeID, streamID)
	} else {
		_ = b.streamSvc.DeleteStreamLocal(ctx, streamID)
	}

	b.mu.Lock()
	delete(b.assignments, streamID)
	delete(b.streamSpecs, streamID)
	b.mu.Unlock()

	if b.mesh != nil {
		_ = b.mesh.DeleteClusterAssignment(ctx, streamID)
	}
	return nil
}

// GetClusterMetrics returns a live snapshot of all cluster nodes, hardware load, stream assignments, and balancer status.
func (b *BalancerService) GetClusterMetrics(ctx context.Context) (*domain.ClusterMetricsSnapshot, error) {
	b.publishLocalHeartbeat(ctx)
	b.refreshNodeStatusesLocked()

	b.mu.RLock()
	nodes := b.snapshotNodesLocked()
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
	assignments := make([]domain.StreamAssignment, 0, len(b.assignments))
	for _, a := range b.assignments {
		assignments = append(assignments, a)
	}
	sort.Slice(assignments, func(i, j int) bool { return assignments[i].StreamID < assignments[j].StreamID })
	recent := make([]domain.MigrationEvent, len(b.recentMigrations))
	copy(recent, b.recentMigrations)
	lastReb := b.lastRebalanceAt
	b.mu.RUnlock()

	onlineCount := 0
	for _, n := range nodes {
		if n.Status == domain.NodeStatusOnline {
			onlineCount++
		}
	}

	return &domain.ClusterMetricsSnapshot{
		ClusterRole:  b.role,
		LocalNodeID:  b.nodeID,
		TotalNodes:   len(nodes),
		OnlineNodes:  onlineCount,
		TotalStreams: len(assignments),
		Nodes:        nodes,
		Assignments:  assignments,
		Balancer: domain.BalancerStatus{
			Enabled:               b.cfg.Enabled,
			Strategy:              "least_load_with_hysteresis",
			RebalancingInProgress: b.rebalancing.Load(),
			HighCPUThresholdPct:   b.cfg.HighCPUThresholdPct,
			LowCPUThresholdPct:    b.cfg.LowCPUThresholdPct,
			TotalMigrations:       b.totalMigrations.Load(),
			LastRebalanceAt:       lastReb,
			RecentMigrations:      recent,
		},
		CollectedAt: time.Now().UTC(),
	}, nil
}
