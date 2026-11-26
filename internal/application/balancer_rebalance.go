package application

import (
	"context"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"hydrastream/internal/domain"
)

func (b *BalancerService) handleRemoteAssign(ctx context.Context, st *domain.Stream) error {
	return b.streamSvc.RegisterStreamLocal(ctx, st)
}

func (b *BalancerService) handleRemoteUnassign(ctx context.Context, streamID string) error {
	return b.streamSvc.DeleteStreamLocal(ctx, streamID)
}

func (b *BalancerService) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.publishLocalHeartbeat(ctx)
		}
	}
}

func (b *BalancerService) rebalanceLoop(ctx context.Context) {
	interval := b.cfg.RebalanceInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.RunRebalanceCycle(ctx)
		}
	}
}

// RunRebalanceCycle evaluates cluster hardware metrics and migrates streams off overloaded nodes.
func (b *BalancerService) RunRebalanceCycle(ctx context.Context) {
	if !b.cfg.Enabled || b.role != domain.NodeRoleMaster {
		return
	}
	if !b.rebalancing.CompareAndSwap(false, true) {
		return
	}
	defer b.rebalancing.Store(false)

	b.refreshNodeStatusesLocked()

	b.mu.RLock()
	nodes := b.snapshotNodesLocked()
	assignments := make([]domain.StreamAssignment, 0, len(b.assignments))
	for _, a := range b.assignments {
		if last, ok := b.lastMigratedAt[a.StreamID]; ok && time.Since(last) < b.cfg.MigrationCooldown {
			continue
		}
		assignments = append(assignments, a)
	}
	b.mu.RUnlock()

	plan := domain.PlanRebalance(nodes, assignments, b.cfg)
	if len(plan) == 0 {
		return
	}

	now := time.Now().UTC()
	b.mu.Lock()
	b.lastRebalanceAt = &now
	b.mu.Unlock()

	for _, item := range plan {
		b.executeMigration(ctx, item)
	}
}

func (b *BalancerService) executeMigration(ctx context.Context, item domain.RebalancePlanItem) {
	b.mu.RLock()
	spec := b.streamSpecs[item.StreamID]
	b.mu.RUnlock()
	if spec == nil {
		if st, err := b.streamSvc.repo.FindByID(ctx, item.StreamID); err == nil {
			spec = cloneStream(st)
		}
	}
	if spec == nil {
		return
	}

	var assignErr error
	if item.ToNode == b.nodeID {
		assignErr = b.streamSvc.RegisterStreamLocal(ctx, spec)
	} else if b.mesh != nil {
		assignErr = b.mesh.AssignStream(ctx, item.ToNode, spec)
	}

	evt := domain.MigrationEvent{
		StreamID:  item.StreamID,
		FromNode:  item.FromNode,
		ToNode:    item.ToNode,
		Reason:    item.Reason,
		Status:    "completed",
		Timestamp: time.Now().UTC(),
	}

	if assignErr != nil {
		evt.Status = "failed"
		b.recordMigrationEvent(evt)
		log.Printf("⚠️ [Balancer] Migration %s (%s -> %s) failed: %v\n", item.StreamID, item.FromNode, item.ToNode, assignErr)
		return
	}

	if item.FromNode == b.nodeID {
		_ = b.streamSvc.DeleteStreamLocal(ctx, item.StreamID)
	} else if b.mesh != nil {
		_ = b.mesh.UnassignStream(ctx, item.FromNode, item.StreamID)
	}

	b.mu.Lock()
	if a, ok := b.assignments[item.StreamID]; ok {
		a.NodeID = item.ToNode
		a.AssignedAt = time.Now().UTC()
		b.assignments[item.StreamID] = a
		if b.mesh != nil {
			_ = b.mesh.SaveClusterAssignment(ctx, a)
		}
	}
	b.lastMigratedAt[item.StreamID] = time.Now().UTC()
	b.mu.Unlock()

	b.totalMigrations.Add(1)
	b.recordMigrationEvent(evt)
	log.Printf("⚖️ [Balancer] Migrated stream '%s': %s -> %s (%s)\n", item.StreamID, item.FromNode, item.ToNode, item.Reason)
}

func (b *BalancerService) publishLocalHeartbeat(ctx context.Context) {
	hw, _ := b.streamSvc.GetHardwareTelemetry(ctx)
	streams, total, _ := b.streamSvc.repo.ListFiltered(ctx, "", "", "", 1, 1000)

	var totalFPS float64
	for _, st := range streams {
		totalFPS += st.IngestFPS
	}

	metrics := domain.ClusterNodeMetrics{
		NodeID:         b.nodeID,
		Role:           b.role,
		IP:             b.localIP,
		APIBaseURL:     b.apiBaseURL,
		Status:         domain.NodeStatusOnline,
		ActiveStreams:  total,
		TotalIngestFPS: totalFPS,
		LastHeartbeat:  time.Now().UTC(),
	}
	if hw != nil {
		metrics.Hostname = hw.Host.Hostname
		metrics.CPUCores = hw.Host.CPUCores
		metrics.CPUUsagePct = hw.Host.CPUUsagePct
		metrics.RAMTotalMB = hw.Host.HostTotalRAMMB
		metrics.RAMUsedMB = hw.Host.HostUsedRAMMB
		metrics.RAMUsagePct = hw.Host.HostRAMUsagePct
		metrics.GPUDetected = hw.GPU.Detected
		metrics.GPUModel = hw.GPU.Model
		metrics.GPUUtilPct = hw.GPU.GPUUtilPct
		metrics.VRAMUsagePct = hw.GPU.VRAMUsagePct
		metrics.SHMOccupancyPct = hw.SHM.OccupancyPct
	}
	metrics.LoadScore = domain.CalculateLoadScore(metrics)

	b.recordNodeHeartbeat(metrics)
	if b.mesh != nil {
		_ = b.mesh.PublishNodeHeartbeat(ctx, metrics)
	}
}

func (b *BalancerService) recordNodeHeartbeat(m domain.ClusterNodeMetrics) {
	if m.NodeID == "" {
		return
	}
	if m.LastHeartbeat.IsZero() {
		m.LastHeartbeat = time.Now().UTC()
	}
	if m.LoadScore == 0 {
		m.LoadScore = domain.CalculateLoadScore(m)
	}
	m.Status = domain.NodeStatusOnline

	b.mu.Lock()
	b.nodes[m.NodeID] = m
	b.mu.Unlock()
}

func (b *BalancerService) refreshNodeStatusesLocked() {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now().UTC()
	for id, n := range b.nodes {
		if id == b.nodeID {
			continue
		}
		if now.Sub(n.LastHeartbeat) > b.cfg.NodeTTL {
			n.Status = domain.NodeStatusOffline
			b.nodes[id] = n
		}
	}
}

func (b *BalancerService) snapshotNodesLocked() []domain.ClusterNodeMetrics {
	out := make([]domain.ClusterNodeMetrics, 0, len(b.nodes))
	for _, n := range b.nodes {
		out = append(out, n)
	}
	return out
}

func (b *BalancerService) recordMigrationEvent(evt domain.MigrationEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.recentMigrations = append([]domain.MigrationEvent{evt}, b.recentMigrations...)
	if len(b.recentMigrations) > 50 {
		b.recentMigrations = b.recentMigrations[:50]
	}
}

func (b *BalancerService) restoreAssignmentsFromKV(ctx context.Context) {
	list, err := b.mesh.ListClusterAssignments(ctx)
	if err != nil {
		return
	}
	b.mu.Lock()
	for _, a := range list {
		b.assignments[a.StreamID] = a
	}
	b.mu.Unlock()
}

func detectProtocol(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" {
		return "unknown"
	}
	return strings.ToLower(u.Scheme)
}

func maskSourceURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.User == nil {
		return rawURL
	}
	u.User = url.UserPassword("***", "***")
	return u.String()
}

func cloneStream(s *domain.Stream) *domain.Stream {
	if s == nil {
		return nil
	}
	cp := *s
	return &cp
}

func detectLocalIP() string {
	h, _ := os.Hostname()
	_ = h
	return "127.0.0.1"
}
