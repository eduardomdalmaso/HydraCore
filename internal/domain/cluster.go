package domain

import (
	"math"
	"sort"
	"time"
)

// NodeRole defines whether a HydraCore instance acts as Master or Slave.
type NodeRole string

const (
	NodeRoleMaster NodeRole = "master"
	NodeRoleSlave  NodeRole = "slave"
)

// NodeStatus defines the live availability state of a cluster node.
type NodeStatus string

const (
	NodeStatusOnline   NodeStatus = "online"
	NodeStatusDraining NodeStatus = "draining"
	NodeStatusOffline  NodeStatus = "offline"
)

// ClusterNodeMetrics holds real-time hardware and stream load metrics for one node.
type ClusterNodeMetrics struct {
	NodeID          string     `json:"node_id"`
	Role            NodeRole   `json:"role"`
	Hostname        string     `json:"hostname"`
	IP              string     `json:"ip"`
	APIBaseURL      string     `json:"api_base_url,omitempty"`
	Status          NodeStatus `json:"status"`
	CPUCores        int        `json:"cpu_cores"`
	CPUUsagePct     float64    `json:"cpu_usage_pct"`
	RAMTotalMB      float64    `json:"ram_total_mb"`
	RAMUsedMB       float64    `json:"ram_used_mb"`
	RAMUsagePct     float64    `json:"ram_usage_pct"`
	GPUDetected     bool       `json:"gpu_detected"`
	GPUModel        string     `json:"gpu_model,omitempty"`
	GPUUtilPct      float64    `json:"gpu_util_pct"`
	VRAMUsagePct    float64    `json:"vram_usage_pct"`
	SHMOccupancyPct float64    `json:"shm_occupancy_pct"`
	ActiveStreams   int        `json:"active_streams"`
	TotalIngestFPS  float64    `json:"total_ingest_fps"`
	LoadScore       float64    `json:"load_score"`
	LastHeartbeat   time.Time  `json:"last_heartbeat"`
}

// StreamAssignment maps a registered camera/stream to the node currently ingesting it.
type StreamAssignment struct {
	StreamID     string    `json:"stream_id"`
	NodeID       string    `json:"node_id"`
	TenantID     string    `json:"tenant_id,omitempty"`
	Protocol     string    `json:"protocol"`
	SourceURL    string    `json:"source_url"`
	SubstreamURL string    `json:"substream_url,omitempty"`
	Codec        string    `json:"codec"`
	IngestFPS    float64   `json:"ingest_fps"`
	Status       string    `json:"status"`
	AssignedAt   time.Time `json:"assigned_at"`
}

// MigrationEvent records an automatic or manual stream handoff between cluster nodes.
type MigrationEvent struct {
	StreamID  string    `json:"stream_id"`
	FromNode  string    `json:"from_node"`
	ToNode    string    `json:"to_node"`
	Reason    string    `json:"reason"`
	Status    string    `json:"status"` // "completed", "failed"
	Timestamp time.Time `json:"timestamp"`
}

// BalancerConfig tunes automatic hardware-based stream distribution and rebalancing.
type BalancerConfig struct {
	Enabled               bool          `json:"enabled"`
	HighCPUThresholdPct   float64       `json:"high_cpu_threshold_pct"`
	LowCPUThresholdPct    float64       `json:"low_cpu_threshold_pct"`
	HighScoreThreshold    float64       `json:"high_score_threshold"`
	MinScoreDelta         float64       `json:"min_score_delta"`
	MaxMigrationsPerCycle int           `json:"max_migrations_per_cycle"`
	RebalanceInterval     time.Duration `json:"rebalance_interval"`
	NodeTTL               time.Duration `json:"node_ttl"`
	MigrationCooldown     time.Duration `json:"migration_cooldown"`
}

// DefaultBalancerConfig returns production-safe defaults with hysteresis to prevent ping-pong.
func DefaultBalancerConfig() BalancerConfig {
	return BalancerConfig{
		Enabled:               true,
		HighCPUThresholdPct:   75.0,
		LowCPUThresholdPct:    45.0,
		HighScoreThreshold:    72.0,
		MinScoreDelta:         20.0,
		MaxMigrationsPerCycle: 2,
		RebalanceInterval:     5 * time.Second,
		NodeTTL:               15 * time.Second,
		MigrationCooldown:     20 * time.Second,
	}
}

// BalancerStatus summarizes the live state and history of the cluster load balancer.
type BalancerStatus struct {
	Enabled               bool             `json:"enabled"`
	Strategy              string           `json:"strategy"`
	RebalancingInProgress bool             `json:"rebalancing_in_progress"`
	HighCPUThresholdPct   float64          `json:"high_cpu_threshold_pct"`
	LowCPUThresholdPct    float64          `json:"low_cpu_threshold_pct"`
	TotalMigrations       uint64           `json:"total_migrations"`
	LastRebalanceAt       *time.Time       `json:"last_rebalance_at,omitempty"`
	RecentMigrations      []MigrationEvent `json:"recent_migrations"`
}

// ClusterMetricsSnapshot is the unified payload exposed to third-party apps and dashboards.
type ClusterMetricsSnapshot struct {
	ClusterRole      NodeRole             `json:"cluster_role"`
	LocalNodeID      string               `json:"local_node_id"`
	TotalNodes       int                  `json:"total_nodes"`
	OnlineNodes      int                  `json:"online_nodes"`
	TotalStreams     int                  `json:"total_streams"`
	Nodes            []ClusterNodeMetrics `json:"nodes"`
	Assignments      []StreamAssignment   `json:"assignments"`
	Balancer         BalancerStatus       `json:"balancer"`
	CollectedAt      time.Time            `json:"collected_at"`
}

// RebalancePlanItem represents one recommended stream migration from an overloaded node to a target node.
type RebalancePlanItem struct {
	StreamID string
	FromNode string
	ToNode   string
	Reason   string
}

// CalculateLoadScore computes a 0-100 composite hardware & stream load score for a node.
func CalculateLoadScore(m ClusterNodeMetrics) float64 {
	cpuWeight := 0.50
	ramWeight := 0.25
	gpuWeight := 0.15
	streamPenalty := math.Min(25.0, float64(m.ActiveStreams)*2.0)

	score := (m.CPUUsagePct * cpuWeight) + (m.RAMUsagePct * ramWeight)
	if m.GPUDetected {
		gpuLoad := math.Max(m.GPUUtilPct, m.VRAMUsagePct)
		score += gpuLoad * gpuWeight
	} else {
		score += m.CPUUsagePct * gpuWeight
	}
	score += streamPenalty * 0.4

	if m.Role == NodeRoleMaster {
		// Slight preference for placing ingestion workloads on Slaves when available.
		score += 5.0
	}
	return math.Round(math.Max(0, math.Min(100, score))*10) / 10
}

// SelectLeastLoadedNode picks the best online node for a new stream registration.
func SelectLeastLoadedNode(nodes []ClusterNodeMetrics, preferSlaves bool) (ClusterNodeMetrics, bool) {
	var candidates []ClusterNodeMetrics
	for _, n := range nodes {
		if n.Status == NodeStatusOnline {
			candidates = append(candidates, n)
		}
	}
	if len(candidates) == 0 {
		return ClusterNodeMetrics{}, false
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if preferSlaves && candidates[i].Role != candidates[j].Role {
			return candidates[i].Role == NodeRoleSlave
		}
		if candidates[i].LoadScore != candidates[j].LoadScore {
			return candidates[i].LoadScore < candidates[j].LoadScore
		}
		return candidates[i].ActiveStreams < candidates[j].ActiveStreams
	})

	return candidates[0], true
}

// PlanRebalance evaluates overloaded nodes against underloaded online nodes and proposes migrations.
func PlanRebalance(nodes []ClusterNodeMetrics, assignments []StreamAssignment, cfg BalancerConfig) []RebalancePlanItem {
	if !cfg.Enabled || len(nodes) < 2 || cfg.MaxMigrationsPerCycle <= 0 {
		return nil
	}

	var overloaded []ClusterNodeMetrics
	var targets []ClusterNodeMetrics

	for _, n := range nodes {
		if n.Status != NodeStatusOnline {
			continue
		}
		if (n.CPUUsagePct >= cfg.HighCPUThresholdPct || n.LoadScore >= cfg.HighScoreThreshold) && n.ActiveStreams > 0 {
			overloaded = append(overloaded, n)
		} else if n.CPUUsagePct <= cfg.LowCPUThresholdPct {
			targets = append(targets, n)
		}
	}

	if len(overloaded) == 0 || len(targets) == 0 {
		return nil
	}

	sort.SliceStable(overloaded, func(i, j int) bool {
		return overloaded[i].LoadScore > overloaded[j].LoadScore
	})
	sort.SliceStable(targets, func(i, j int) bool {
		return targets[i].LoadScore < targets[j].LoadScore
	})

	var plan []RebalancePlanItem
	targetIdx := 0

	for _, src := range overloaded {
		if len(plan) >= cfg.MaxMigrationsPerCycle {
			break
		}
		for _, a := range assignments {
			if len(plan) >= cfg.MaxMigrationsPerCycle || targetIdx >= len(targets) {
				break
			}
			if a.NodeID != src.NodeID {
				continue
			}
			dst := targets[targetIdx]
			if src.LoadScore-dst.LoadScore < cfg.MinScoreDelta {
				continue
			}
			reason := "high_load_score"
			if src.CPUUsagePct >= cfg.HighCPUThresholdPct {
				reason = "cpu_threshold_exceeded"
			}
			plan = append(plan, RebalancePlanItem{
				StreamID: a.StreamID,
				FromNode: src.NodeID,
				ToNode:   dst.NodeID,
				Reason:   reason,
			})
			targetIdx = (targetIdx + 1) % len(targets)
		}
	}

	return plan
}
