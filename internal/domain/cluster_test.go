package domain

import (
	"testing"
	"time"
)

func TestCalculateLoadScorePrefersSlavesAndTracksCPU(t *testing.T) {
	master := ClusterNodeMetrics{
		NodeID:        "master-1",
		Role:          NodeRoleMaster,
		Status:        NodeStatusOnline,
		CPUUsagePct:   50,
		RAMUsagePct:   40,
		ActiveStreams: 2,
	}
	slave := ClusterNodeMetrics{
		NodeID:        "slave-1",
		Role:          NodeRoleSlave,
		Status:        NodeStatusOnline,
		CPUUsagePct:   50,
		RAMUsagePct:   40,
		ActiveStreams: 2,
	}

	if CalculateLoadScore(slave) >= CalculateLoadScore(master) {
		t.Fatalf("expected slave load score (%.1f) < master load score (%.1f)", CalculateLoadScore(slave), CalculateLoadScore(master))
	}
}

func TestSelectLeastLoadedNodePrefersOnlineSlave(t *testing.T) {
	nodes := []ClusterNodeMetrics{
		{NodeID: "master-1", Role: NodeRoleMaster, Status: NodeStatusOnline, LoadScore: 10},
		{NodeID: "slave-off", Role: NodeRoleSlave, Status: NodeStatusOffline, LoadScore: 1},
		{NodeID: "slave-2", Role: NodeRoleSlave, Status: NodeStatusOnline, LoadScore: 25},
		{NodeID: "slave-1", Role: NodeRoleSlave, Status: NodeStatusOnline, LoadScore: 15},
	}

	picked, ok := SelectLeastLoadedNode(nodes, true)
	if !ok {
		t.Fatal("expected an online node to be selected")
	}
	if picked.NodeID != "slave-1" {
		t.Fatalf("expected slave-1, got %s", picked.NodeID)
	}
}

func TestPlanRebalanceOffloadsOverloadedMasterToIdleSlave(t *testing.T) {
	cfg := DefaultBalancerConfig()
	now := time.Now().UTC()

	nodes := []ClusterNodeMetrics{
		{NodeID: "master-1", Role: NodeRoleMaster, Status: NodeStatusOnline, CPUUsagePct: 86, LoadScore: 82, ActiveStreams: 3, LastHeartbeat: now},
		{NodeID: "slave-1", Role: NodeRoleSlave, Status: NodeStatusOnline, CPUUsagePct: 12, LoadScore: 10, ActiveStreams: 0, LastHeartbeat: now},
	}
	assignments := []StreamAssignment{
		{StreamID: "cam_01", NodeID: "master-1", Protocol: "rtsp"},
		{StreamID: "cam_02", NodeID: "master-1", Protocol: "onvif"},
		{StreamID: "cam_03", NodeID: "master-1", Protocol: "rtmp"},
	}

	plan := PlanRebalance(nodes, assignments, cfg)
	if len(plan) != cfg.MaxMigrationsPerCycle {
		t.Fatalf("expected %d planned migrations, got %d", cfg.MaxMigrationsPerCycle, len(plan))
	}
	for _, item := range plan {
		if item.FromNode != "master-1" || item.ToNode != "slave-1" {
			t.Fatalf("unexpected migration item: %+v", item)
		}
		if item.Reason != "cpu_threshold_exceeded" {
			t.Fatalf("expected cpu_threshold_exceeded reason, got %s", item.Reason)
		}
	}
}
