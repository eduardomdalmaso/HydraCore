package http

import (
	"encoding/json"
	"fmt"
	"net/http"

	"hydrastream/internal/domain"
)

func (h *Handler) handleClusterTopology(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	streamID := r.URL.Query().Get("stream_id")
	topo, err := h.useCase.GetClusterTopology(r.Context(), streamID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(topo)
}

func (h *Handler) handleControlPanelTelemetry(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	stats, err := h.useCase.GetControlPanelTelemetry(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(stats)
}

func (h *Handler) handleSystemInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	info, err := h.useCase.GetSystemInfo(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(info)
}

func (h *Handler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	snap, err := h.useCase.GetClusterMetrics(r.Context())
	if err != nil || snap == nil {
		fmt.Fprintf(w, "# HELP hydrastream_up HydraStream metrics availability\n")
		fmt.Fprintf(w, "# TYPE hydrastream_up gauge\n")
		fmt.Fprintf(w, "hydrastream_up 1\n")
		return
	}

	fmt.Fprintf(w, "# HELP hydrastream_node_cpu_usage_pct Host CPU usage percentage per cluster node\n")
	fmt.Fprintf(w, "# TYPE hydrastream_node_cpu_usage_pct gauge\n")
	for _, n := range snap.Nodes {
		fmt.Fprintf(w, "hydrastream_node_cpu_usage_pct{node_id=%q,role=%q,status=%q} %.1f\n", n.NodeID, n.Role, n.Status, n.CPUUsagePct)
	}

	fmt.Fprintf(w, "# HELP hydrastream_node_ram_usage_pct Host RAM usage percentage per cluster node\n")
	fmt.Fprintf(w, "# TYPE hydrastream_node_ram_usage_pct gauge\n")
	for _, n := range snap.Nodes {
		fmt.Fprintf(w, "hydrastream_node_ram_usage_pct{node_id=%q,role=%q} %.1f\n", n.NodeID, n.Role, n.RAMUsagePct)
	}

	fmt.Fprintf(w, "# HELP hydrastream_node_load_score Composite hardware load score per cluster node\n")
	fmt.Fprintf(w, "# TYPE hydrastream_node_load_score gauge\n")
	for _, n := range snap.Nodes {
		fmt.Fprintf(w, "hydrastream_node_load_score{node_id=%q,role=%q} %.1f\n", n.NodeID, n.Role, n.LoadScore)
	}

	fmt.Fprintf(w, "# HELP hydrastream_node_active_streams Active ingested streams per cluster node\n")
	fmt.Fprintf(w, "# TYPE hydrastream_node_active_streams gauge\n")
	for _, n := range snap.Nodes {
		fmt.Fprintf(w, "hydrastream_node_active_streams{node_id=%q,role=%q} %d\n", n.NodeID, n.Role, n.ActiveStreams)
	}

	fmt.Fprintf(w, "# HELP hydrastream_cluster_migrations_total Total automatic stream migrations by balancer\n")
	fmt.Fprintf(w, "# TYPE hydrastream_cluster_migrations_total counter\n")
	fmt.Fprintf(w, "hydrastream_cluster_migrations_total %d\n", snap.Balancer.TotalMigrations)
}

func (h *Handler) handleChaosInject(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"POST required"}`, http.StatusMethodNotAllowed)
		return
	}

	var req domain.ChaosInjection
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
		return
	}

	res, err := h.useCase.InjectChaos(r.Context(), &req)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(res)
}

func (h *Handler) handleChaosReset(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"POST required"}`, http.StatusMethodNotAllowed)
		return
	}

	if err := h.useCase.ResetChaos(r.Context()); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"reset","message":"All chaos injection circuits disarmed and telemetry stabilized."}`))
}
