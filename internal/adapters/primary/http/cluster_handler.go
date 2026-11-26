package http

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// handleClusterMetrics exposes live cluster node hardware metrics, stream assignments, and balancer status.
func (h *Handler) handleClusterMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	snap, err := h.useCase.GetClusterMetrics(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(snap)
}
