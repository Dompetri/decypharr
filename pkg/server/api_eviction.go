package server

import (
	"io"
	"net/http"
	"strings"

	json "github.com/bytedance/sonic"
	"github.com/go-chi/chi/v5"
	"github.com/sirrobot01/decypharr/internal/utils"
)

// handleForceEvict evicts a single torrent on demand, ignoring the quota
// threshold. Used to test the eviction + probe-cache + re-hydration cycle:
//
//	curl -X POST http://<host>/api/eviction/evict/<infohash>
func (s *Server) handleForceEvict(w http.ResponseWriter, r *http.Request) {
	hash := chi.URLParam(r, "hash")
	if hash == "" {
		http.Error(w, "No infohash provided", http.StatusBadRequest)
		return
	}
	freed, err := s.manager.ForceEvict(r.Context(), hash)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	utils.JSONResponse(w, map[string]any{
		"status":      "evicted",
		"infohash":    hash,
		"freed_bytes": freed,
	}, http.StatusOK)
}

// handleEvictionStatus reports a quota-limited provider's storage usage.
//
//	curl http://<host>/api/eviction/status/<debrid-name>
func (s *Server) handleEvictionStatus(w http.ResponseWriter, r *http.Request) {
	debridName := chi.URLParam(r, "debrid")
	used, limit, err := s.manager.QuotaStatus(r.Context(), debridName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	pct := 0.0
	if limit > 0 {
		pct = float64(used) / float64(limit) * 100
	}
	utils.JSONResponse(w, map[string]any{
		"debrid":       debridName,
		"used_bytes":   used,
		"limit_bytes":  limit,
		"used_percent": pct,
	}, http.StatusOK)
}

// handleIngestMagnets queues magnet links for archive ingestion (drip-fed into
// the provider and reduced to probe slices). Accepts JSON {"magnets":[...]} or a
// newline-delimited text body.
//
//	curl -X POST http://<host>/api/eviction/ingest -H 'Content-Type: application/json' -d '{"magnets":["magnet:?...","magnet:?..."]}'
func (s *Server) handleIngestMagnets(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<20)) // up to 64MB of magnets
	if err != nil {
		http.Error(w, "Failed to read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	var magnets []string
	var payload struct {
		Magnets []string `json:"magnets"`
	}
	if json.Unmarshal(body, &payload) == nil && len(payload.Magnets) > 0 {
		magnets = payload.Magnets
	} else {
		// Fall back to newline-delimited text.
		for _, line := range splitLines(string(body)) {
			if line != "" {
				magnets = append(magnets, line)
			}
		}
	}

	added, total, err := s.manager.EnqueueMagnets(magnets)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	utils.JSONResponse(w, map[string]any{
		"added":        added,
		"queued_total": total,
		"received":     len(magnets),
	}, http.StatusOK)
}

// handleIngestStatus reports the current ingest queue length.
func (s *Server) handleIngestStatus(w http.ResponseWriter, r *http.Request) {
	utils.JSONResponse(w, map[string]any{"queued": s.manager.IngestQueueLen()}, http.StatusOK)
}

// handleRebuildFromHistory recovers entries an *arr still references (via a
// symlink) but that no longer exist locally — e.g. after a wiped DB or a PikPak
// account switch — by reading each release's grab history for the torrent
// infohash and re-adding it. Optional ?arr=<name> scopes it to one arr;
// ?dry_run=true reports what would be rebuilt without touching the provider.
//
//	curl -X POST 'http://<host>/api/repair/rebuild-from-history?arr=radarr&dry_run=true'
func (s *Server) handleRebuildFromHistory(w http.ResponseWriter, r *http.Request) {
	arrName := r.URL.Query().Get("arr")
	dryRun := isTruthy(r.URL.Query().Get("dry_run"))

	if strings.Contains(r.Header.Get("Content-Type"), "application/json") && r.Body != nil {
		var req struct {
			Arr    string `json:"arr"`
			DryRun *bool  `json:"dry_run"`
		}
		if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err == nil {
			if req.Arr != "" {
				arrName = req.Arr
			}
			if req.DryRun != nil {
				dryRun = *req.DryRun
			}
		}
	}

	report, err := s.manager.RebuildFromArrHistory(r.Context(), arrName, dryRun)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	utils.JSONResponse(w, report, http.StatusOK)
}

// isTruthy reports whether a query-string value means "true".
func isTruthy(v string) bool {
	switch strings.ToLower(trimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' || s[i] == '\r' {
			out = append(out, trimSpace(s[start:i]))
			start = i + 1
		}
	}
	out = append(out, trimSpace(s[start:]))
	return out
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}
