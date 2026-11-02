package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/gorilla/mux"

	"opspilot/internal/cache"
	"opspilot/internal/model"
)

type AlertHandler struct {
	store IncidentStore
	dedup *cache.Deduper // nil when Redis isn't configured: every alert creates an incident
}

func NewAlertHandler(s IncidentStore, d *cache.Deduper) *AlertHandler {
	return &AlertHandler{store: s, dedup: d}
}

func (h *AlertHandler) RegisterRoutes(r *mux.Router) {
	r.HandleFunc("/alerts", h.Receive).Methods(http.MethodPost)
}

// What a monitoring system (e.g. Prometheus Alertmanager) sends.
// The fingerprint identifies "the same problem": same alert, same host.
type alertRequest struct {
	Fingerprint string `json:"fingerprint"`
	Title       string `json:"title"`
	Severity    string `json:"severity"`
}

func (req alertRequest) validate() error {
	if req.Fingerprint == "" {
		return errors.New("fingerprint is required")
	}
	if req.Title == "" {
		return errors.New("title is required")
	}
	if !model.ValidSeverities[req.Severity] {
		return errors.New("severity must be low, medium, high or critical")
	}
	return nil
}

type alertResponse struct {
	Duplicate  bool `json:"duplicate"`
	IncidentID int  `json:"incident_id,omitempty"`
}

func (h *AlertHandler) Receive(w http.ResponseWriter, r *http.Request) {
	var req alertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Warn("alert: invalid JSON body", "error", err)
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := req.validate(); err != nil {
		slog.Warn("alert: validation failed", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	claimed := false
	if h.dedup != nil {
		ok, existingID, err := h.dedup.Claim(r.Context(), req.Fingerprint)
		switch {
		case err != nil:
			// Redis is down: a duplicate incident is better than a lost alert.
			slog.Warn("alert: dedup unavailable, creating incident anyway", "fingerprint", req.Fingerprint, "error", err)
		case !ok && existingID == 0:
			// The first alert won the claim but hasn't finished creating its incident yet.
			slog.Info("alert deduplicated, incident still being created", "fingerprint", req.Fingerprint)
			writeJSON(w, http.StatusOK, alertResponse{Duplicate: true})
			return
		case !ok:
			slog.Info("alert deduplicated", "fingerprint", req.Fingerprint, "incident_id", existingID)
			writeJSON(w, http.StatusOK, alertResponse{Duplicate: true, IncidentID: existingID})
			return
		default:
			claimed = true
		}
	}

	inc, err := h.store.Create(r.Context(), req.Title, req.Severity)
	if err != nil {
		slog.Error("alert: create incident failed", "fingerprint", req.Fingerprint, "error", err)
		if claimed {
			if err := h.dedup.Release(r.Context(), req.Fingerprint); err != nil {
				slog.Warn("alert: release fingerprint failed", "fingerprint", req.Fingerprint, "error", err)
			}
		}
		writeError(w, http.StatusInternalServerError, "could not create incident")
		return
	}

	if claimed {
		if err := h.dedup.Attach(r.Context(), req.Fingerprint, inc.ID); err != nil {
			slog.Warn("alert: attach incident failed", "fingerprint", req.Fingerprint, "error", err)
		}
	}
	slog.Info("alert created incident", "fingerprint", req.Fingerprint, "incident_id", inc.ID, "severity", inc.Severity)
	writeJSON(w, http.StatusCreated, alertResponse{Duplicate: false, IncidentID: inc.ID})
}
