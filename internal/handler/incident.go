package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"

	"opspilot/internal/model"
	"opspilot/internal/store"
)

// IncidentStore is what the handler needs from storage.
// MemoryStore satisfies it today; a Postgres store will satisfy it in Step 2.
type IncidentStore interface {
	Create(ctx context.Context, title, severity string) (model.Incident, error)
	Get(ctx context.Context, id int) (model.Incident, error)
	List(ctx context.Context) ([]model.Incident, error)
	Update(ctx context.Context, id int, title, severity, status string) (model.Incident, error)
	Delete(ctx context.Context, id int) error
}

type IncidentHandler struct {
	store IncidentStore
}

func NewIncidentHandler(s IncidentStore) *IncidentHandler {
	return &IncidentHandler{store: s}
}

func (h *IncidentHandler) RegisterRoutes(r *mux.Router) {
	r.HandleFunc("/incidents", h.Create).Methods(http.MethodPost)
	r.HandleFunc("/incidents", h.List).Methods(http.MethodGet)
	r.HandleFunc("/incidents/{id:[0-9]+}", h.Get).Methods(http.MethodGet)
	r.HandleFunc("/incidents/{id:[0-9]+}", h.Update).Methods(http.MethodPut)
	r.HandleFunc("/incidents/{id:[0-9]+}", h.Delete).Methods(http.MethodDelete)
}

// What the client is allowed to send. No ID here: the server decides the ID.
type incidentRequest struct {
	Title    string `json:"title"`
	Severity string `json:"severity"`
	Status   string `json:"status"`
}

func (req incidentRequest) validate(checkStatus bool) error {
	if req.Title == "" {
		return errors.New("title is required")
	}
	if !model.ValidSeverities[req.Severity] {
		return errors.New("severity must be low, medium, high or critical")
	}
	if checkStatus && !model.ValidStatuses[req.Status] {
		return errors.New("status must be open, acknowledged or resolved")
	}
	return nil
}

func (h *IncidentHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req incidentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Warn("create: invalid JSON body", "error", err)
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := req.validate(false); err != nil {
		slog.Warn("create: validation failed", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	inc, err := h.store.Create(r.Context(), req.Title, req.Severity)
	if err != nil {
		slog.Error("create: store failed", "error", err)
		writeError(w, http.StatusInternalServerError, "could not create incident")
		return
	}
	slog.Info("incident created", "id", inc.ID, "severity", inc.Severity)
	writeJSON(w, http.StatusCreated, inc)
}

func (h *IncidentHandler) List(w http.ResponseWriter, r *http.Request) {
	list, err := h.store.List(r.Context())
	if err != nil {
		slog.Error("list: store failed", "error", err)
		writeError(w, http.StatusInternalServerError, "could not list incidents")
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (h *IncidentHandler) Get(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	inc, err := h.store.Get(r.Context(), id)
	if err != nil {
		h.storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, inc)
}

func (h *IncidentHandler) Update(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	var req incidentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Warn("update: invalid JSON body", "id", id, "error", err)
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := req.validate(true); err != nil {
		slog.Warn("update: validation failed", "id", id, "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	inc, err := h.store.Update(r.Context(), id, req.Title, req.Severity, req.Status)
	if err != nil {
		h.storeError(w, err)
		return
	}
	slog.Info("incident updated", "id", inc.ID, "status", inc.Status, "severity", inc.Severity)
	writeJSON(w, http.StatusOK, inc)
}

func (h *IncidentHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	if err := h.store.Delete(r.Context(), id); err != nil {
		h.storeError(w, err)
		return
	}
	slog.Info("incident deleted", "id", id)
	w.WriteHeader(http.StatusNoContent)
}

// parseID reads {id} from the URL, e.g. /incidents/42 → 42
func parseID(r *http.Request) (int, error) {
	return strconv.Atoi(mux.Vars(r)["id"])
}

// storeError turns a store error into the right HTTP status.
func (h *IncidentHandler) storeError(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	slog.Error("unexpected store error", "error", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}
