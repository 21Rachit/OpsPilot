package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"opspilot/internal/rag"
)

const maxQuestionChars = 500

// Asker is what the handler needs from the rag package.
type Asker interface {
	Ask(ctx context.Context, question string) (rag.Answer, error)
}

type AskHandler struct {
	asker Asker
}

func NewAskHandler(a Asker) *AskHandler {
	return &AskHandler{asker: a}
}

func (h *AskHandler) RegisterRoutes(r *mux.Router) {
	r.HandleFunc("/ask", h.Ask).Methods(http.MethodPost)
}

type askRequest struct {
	Question string `json:"question"`
}

// Ask handles POST /ask with a body like {"question": "the disk is full, what do I do?"}
func (h *AskHandler) Ask(w http.ResponseWriter, r *http.Request) {
	var req askRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, `body must be JSON like {"question": "..."}`)
		return
	}
	q := strings.TrimSpace(req.Question)
	if q == "" {
		writeError(w, http.StatusBadRequest, "question is required")
		return
	}
	if len(q) > maxQuestionChars {
		writeError(w, http.StatusBadRequest, "question is too long (max 500 characters)")
		return
	}

	start := time.Now()
	ans, err := h.asker.Ask(r.Context(), q)
	if err != nil {
		// Ollama or Claude is down, slow or not logged in. Only /ask is affected.
		slog.Error("ask failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "ask is unavailable right now")
		return
	}
	// Like /search: log sizes and cost, not the question itself.
	slog.Info("ask done", "question_chars", len(q), "sources", len(ans.Sources),
		"model", ans.Model, "cost_usd", ans.CostUSD, "took", time.Since(start).Round(time.Millisecond))
	writeJSON(w, http.StatusOK, ans)
}
