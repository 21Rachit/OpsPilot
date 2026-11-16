package handler

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"opspilot/internal/search"
)

const (
	defaultSearchLimit = 5
	maxSearchLimit     = 20
	maxQueryChars      = 500
)

// Searcher is what the handler needs from the search package.
type Searcher interface {
	Search(ctx context.Context, query string, limit int) (search.Results, error)
}

type SearchHandler struct {
	searcher Searcher
}

func NewSearchHandler(s Searcher) *SearchHandler {
	return &SearchHandler{searcher: s}
}

func (h *SearchHandler) RegisterRoutes(r *mux.Router) {
	r.HandleFunc("/search", h.Search).Methods(http.MethodGet)
}

// Search handles GET /search?q=...&limit=...
func (h *SearchHandler) Search(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q == "" {
		writeError(w, http.StatusBadRequest, "q is required, e.g. /search?q=login+is+slow")
		return
	}
	if len(q) > maxQueryChars {
		writeError(w, http.StatusBadRequest, "q is too long (max 500 characters)")
		return
	}

	limit := defaultSearchLimit
	if s := r.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > maxSearchLimit {
			writeError(w, http.StatusBadRequest, "limit must be a number from 1 to 20")
			return
		}
		limit = n
	}

	start := time.Now()
	res, err := h.searcher.Search(r.Context(), q, limit)
	if err != nil {
		// Usually Ollama is down. The rest of the API still works, so say "unavailable"
		// (503) instead of a generic 500.
		slog.Error("search failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "search is unavailable right now")
		return
	}
	// Don't log q itself: people may paste customer names or secrets into a search box.
	slog.Info("search done", "query_chars", len(q), "incidents", len(res.Incidents),
		"runbooks", len(res.Runbooks), "took", time.Since(start).Round(time.Millisecond))
	writeJSON(w, http.StatusOK, res)
}
