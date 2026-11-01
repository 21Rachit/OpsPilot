package main

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/gorilla/mux"

	"opspilot/internal/handler"
	"opspilot/internal/middleware"
	"opspilot/internal/store"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	s := store.NewMemoryStore()
	h := handler.NewIncidentHandler(s)

	r := mux.NewRouter()
	h.RegisterRoutes(r)

	// Wrap the whole router (not r.Use) so 404/405 from mux get logged too.
	app := middleware.Logging(r)

	addr := ":8087"
	slog.Info("OpsPilot starting", "addr", "http://localhost"+addr)
	if err := http.ListenAndServe(addr, app); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
