// Command mcp is the OpsPilot MCP server. An AI agent starts it and talks to it over
// stdin/stdout; it answers by calling the OpsPilot API.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"opspilot/internal/config"
	"opspilot/internal/mcpserver"
)

func main() {
	// stdout carries the MCP messages. Anything else printed there (a log line, a
	// fmt.Println) breaks the protocol, so logs go to stderr. The agent shows stderr in
	// its own logs.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	api := mcpserver.NewAPIClient(cfg.OpsPilotURL, cfg.OpsPilotUser, cfg.OpsPilotPassword)
	server := mcpserver.New(api)

	// The user and role decide what the agent may do. The password is never logged.
	slog.Info("mcp server starting", "transport", "stdio", "api", cfg.OpsPilotURL, "user", cfg.OpsPilotUser)

	// Run returns when the agent closes stdin (it quit) or on Ctrl+C.
	if err := server.Run(ctx, &mcp.StdioTransport{}); err != nil && ctx.Err() == nil {
		slog.Error("mcp server stopped", "error", err)
		os.Exit(1)
	}
	slog.Info("mcp server stopped")
}
