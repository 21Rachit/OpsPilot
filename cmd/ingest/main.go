package main

import (
	"context"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"opspilot/internal/config"
	"opspilot/internal/embedding"
	"opspilot/internal/runbook"
	"opspilot/internal/store"
)

// ingest loads the Markdown runbooks in a folder into runbook_chunks, so search can find them.
// Unlike incidents, runbooks are files that change rarely, so this is a command you run
// (after editing a runbook, or from CI), not a Kafka consumer that runs all the time.
//
//	go run ./cmd/ingest                 # ingest ./runbooks
//	go run ./cmd/ingest -dry-run        # only show how the files are split
//	go run ./cmd/ingest -force          # re-embed everything, even unchanged files
func main() {
	os.Exit(run())
}

func run() int {
	dir := flag.String("dir", "runbooks", "folder with the Markdown (.md) runbooks")
	dryRun := flag.Bool("dry-run", false, "only print the chunks; don't call the model or the database")
	force := flag.Bool("force", false, "re-embed every file, even unchanged ones")
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid config", "error", err)
		return 1
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	files, err := findMarkdown(*dir)
	if err != nil {
		slog.Error("could not read the runbook folder", "dir", *dir, "error", err)
		return 1
	}
	if len(files) == 0 {
		// Refuse instead of carrying on: removing "missing" runbooks would delete them all.
		slog.Error("no .md files found", "dir", *dir)
		return 1
	}

	if *dryRun {
		return printChunks(files)
	}

	if cfg.DatabaseURL == "" {
		slog.Error("ingest needs DATABASE_URL (use -dry-run to only see the chunks)")
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	connectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	pg, err := store.NewPostgresStore(connectCtx, cfg.DatabaseURL)
	cancel()
	if err != nil {
		slog.Error("startup failed", "error", err)
		return 1
	}
	defer pg.Close()

	emb := embedding.NewOllama(cfg.OllamaURL, cfg.EmbeddingModel)
	checkCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	_, err = emb.EmbedDocument(checkCtx, "startup check")
	cancel()
	if err != nil {
		slog.Error("startup failed: embedding model not ready (is Ollama running? did you run 'ollama pull "+cfg.EmbeddingModel+"'?)",
			"ollama_url", cfg.OllamaURL, "model", cfg.EmbeddingModel, "error", err)
		return 1
	}

	ing := runbook.NewIngester(emb, pg.Pool())
	var ingested, skipped, failed, chunks int
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			slog.Error("runbook failed", "source", f, "error", err)
			failed++
			continue
		}
		start := time.Now()
		res, err := ing.Ingest(ctx, f, data, *force)
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("stopped, the remaining files were not ingested")
				return 1
			}
			// One bad file shouldn't stop the others. Its old chunks stay as they were.
			slog.Error("runbook failed", "source", f, "error", err)
			failed++
			continue
		}
		if res.Skipped {
			slog.Info("runbook unchanged, skipped", "source", f)
			skipped++
			continue
		}
		slog.Info("runbook ingested", "source", f, "chunks", res.Chunks, "took", time.Since(start).Round(time.Millisecond))
		ingested++
		chunks += res.Chunks
	}

	removed, err := ing.RemoveMissing(ctx, *dir, files)
	if err != nil {
		slog.Error("could not remove deleted runbooks", "error", err)
		failed++
	} else if removed > 0 {
		slog.Info("removed chunks of runbooks that no longer exist", "chunks", removed)
	}

	slog.Info("ingest finished", "files", len(files), "ingested", ingested, "skipped", skipped,
		"failed", failed, "chunks_written", chunks, "model", emb.Model())
	if failed > 0 {
		return 1
	}
	return 0
}

// findMarkdown lists every .md file under dir, including subfolders.
func findMarkdown(dir string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.EqualFold(filepath.Ext(path), ".md") {
			files = append(files, filepath.ToSlash(path))
		}
		return nil
	})
	return files, err
}

// printChunks shows how each file is split, without embedding anything.
func printChunks(files []string) int {
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			return 1
		}
		title, chunks := runbook.Split(string(data))
		fmt.Printf("%s  title=%q  %d chunks\n", f, title, len(chunks))
		for i, c := range chunks {
			first, _, _ := strings.Cut(c.Text, "\n")
			if len(first) > 70 {
				first = first[:70] + "..."
			}
			fmt.Printf("  [%d] %-28s %4d chars | %s\n", i, c.Heading, len(c.Text), first)
		}
		fmt.Println()
	}
	return 0
}
