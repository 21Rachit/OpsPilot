package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// ClaudeCLI answers by running the Claude Code CLI in headless mode ("claude -p"), using
// the login Claude Code already has on this machine. It is meant for a laptop: a server
// would use the API instead.
type ClaudeCLI struct {
	bin     string        // path to the claude program
	model   string        // e.g. "claude-opus-5-5"
	timeout time.Duration // a call that takes longer is stopped
	workDir string        // an empty folder the CLI runs in
}

// NewClaudeCLI checks that the claude program can be found and makes the empty folder it
// will run in.
func NewClaudeCLI(bin, model string, timeout time.Duration) (*ClaudeCLI, error) {
	path, err := exec.LookPath(bin)
	if err != nil {
		return nil, fmt.Errorf("claude CLI not found (%q): %w", bin, err)
	}
	dir, err := os.MkdirTemp("", "opspilot-claude-")
	if err != nil {
		return nil, fmt.Errorf("make work dir: %w", err)
	}
	return &ClaudeCLI{bin: path, model: model, timeout: timeout, workDir: dir}, nil
}

// cliOutput is the part of the JSON that "claude -p --output-format json" prints that we use.
type cliOutput struct {
	Subtype      string  `json:"subtype"` // "success" when it worked
	IsError      bool    `json:"is_error"`
	Result       string  `json:"result"` // the answer text
	TotalCostUSD float64 `json:"total_cost_usd"`
	Usage        struct {
		InputTokens  int `json:"input_tokens"`
		CacheWrite   int `json:"cache_creation_input_tokens"`
		CacheRead    int `json:"cache_read_input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

func (c *ClaudeCLI) Complete(ctx context.Context, system, prompt string) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	// Safety: the prompt contains incident and runbook text that anyone could have written,
	// so it may try to give Claude orders (prompt injection). Claude Code can normally run
	// commands and edit files, so every way to act is switched off here. With these flags
	// it can only write text back.
	cmd := exec.CommandContext(ctx, c.bin,
		// headless: answer once and exit, printing one JSON object
		"-p", "--output-format", "json",
		"--model", c.model,
		// replaces Claude Code's own coding-assistant prompt
		"--system-prompt", system,
		// no tools at all: no shell, no file edits, no web
		"--tools", "",
		// no MCP servers, and ignore settings files (hooks, permissions, plugins)
		"--strict-mcp-config", "--setting-sources", "",
		// don't save this as a session on disk
		"--no-session-persistence",
	)
	cmd.Dir = c.workDir                   // an empty folder: no project files to read
	cmd.Stdin = strings.NewReader(prompt) // stdin, not an argument: no length limit, not shown in ps
	cmd.WaitDelay = 5 * time.Second       // after a timeout, don't wait forever for it to exit

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	start := time.Now()
	err := cmd.Run()
	took := time.Since(start)

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return Result{}, fmt.Errorf("claude CLI took longer than %s", c.timeout)
	}

	// Even when it fails, the CLI usually prints JSON that says why, so try to read it first.
	var out cliOutput
	if jsonErr := json.Unmarshal(stdout.Bytes(), &out); jsonErr != nil {
		if err != nil {
			return Result{}, fmt.Errorf("claude CLI failed: %w: %s", err, firstLine(stderr.String()))
		}
		return Result{}, fmt.Errorf("claude CLI printed something that isn't JSON: %w", jsonErr)
	}
	if out.IsError || out.Subtype != "success" {
		return Result{}, fmt.Errorf("claude CLI returned an error: %s", firstLine(out.Result))
	}
	if err != nil {
		return Result{}, fmt.Errorf("claude CLI failed: %w", err)
	}
	if strings.TrimSpace(out.Result) == "" {
		return Result{}, errors.New("claude CLI returned an empty answer")
	}

	return Result{
		Text:  out.Result,
		Model: c.model,
		// The CLI splits input into new, cache-written and cache-read tokens; all were sent.
		InputTokens:  out.Usage.InputTokens + out.Usage.CacheWrite + out.Usage.CacheRead,
		OutputTokens: out.Usage.OutputTokens,
		CostUSD:      out.TotalCostUSD,
		Took:         took,
	}, nil
}

// firstLine keeps error messages short: the first line, at most 200 characters.
func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}
