// Package llm is how OpsPilot talks to a large language model. The rest of the code only
// sees the Client interface, so the way Claude is reached (today the Claude Code CLI,
// later maybe the API) can change without touching the RAG code.
package llm

import (
	"context"
	"time"
)

// Result is one answer from the model.
type Result struct {
	Text         string        // the answer
	Model        string        // which model answered
	InputTokens  int           // tokens sent (system prompt + prompt)
	OutputTokens int           // tokens in the answer
	CostUSD      float64       // what this call cost, in US dollars
	Took         time.Duration // how long the call took
}

// Client sends one prompt and returns one answer. No conversation history: every call
// stands alone.
type Client interface {
	Complete(ctx context.Context, system, prompt string) (Result, error)
}
