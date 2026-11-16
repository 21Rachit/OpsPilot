package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Ollama calls a local Ollama server (https://ollama.com), which runs the model on this machine.
type Ollama struct {
	baseURL string
	model   string
	client  *http.Client
}

// NewOllama creates a client for the Ollama server at baseURL, such as http://localhost:11434.
func NewOllama(baseURL, model string) *Ollama {
	return &Ollama{
		baseURL: strings.TrimRight(baseURL, "/"),
		model:   model,
		// The first call after Ollama starts loads the model into memory, which can take
		// several seconds. Later calls take milliseconds.
		client: &http.Client{Timeout: 60 * time.Second},
	}
}

func (o *Ollama) Model() string { return o.model }

// nomic-embed-text was trained with a prefix that says what kind of text it is reading,
// and gives better matches when it gets one. Other models ignore it or want different ones,
// which is why the prefixes live here and not in the callers.
func (o *Ollama) EmbedDocument(ctx context.Context, text string) ([]float32, error) {
	return o.embed(ctx, "search_document: "+text)
}

func (o *Ollama) EmbedQuery(ctx context.Context, text string) ([]float32, error) {
	return o.embed(ctx, "search_query: "+text)
}

// ErrWrongDimensions means the model returned a different number of values than the table
// stores. Retrying can't fix it: the model or the table has to change.
var ErrWrongDimensions = errors.New("embedding has the wrong number of dimensions")

// embed calls POST /api/embed: {"model": "...", "input": "..."} → {"embeddings": [[...]]}.
func (o *Ollama) embed(ctx context.Context, input string) ([]float32, error) {
	body, err := json.Marshal(map[string]string{"model": o.model, "input": input})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.baseURL+"/api/embed", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := o.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call ollama: %w", err) // e.g. connection refused: Ollama isn't running
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Ollama explains what went wrong in {"error": "..."}, e.g. model "x" not found.
		var e struct {
			Error string `json:"error"`
		}
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if json.Unmarshal(msg, &e) == nil && e.Error != "" {
			msg = []byte(e.Error)
		}
		return nil, fmt.Errorf("ollama returned %s: %s", resp.Status, msg)
	}

	var out struct {
		Embeddings [][]float32 `json:"embeddings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode ollama response: %w", err)
	}
	if len(out.Embeddings) != 1 {
		return nil, fmt.Errorf("ollama returned %d embeddings, want 1", len(out.Embeddings))
	}
	v := out.Embeddings[0]
	if len(v) != Dimensions {
		return nil, fmt.Errorf("%w: model %s returned %d, the table stores %d", ErrWrongDimensions, o.model, len(v), Dimensions)
	}
	return v, nil
}
