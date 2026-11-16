// Package embedding turns text into vectors: lists of numbers that stand for its meaning,
// so texts with similar meaning get vectors that are close together.
package embedding

import (
	"context"
	"strconv"
	"strings"
)

// Dimensions is how many numbers every vector has. It must match vector(768) in
// migrations/004_create_incident_embeddings.sql, and the model must produce exactly that many.
const Dimensions = 768

// Embedder is anything that can turn text into a vector. The rest of OpsPilot only knows
// this interface, so the Ollama model could be swapped for a hosted one (such as Voyage AI)
// without touching the embedder or the search code.
type Embedder interface {
	// EmbedDocument embeds text that gets stored and searched: incidents, runbook chunks.
	EmbedDocument(ctx context.Context, text string) ([]float32, error)
	// EmbedQuery embeds the question someone is searching with.
	EmbedQuery(ctx context.Context, text string) ([]float32, error)
	// Model names the model, saved next to every vector. Vectors from different models
	// live on different "maps", so they must never be compared with each other.
	Model() string
}

// ToPgvector formats a vector the way pgvector writes it as text, e.g. "[0.1,-0.2,0.3]".
// In SQL, "$1::vector" turns that text back into a vector, so no extra library is needed.
func ToPgvector(v []float32) string {
	var b strings.Builder
	b.Grow(len(v) * 12)
	b.WriteByte('[')
	for i, x := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(x), 'g', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}
