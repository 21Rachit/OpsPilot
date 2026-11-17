// Package search finds the incidents and runbook sections whose meaning is closest to a
// question, using the vectors the embedder and the ingest command stored in pgvector.
package search

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"opspilot/internal/embedding"
)

// IncidentHit is a past incident that looks like the question.
type IncidentHit struct {
	ID        int       `json:"id"`
	Title     string    `json:"title"`
	Severity  string    `json:"severity"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	Score     float64   `json:"score"` // 1 = same meaning, 0 = unrelated
}

// RunbookHit is one runbook section that looks like the question.
type RunbookHit struct {
	Source  string  `json:"source"`  // file, e.g. runbooks/dns-latency.md
	Title   string  `json:"title"`   // runbook title
	Heading string  `json:"heading"` // section, e.g. "Fix"
	Content string  `json:"content"` // the text that was embedded
	Score   float64 `json:"score"`
}

// Results is what one search returns: both kinds, each best match first.
type Results struct {
	Query     string        `json:"query"`
	Model     string        `json:"model"`
	MinScore  float64       `json:"min_score"`
	Incidents []IncidentHit `json:"incidents"`
	Runbooks  []RunbookHit  `json:"runbooks"`
}

// Searcher embeds a question and looks up the nearest stored vectors.
type Searcher struct {
	emb  embedding.Embedder
	pool *pgxpool.Pool
}

func NewSearcher(emb embedding.Embedder, pool *pgxpool.Pool) *Searcher {
	return &Searcher{emb: emb, pool: pool}
}

// Search embeds query once and returns up to limit incidents and up to limit runbook sections.
// Matches scoring below minScore are left out (0 keeps everything).
func (s *Searcher) Search(ctx context.Context, query string, limit int, minScore float64) (Results, error) {
	vec, err := s.emb.EmbedQuery(ctx, query) // "search_query: " prefix, not "search_document: "
	if err != nil {
		return Results{}, fmt.Errorf("embed query: %w", err)
	}
	q := embedding.ToPgvector(vec)

	incidents, err := s.incidents(ctx, q, limit, minScore)
	if err != nil {
		return Results{}, err
	}
	runbooks, err := s.runbooks(ctx, q, limit, minScore)
	if err != nil {
		return Results{}, err
	}
	return Results{Query: query, Model: s.emb.Model(), MinScore: minScore, Incidents: incidents, Runbooks: runbooks}, nil
}

// incidents joins incident_embeddings to incidents, so the result has the incident's current
// title and status. The model filter skips vectors made by a different model: they can't be
// compared with this query's vector.
func (s *Searcher) incidents(ctx context.Context, q string, limit int, minScore float64) ([]IncidentHit, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT i.id, i.title, i.severity, i.status, i.created_at, 1 - (e.embedding <=> $1::vector)
		FROM incident_embeddings e
		JOIN incidents i ON i.id = e.incident_id
		WHERE e.model = $2 AND 1 - (e.embedding <=> $1::vector) >= $4
		ORDER BY e.embedding <=> $1::vector
		LIMIT $3`, q, s.emb.Model(), limit, minScore)
	if err != nil {
		return nil, fmt.Errorf("search incidents: %w", err)
	}
	defer rows.Close()

	hits := []IncidentHit{} // not nil, so the JSON says [] instead of null when nothing matches
	for rows.Next() {
		var h IncidentHit
		if err := rows.Scan(&h.ID, &h.Title, &h.Severity, &h.Status, &h.CreatedAt, &h.Score); err != nil {
			return nil, fmt.Errorf("scan incident: %w", err)
		}
		hits = append(hits, h)
	}
	return hits, rows.Err()
}

func (s *Searcher) runbooks(ctx context.Context, q string, limit int, minScore float64) ([]RunbookHit, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT source, title, heading, content, 1 - (embedding <=> $1::vector)
		FROM runbook_chunks
		WHERE model = $2 AND 1 - (embedding <=> $1::vector) >= $4
		ORDER BY embedding <=> $1::vector
		LIMIT $3`, q, s.emb.Model(), limit, minScore)
	if err != nil {
		return nil, fmt.Errorf("search runbooks: %w", err)
	}
	defer rows.Close()

	hits := []RunbookHit{}
	for rows.Next() {
		var h RunbookHit
		if err := rows.Scan(&h.Source, &h.Title, &h.Heading, &h.Content, &h.Score); err != nil {
			return nil, fmt.Errorf("scan runbook chunk: %w", err)
		}
		hits = append(hits, h)
	}
	return hits, rows.Err()
}
