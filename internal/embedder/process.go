// Package embedder keeps incident_embeddings in step with the incidents table: for every
// incident event it (re)computes the incident's vector, or removes it when the incident is deleted.
package embedder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"opspilot/internal/consumer"
	"opspilot/internal/embedding"
	"opspilot/internal/event"
	"opspilot/internal/model"
)

// Processor turns incident events into rows in incident_embeddings.
type Processor struct {
	emb  embedding.Embedder
	pool *pgxpool.Pool
}

func NewProcessor(emb embedding.Embedder, pool *pgxpool.Pool) *Processor {
	return &Processor{emb: emb, pool: pool}
}

// IncidentText is the text that gets embedded for an incident. The title carries most of
// the meaning; severity and status add context ("critical", "resolved") a search can match.
func IncidentText(inc model.Incident) string {
	return fmt.Sprintf("Incident: %s\nSeverity: %s\nStatus: %s", inc.Title, inc.Severity, inc.Status)
}

// Process is a consumer.ProcessFunc: the shared consumer loop calls it once per event,
// and handles retries, duplicates and the DLQ around it.
func (p *Processor) Process(ctx context.Context, log *slog.Logger, ev event.Event, _ int) error {
	switch ev.Type {
	case event.IncidentCreated, event.IncidentUpdated:
		var inc model.Incident
		if err := json.Unmarshal(ev.Data, &inc); err != nil || inc.ID == 0 {
			if err == nil {
				err = errors.New("missing incident id")
			}
			return consumer.Permanent(fmt.Errorf("decode incident: %w", err)) // retrying won't fix bad JSON
		}
		return p.upsert(ctx, log, inc)

	case event.IncidentDeleted:
		// ON DELETE CASCADE has usually removed the row already, together with the incident.
		// Deleting again is harmless and covers the case where it hasn't.
		if _, err := p.pool.Exec(ctx, `DELETE FROM incident_embeddings WHERE incident_id = $1`, ev.IncidentID); err != nil {
			return fmt.Errorf("delete embedding: %w", err)
		}
		log.Info("embedder: embedding deleted")
		return nil

	default:
		// A newer event type this version doesn't know about. Not an error: skip it.
		log.Debug("embedder: event type ignored")
		return nil
	}
}

func (p *Processor) upsert(ctx context.Context, log *slog.Logger, inc model.Incident) error {
	content := IncidentText(inc)

	// Calling the model is the slow part. If the text and the model are the same as last
	// time (e.g. only something we don't embed changed), the vector would be identical too.
	var oldContent, oldModel string
	err := p.pool.QueryRow(ctx,
		`SELECT content, model FROM incident_embeddings WHERE incident_id = $1`, inc.ID,
	).Scan(&oldContent, &oldModel)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("read current embedding: %w", err)
	}
	if err == nil && oldContent == content && oldModel == p.emb.Model() {
		log.Info("embedder: text unchanged, embedding kept")
		return nil
	}

	vec, err := p.emb.EmbedDocument(ctx, content)
	if err != nil {
		if errors.Is(err, embedding.ErrWrongDimensions) {
			return consumer.Permanent(err)
		}
		return err // Ollama down or slow: worth retrying
	}

	// Upsert: insert the row, or replace it if this incident already has one.
	_, err = p.pool.Exec(ctx,
		`INSERT INTO incident_embeddings (incident_id, content, model, embedding)
		 VALUES ($1, $2, $3, $4::vector)
		 ON CONFLICT (incident_id) DO UPDATE
		 SET content = EXCLUDED.content, model = EXCLUDED.model,
		     embedding = EXCLUDED.embedding, updated_at = now()`,
		inc.ID, content, p.emb.Model(), embedding.ToPgvector(vec))
	if err != nil {
		// 23503 = foreign key violation: the incident was deleted after this event was
		// published (its "deleted" event is further back in the queue). Nothing to store.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			log.Info("embedder: incident no longer exists, skipped")
			return nil
		}
		return fmt.Errorf("save embedding: %w", err)
	}
	log.Info("embedder: embedding saved", "model", p.emb.Model(), "dims", len(vec))
	return nil
}
