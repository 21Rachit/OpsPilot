package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"opspilot/internal/event"
	"opspilot/internal/model"
)

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(ctx context.Context, databaseURL string) (*PostgresStore, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	// pgxpool doesn't connect yet, so Ping to fail fast if the DB is unreachable.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	// Log where we connected, but never the password.
	slog.Info("connected to postgres",
		"host", cfg.ConnConfig.Host,
		"port", cfg.ConnConfig.Port,
		"database", cfg.ConnConfig.Database,
		"user", cfg.ConnConfig.User,
		"max_conns", cfg.MaxConns,
	)
	return &PostgresStore{pool: pool}, nil
}

// Pool gives the outbox relay access to the same connection pool.
func (s *PostgresStore) Pool() *pgxpool.Pool {
	return s.pool
}

func (s *PostgresStore) Close() {
	s.pool.Close()
	slog.Info("postgres connection pool closed")
}

const incidentColumns = "id, title, severity, status, created_at"

// scanIncident reads one row into an Incident. Works for both QueryRow and Query rows.
func scanIncident(row pgx.Row) (model.Incident, error) {
	var inc model.Incident
	err := row.Scan(&inc.ID, &inc.Title, &inc.Severity, &inc.Status, &inc.CreatedAt)
	return inc, err
}

// Create inserts the incident and its "created" event in one transaction.
func (s *PostgresStore) Create(ctx context.Context, title, severity string) (model.Incident, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return model.Incident{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) // undoes everything unless Commit succeeded; harmless after Commit

	row := tx.QueryRow(ctx,
		`INSERT INTO incidents (title, severity) VALUES ($1, $2) RETURNING `+incidentColumns,
		title, severity)

	inc, err := scanIncident(row)
	if err != nil {
		return model.Incident{}, fmt.Errorf("insert incident: %w", err)
	}
	if err := insertOutbox(ctx, tx, event.IncidentCreated, inc.ID, inc); err != nil {
		return model.Incident{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return model.Incident{}, fmt.Errorf("commit: %w", err)
	}
	return inc, nil
}

func (s *PostgresStore) Get(ctx context.Context, id int) (model.Incident, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+incidentColumns+` FROM incidents WHERE id = $1`, id)

	inc, err := scanIncident(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Incident{}, ErrNotFound
	}
	if err != nil {
		return model.Incident{}, fmt.Errorf("get incident %d: %w", id, err)
	}
	return inc, nil
}

func (s *PostgresStore) List(ctx context.Context) ([]model.Incident, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+incidentColumns+` FROM incidents ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list incidents: %w", err)
	}
	defer rows.Close()

	list := []model.Incident{} // empty slice, not nil, so JSON shows [] instead of null
	for rows.Next() {
		inc, err := scanIncident(rows)
		if err != nil {
			return nil, fmt.Errorf("scan incident: %w", err)
		}
		list = append(list, inc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate incidents: %w", err)
	}
	return list, nil
}

// Update changes the incident and records an "updated" event in one transaction.
func (s *PostgresStore) Update(ctx context.Context, id int, title, severity, status string) (model.Incident, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return model.Incident{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx,
		`UPDATE incidents SET title = $2, severity = $3, status = $4
		 WHERE id = $1
		 RETURNING `+incidentColumns,
		id, title, severity, status)

	inc, err := scanIncident(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Incident{}, ErrNotFound
	}
	if err != nil {
		return model.Incident{}, fmt.Errorf("update incident %d: %w", id, err)
	}
	if err := insertOutbox(ctx, tx, event.IncidentUpdated, inc.ID, inc); err != nil {
		return model.Incident{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return model.Incident{}, fmt.Errorf("commit: %w", err)
	}
	return inc, nil
}

// Delete removes the incident and records a "deleted" event in one transaction.
func (s *PostgresStore) Delete(ctx context.Context, id int) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `DELETE FROM incidents WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete incident %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	if err := insertOutbox(ctx, tx, event.IncidentDeleted, id, map[string]int{"id": id}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// insertOutbox records an event inside the caller's transaction. Because the event and the
// incident change commit together, an event can never be lost or published for a change
// that didn't happen.
func insertOutbox(ctx context.Context, tx pgx.Tx, eventType string, incidentID int, data any) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal %s event: %w", eventType, err)
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (event_type, incident_id, payload) VALUES ($1, $2, $3)`,
		eventType, incidentID, payload)
	if err != nil {
		return fmt.Errorf("insert %s event: %w", eventType, err)
	}
	return nil
}
