package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"

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
	// Send every SQL query to our logger. They show up only when LOG_LEVEL=debug.
	cfg.ConnConfig.Tracer = &tracelog.TraceLog{
		Logger:   tracelog.LoggerFunc(logPgx),
		LogLevel: tracelog.LogLevelInfo,
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

func (s *PostgresStore) Close() {
	s.pool.Close()
	slog.Info("postgres connection pool closed")
}

// logPgx turns pgx's query logs into slog DEBUG lines (sql, args, time, rows affected).
// Query errors are also logged at DEBUG: the handler already logs them at ERROR.
func logPgx(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]any) {
	attrs := []any{"pgx_level", level.String()}
	for _, k := range slices.Sorted(maps.Keys(data)) {
		attrs = append(attrs, k, data[k])
	}
	slog.DebugContext(ctx, "sql "+msg, attrs...)
}

const incidentColumns = "id, title, severity, status, created_at"

// scanIncident reads one row into an Incident. Works for both QueryRow and Query rows.
func scanIncident(row pgx.Row) (model.Incident, error) {
	var inc model.Incident
	err := row.Scan(&inc.ID, &inc.Title, &inc.Severity, &inc.Status, &inc.CreatedAt)
	return inc, err
}

func (s *PostgresStore) Create(ctx context.Context, title, severity string) (model.Incident, error) {
	row := s.pool.QueryRow(ctx,
		`INSERT INTO incidents (title, severity) VALUES ($1, $2) RETURNING `+incidentColumns,
		title, severity)

	inc, err := scanIncident(row)
	if err != nil {
		return model.Incident{}, fmt.Errorf("insert incident: %w", err)
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

func (s *PostgresStore) Update(ctx context.Context, id int, title, severity, status string) (model.Incident, error) {
	row := s.pool.QueryRow(ctx,
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
	return inc, nil
}

func (s *PostgresStore) Delete(ctx context.Context, id int) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM incidents WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete incident %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
