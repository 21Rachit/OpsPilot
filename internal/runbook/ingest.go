package runbook

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"opspilot/internal/embedding"
)

// Ingester embeds runbook files and stores their chunks in runbook_chunks.
type Ingester struct {
	emb  embedding.Embedder
	pool *pgxpool.Pool
}

func NewIngester(emb embedding.Embedder, pool *pgxpool.Pool) *Ingester {
	return &Ingester{emb: emb, pool: pool}
}

// Result says what Ingest did with one file.
type Result struct {
	Chunks  int  // how many chunks the file has now
	Skipped bool // the file hadn't changed since the last run, so nothing was done
}

// Ingest splits one file, embeds every chunk and replaces the file's old chunks with the new
// ones. source is the file's path, used as its name in the table. Unless force is set, a file
// whose content and model are the same as last time is skipped.
func (in *Ingester) Ingest(ctx context.Context, source string, data []byte, force bool) (Result, error) {
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])

	if !force {
		var oldHash, oldModel string
		err := in.pool.QueryRow(ctx,
			`SELECT file_hash, model FROM runbook_chunks WHERE source = $1 LIMIT 1`, source,
		).Scan(&oldHash, &oldModel)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return Result{}, fmt.Errorf("read current chunks: %w", err)
		}
		if err == nil && oldHash == hash && oldModel == in.emb.Model() {
			return Result{Skipped: true}, nil
		}
	}

	title, chunks := Split(string(data))
	if title == "" {
		title = strings.TrimSuffix(filepath.Base(source), filepath.Ext(source)) // no "# " heading: use the file name
	}

	// Embed everything first. This is the slow part, and no transaction is open while it runs.
	texts := make([]string, len(chunks))
	vecs := make([]string, len(chunks))
	for i, c := range chunks {
		texts[i] = EmbedText(title, c)
		vec, err := in.emb.EmbedDocument(ctx, texts[i])
		if err != nil {
			return Result{}, fmt.Errorf("embed chunk %d (%s): %w", i, c.Heading, err)
		}
		vecs[i] = embedding.ToPgvector(vec)
	}

	// Swap old chunks for new ones in one transaction, so a search never sees half of each.
	// Delete-then-insert also handles a file that now has fewer chunks than before.
	tx, err := in.pool.Begin(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) // undoes everything unless Commit succeeded

	if _, err := tx.Exec(ctx, `DELETE FROM runbook_chunks WHERE source = $1`, source); err != nil {
		return Result{}, fmt.Errorf("delete old chunks: %w", err)
	}
	for i, c := range chunks {
		_, err := tx.Exec(ctx,
			`INSERT INTO runbook_chunks (source, chunk_index, title, heading, content, file_hash, model, embedding)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8::vector)`,
			source, i, title, c.Heading, texts[i], hash, in.emb.Model(), vecs[i])
		if err != nil {
			return Result{}, fmt.Errorf("insert chunk %d: %w", i, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, fmt.Errorf("commit: %w", err)
	}
	return Result{Chunks: len(chunks)}, nil
}

// RemoveMissing deletes the chunks of runbooks under dir that no longer exist on disk.
// keep lists the files that do exist. Only rows from dir are touched, so ingesting a
// second folder doesn't remove the first one's runbooks.
func (in *Ingester) RemoveMissing(ctx context.Context, dir string, keep []string) (int64, error) {
	prefix := filepath.ToSlash(filepath.Clean(dir)) + "/"
	tag, err := in.pool.Exec(ctx,
		`DELETE FROM runbook_chunks WHERE starts_with(source, $1) AND NOT (source = ANY($2))`,
		prefix, keep)
	if err != nil {
		return 0, fmt.Errorf("remove deleted runbooks: %w", err)
	}
	return tag.RowsAffected(), nil
}
