-- Step 6c: runbooks (how-to guides) split into chunks, each with its own vector, so a search
-- finds the exact section that helps instead of a whole 10-page document.
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS runbook_chunks (
    source      TEXT        NOT NULL,  -- the file it came from, e.g. runbooks/disk-full.md
    chunk_index INT         NOT NULL,  -- position in the file, from 0
    title       TEXT        NOT NULL,  -- the runbook's "# " heading
    heading     TEXT        NOT NULL,  -- the section, e.g. "Diagnose" or "Fix > Free space quickly"
    content     TEXT        NOT NULL,  -- exact text that was embedded (shown in search results)
    file_hash   TEXT        NOT NULL,  -- sha256 of the whole file: same hash = file unchanged, skip it
    model       TEXT        NOT NULL,
    embedding   vector(768) NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, chunk_index)
);

CREATE INDEX IF NOT EXISTS runbook_chunks_embedding_idx
    ON runbook_chunks USING hnsw (embedding vector_cosine_ops);
