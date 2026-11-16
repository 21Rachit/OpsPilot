-- Step 6: semantic search. Needs the pgvector extension, so Postgres must run the
-- pgvector/pgvector image instead of plain postgres (see README, Quick start).
CREATE EXTENSION IF NOT EXISTS vector;

-- One row per incident: the text we turned into numbers, which model did it, and the
-- numbers themselves. A separate table (not a column on incidents) keeps the AI part
-- optional: the API works the same whether or not an embedding exists yet.
CREATE TABLE IF NOT EXISTS incident_embeddings (
    incident_id BIGINT      PRIMARY KEY REFERENCES incidents(id) ON DELETE CASCADE,
    content     TEXT        NOT NULL,            -- exact text that was embedded
    model       TEXT        NOT NULL,            -- e.g. nomic-embed-text; vectors from different models can't be compared
    embedding   vector(768) NOT NULL,            -- nomic-embed-text returns 768 numbers
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- HNSW = approximate nearest-neighbour index, so "find the closest vectors" doesn't have
-- to compare against every row. vector_cosine_ops makes it serve the <=> (cosine) operator.
CREATE INDEX IF NOT EXISTS incident_embeddings_embedding_idx
    ON incident_embeddings USING hnsw (embedding vector_cosine_ops);
