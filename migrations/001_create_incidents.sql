CREATE TABLE IF NOT EXISTS incidents (
    id         BIGSERIAL   PRIMARY KEY,
    title      TEXT        NOT NULL,
    severity   TEXT        NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    status     TEXT        NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'acknowledged', 'resolved')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
