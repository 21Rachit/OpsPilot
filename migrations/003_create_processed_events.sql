-- Events a consumer has already handled. Kafka delivers at-least-once, so the same event
-- can arrive twice; a consumer checks this table to skip events it has already processed.
CREATE TABLE IF NOT EXISTS processed_events (
    consumer     TEXT        NOT NULL,  -- which service processed it, e.g. "notifier"
    event_id     UUID        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);
