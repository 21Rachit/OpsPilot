-- Events waiting to be published to Kafka. Rows are written in the SAME transaction
-- as the incident change, so an event is saved if and only if the change is saved.
CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL   PRIMARY KEY,
    event_id     UUID        NOT NULL DEFAULT gen_random_uuid(),
    event_type   TEXT        NOT NULL,
    incident_id  BIGINT      NOT NULL,  -- no foreign key: "deleted" events outlive their incident
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ            -- NULL until the relay has sent it to Kafka
);

-- The relay only ever looks for unpublished rows, so index just those.
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
