BEGIN;

CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    event_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    aggregate_sequence BIGINT NOT NULL,

    payload JSONB NOT NULL,

    created_at TIMESTAMPTZ NOT NULL
        DEFAULT clock_timestamp(),

    published_at TIMESTAMPTZ,

    publish_attempts INTEGER NOT NULL
        DEFAULT 0,

    CONSTRAINT outbox_event_type_not_blank
        CHECK (btrim(event_type) <> ''),

    CONSTRAINT outbox_aggregate_type_not_blank
        CHECK (btrim(aggregate_type) <> ''),

    CONSTRAINT outbox_aggregate_sequence_positive
        CHECK (aggregate_sequence > 0),

    CONSTRAINT outbox_payload_is_object
        CHECK (jsonb_typeof(payload) = 'object'),

    CONSTRAINT outbox_publish_attempts_nonnegative
        CHECK (publish_attempts >= 0),

    -- One event for one accepted bid sequence.
    CONSTRAINT uq_outbox_event
        UNIQUE (
            event_type,
            aggregate_id,
            aggregate_sequence
        )
);

-- The relay searches only unpublished rows.
CREATE INDEX IF NOT EXISTS
    ix_outbox_unpublished
ON outbox (
    created_at,
    id
)
WHERE published_at IS NULL;

-- Separate permission role for the relay process.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_roles
        WHERE rolname = 'bidlane_outbox_relay'
    ) THEN
        CREATE ROLE bidlane_outbox_relay NOLOGIN;
    END IF;
END
$$;

REVOKE ALL
ON TABLE outbox
FROM PUBLIC;

-- The Bid Engine may create events but may not mark them published.
GRANT INSERT
ON TABLE outbox
TO bidlane_engine;

-- The relay may read and mark events, but cannot create bid events.
GRANT SELECT
ON TABLE outbox
TO bidlane_outbox_relay;

GRANT UPDATE (
    published_at,
    publish_attempts
)
ON TABLE outbox
TO bidlane_outbox_relay;

COMMIT;