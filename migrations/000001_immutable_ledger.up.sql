BEGIN;

-- Used for gen_random_uuid().
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- This is a capability/permission role.
-- It deliberately has NOLOGIN so no password is committed to source control.
-- A real login principal can later be granted membership in this role.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_roles
        WHERE rolname = 'bidlane_engine'
    ) THEN
        CREATE ROLE bidlane_engine NOLOGIN;
    END IF;
END
$$;

-- Day 2 creates only the minimum auction record required by the bid ledger.
-- The complete auction lifecycle schema arrives in later phases.
CREATE TABLE IF NOT EXISTS auctions (
    id          UUID PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bids (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    auction_id       UUID NOT NULL
                     REFERENCES auctions(id),

    sequence_no      BIGINT NOT NULL
                     CHECK (sequence_no > 0),

    amount           NUMERIC(20, 0) NOT NULL
                     CHECK (amount > 0),

    bidder_id        UUID NOT NULL,
    idempotency_key  UUID NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    stream_entry_id  TEXT NOT NULL,

    -- This is the permanent order within one auction.
    CONSTRAINT uq_bids_auction_sequence
        UNIQUE (auction_id, sequence_no)
);

-- This function rejects changes to an existing bid.
CREATE OR REPLACE FUNCTION reject_bid_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION
        USING
            ERRCODE = '55000',
            MESSAGE = 'bids is append-only (I1). Append a compensating event instead.';
END;
$$;

-- Dropping first makes this migration safely repeatable during local work.
DROP TRIGGER IF EXISTS bids_immutable ON bids;

CREATE TRIGGER bids_immutable
BEFORE UPDATE OR DELETE ON bids
FOR EACH ROW
EXECUTE FUNCTION reject_bid_mutation();

-- Remove accidental public permissions.
REVOKE ALL ON TABLE auctions FROM PUBLIC;
REVOKE ALL ON TABLE bids FROM PUBLIC;

GRANT USAGE ON SCHEMA public TO bidlane_engine;

-- The Engine can create/read auction records for the current phase.
GRANT SELECT, INSERT ON TABLE auctions TO bidlane_engine;

-- The ledger is append-only for the application.
GRANT SELECT, INSERT ON TABLE bids TO bidlane_engine;

REVOKE UPDATE, DELETE, TRUNCATE
ON TABLE bids
FROM bidlane_engine;

COMMIT;