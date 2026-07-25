BEGIN;

-- One counter row exists for each auction.
--
-- This row is locked while a bid receives its permanent sequence
-- number. Different auctions have different rows and therefore do
-- not block each other.
CREATE TABLE IF NOT EXISTS auction_sequences (
    auction_id UUID PRIMARY KEY
               REFERENCES auctions(id)
               ON DELETE RESTRICT,

    last_seq   BIGINT NOT NULL DEFAULT 0
               CHECK (last_seq >= 0)
);

REVOKE ALL
ON TABLE auction_sequences
FROM PUBLIC;

-- The Bid Engine must be able to:
--   INSERT the initial counter row,
--   SELECT and lock the row,
--   UPDATE the counter after assigning a sequence.
GRANT SELECT, INSERT, UPDATE
ON TABLE auction_sequences
TO bidlane_engine;

COMMIT;