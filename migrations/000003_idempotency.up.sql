BEGIN;

-- One logical bid may appear only once in an auction,
-- regardless of how many times Redis delivers it.
CREATE UNIQUE INDEX IF NOT EXISTS
    uq_bids_auction_idempotency
ON bids (
    auction_id,
    idempotency_key
);

COMMIT;