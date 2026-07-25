BEGIN;

-- The authoritative deadline used by bid validation.
ALTER TABLE auctions
ADD COLUMN IF NOT EXISTS effective_close_time TIMESTAMPTZ;

-- Existing local-development rows predate Day 6.
-- Infinity is used only to backfill those historical rows.
UPDATE auctions
SET effective_close_time = 'infinity'::TIMESTAMPTZ
WHERE effective_close_time IS NULL;

-- Every newly created auction must now explicitly have a deadline.
ALTER TABLE auctions
ALTER COLUMN effective_close_time SET NOT NULL;

COMMIT;