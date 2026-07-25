BEGIN;

ALTER TABLE auctions
ADD COLUMN IF NOT EXISTS extension_window
    INTERVAL NOT NULL DEFAULT INTERVAL '30 seconds',

ADD COLUMN IF NOT EXISTS extension_interval
    INTERVAL NOT NULL DEFAULT INTERVAL '30 seconds',

ADD COLUMN IF NOT EXISTS max_extensions
    INTEGER NOT NULL DEFAULT 10,

ADD COLUMN IF NOT EXISTS extension_count
    INTEGER NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname =
            'auctions_extension_window_positive'
    ) THEN
        ALTER TABLE auctions
        ADD CONSTRAINT
            auctions_extension_window_positive
        CHECK (
            extension_window >
            INTERVAL '0 seconds'
        );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname =
            'auctions_extension_interval_positive'
    ) THEN
        ALTER TABLE auctions
        ADD CONSTRAINT
            auctions_extension_interval_positive
        CHECK (
            extension_interval >
            INTERVAL '0 seconds'
        );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname =
            'auctions_max_extensions_nonnegative'
    ) THEN
        ALTER TABLE auctions
        ADD CONSTRAINT
            auctions_max_extensions_nonnegative
        CHECK (
            max_extensions >= 0
        );
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname =
            'auctions_extension_count_valid'
    ) THEN
        ALTER TABLE auctions
        ADD CONSTRAINT
            auctions_extension_count_valid
        CHECK (
            extension_count >= 0
            AND
            extension_count <= max_extensions
        );
    END IF;
END
$$;

-- The Engine may modify only the anti-snipe fields.
-- It does not receive unrestricted UPDATE permission.
GRANT UPDATE (
    effective_close_time,
    extension_count
)
ON auctions
TO bidlane_engine;

COMMIT;