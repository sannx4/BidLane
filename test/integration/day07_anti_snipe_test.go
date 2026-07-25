package integration_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	postgresstore "example.com/bidlane/internal/store/postgres"
)

type day07AppendOutcome struct {
	result postgresstore.AntiSnipeAppendResult
	err    error
}

func TestDay07BidAndExtensionAreAtomic(
	t *testing.T,
) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		45*time.Second,
	)
	defer cancel()

	dsn := os.Getenv("POSTGRES_ADMIN_DSN")
	if dsn == "" {
		dsn =
			"postgres://bidlane:bidlane@" +
				"127.0.0.1:55432/" +
				"bidlane?sslmode=disable"
	}

	adminPool, err := postgresstore.ConnectPool(
		ctx,
		dsn,
		"",
	)
	if err != nil {
		t.Fatalf(
			"connect administrator pool: %v",
			err,
		)
	}
	defer adminPool.Close()

	enginePool, err := postgresstore.ConnectPool(
		ctx,
		dsn,
		"bidlane_engine",
	)
	if err != nil {
		t.Fatalf(
			"connect Engine pool: %v",
			err,
		)
	}
	defer enginePool.Close()

	cleanupDay07Objects(
		t,
		ctx,
		adminPool,
	)

	if _, err := adminPool.Exec(
		ctx,
		`
			TRUNCATE TABLE
				bids,
				auction_sequences,
				auctions
			CASCADE
		`,
	); err != nil {
		t.Fatalf(
			"reset Day 7 tables: %v\n"+
				"Did you apply migration 000005?",
			err,
		)
	}

	defer func() {
		cleanupCtx, cleanupCancel :=
			context.WithTimeout(
				context.Background(),
				10*time.Second,
			)
		defer cleanupCancel()

		cleanupDay07Objects(
			t,
			cleanupCtx,
			adminPool,
		)

		_, _ = adminPool.Exec(
			cleanupCtx,
			`
				TRUNCATE TABLE
					bids,
					auction_sequences,
					auctions
				CASCADE
			`,
		)
	}()

	ledger := postgresstore.NewLedgerStore(
		enginePool,
	)

	t.Run(
		"another transaction cannot observe only the bid or only the extension",
		func(t *testing.T) {
			auctionID := uuid.New()

			initialCloseTime := time.Now().
				UTC().
				Truncate(time.Microsecond).
				Add(10 * time.Second)

			if _, err := adminPool.Exec(
				ctx,
				`
					INSERT INTO auctions (
						id,
						effective_close_time,
						extension_window,
						extension_interval,
						max_extensions,
						extension_count
					)
					VALUES (
						$1,
						$2,
						INTERVAL '30 seconds',
						INTERVAL '30 seconds',
						10,
						0
					)
				`,
				auctionID,
				initialCloseTime,
			); err != nil {
				t.Fatalf(
					"create Day 7 auction: %v",
					err,
				)
			}

			// This trigger pauses the transaction precisely when it
			// attempts to apply the extension.
			//
			// It allows another database connection to inspect what
			// is visible while the bid transaction is incomplete.
			if _, err := adminPool.Exec(
				ctx,
				`
					CREATE OR REPLACE FUNCTION
						day07_wait_on_extension()
					RETURNS trigger
					LANGUAGE plpgsql
					AS $$
					BEGIN
						IF NEW.extension_count >
						   OLD.extension_count THEN
							PERFORM
								pg_advisory_xact_lock(7007);
						END IF;

						RETURN NEW;
					END;
					$$;

					DROP TRIGGER IF EXISTS
						day07_wait_on_extension
					ON auctions;

					CREATE TRIGGER
						day07_wait_on_extension
					BEFORE UPDATE OF
						effective_close_time,
						extension_count
					ON auctions
					FOR EACH ROW
					EXECUTE FUNCTION
						day07_wait_on_extension();
				`,
			); err != nil {
				t.Fatalf(
					"create Day 7 pause trigger: %v",
					err,
				)
			}

			// Hold the advisory lock from a separate database
			// session. The anti-snipe transaction will pause inside
			// its extension UPDATE.
			blocker, err := adminPool.Acquire(ctx)
			if err != nil {
				t.Fatalf(
					"acquire blocker connection: %v",
					err,
				)
			}
			defer blocker.Release()

			if _, err := blocker.Exec(
				ctx,
				"SELECT pg_advisory_lock(7007)",
			); err != nil {
				t.Fatalf(
					"acquire Day 7 advisory lock: %v",
					err,
				)
			}

			bid := postgresstore.BidToPersist{
				AuctionID:      auctionID,
				BidderID:       uuid.New(),
				Amount:         50_000,
				IdempotencyKey: uuid.New(),
				StreamEntryID:  "1900000000000-0",
			}

			submittedAt :=
				initialCloseTime.Add(-5 * time.Second)

			outcomeChannel :=
				make(chan day07AppendOutcome, 1)

			go func() {
				result, appendErr :=
					ledger.
						AppendBidIdempotentWithAntiSnipe(
							ctx,
							bid,
							submittedAt,
						)

				outcomeChannel <- day07AppendOutcome{
					result: result,
					err:    appendErr,
				}
			}()

			// Wait until the bid transaction reaches the blocked
			// extension operation.
			if err := waitForDay07AdvisoryWaiter(
				ctx,
				adminPool,
			); err != nil {
				t.Fatal(err)
			}

			// While the transaction is paused, another connection
			// must see neither change.
			var visibleBidCount int64

			if err := adminPool.QueryRow(
				ctx,
				`
					SELECT COUNT(*)
					FROM bids
					WHERE auction_id = $1
				`,
				auctionID,
			).Scan(&visibleBidCount); err != nil {
				t.Fatalf(
					"query uncommitted bid visibility: %v",
					err,
				)
			}

			if visibleBidCount != 0 {
				t.Fatalf(
					"another transaction observed %d uncommitted bids",
					visibleBidCount,
				)
			}

			var (
				visibleCloseTime time.Time
				visibleCount     int32
			)

			if err := adminPool.QueryRow(
				ctx,
				`
					SELECT
						effective_close_time,
						extension_count
					FROM auctions
					WHERE id = $1
				`,
				auctionID,
			).Scan(
				&visibleCloseTime,
				&visibleCount,
			); err != nil {
				t.Fatalf(
					"query uncommitted extension visibility: %v",
					err,
				)
			}

			if !visibleCloseTime.Equal(
				initialCloseTime,
			) {
				t.Fatalf(
					"extension became visible before commit: got %s expected %s",
					visibleCloseTime,
					initialCloseTime,
				)
			}

			if visibleCount != 0 {
				t.Fatalf(
					"extension count became visible before commit: %d",
					visibleCount,
				)
			}

			// Release the transaction so that it can commit.
			if _, err := blocker.Exec(
				ctx,
				"SELECT pg_advisory_unlock(7007)",
			); err != nil {
				t.Fatalf(
					"release Day 7 advisory lock: %v",
					err,
				)
			}

			outcome := <-outcomeChannel

			if outcome.err != nil {
				t.Fatalf(
					"append anti-snipe bid: %v",
					outcome.err,
				)
			}

			if !outcome.result.Inserted {
				t.Fatal(
					"new bid was not inserted",
				)
			}

			if !outcome.result.Extended {
				t.Fatal(
					"inside-window bid did not extend auction",
				)
			}

			expectedCloseTime :=
				initialCloseTime.Add(30 * time.Second)

			if !outcome.result.
				EffectiveCloseTime.
				Equal(expectedCloseTime) {
				t.Fatalf(
					"expected close time %s, got %s",
					expectedCloseTime,
					outcome.result.EffectiveCloseTime,
				)
			}

			assertDay07CommittedState(
				t,
				ctx,
				adminPool,
				auctionID,
				1,
				expectedCloseTime,
				1,
			)

			// Replay the same logical bid.
			//
			// It must not create another bid and must not apply
			// another extension.
			replayResult, err :=
				ledger.
					AppendBidIdempotentWithAntiSnipe(
						ctx,
						bid,
						submittedAt,
					)
			if err != nil {
				t.Fatalf(
					"replay idempotent bid: %v",
					err,
				)
			}

			if replayResult.Inserted {
				t.Fatal(
					"duplicate replay created another bid",
				)
			}

			if replayResult.Extended {
				t.Fatal(
					"duplicate replay extended auction again",
				)
			}

			assertDay07CommittedState(
				t,
				ctx,
				adminPool,
				auctionID,
				1,
				expectedCloseTime,
				1,
			)
		},
	)

	t.Run(
		"forced commit failure rolls back both bid and extension",
		func(t *testing.T) {
			auctionID := uuid.New()

			initialCloseTime := time.Now().
				UTC().
				Truncate(time.Microsecond).
				Add(10 * time.Second)

			if _, err := adminPool.Exec(
				ctx,
				`
					INSERT INTO auctions (
						id,
						effective_close_time,
						extension_window,
						extension_interval,
						max_extensions,
						extension_count
					)
					VALUES (
						$1,
						$2,
						INTERVAL '30 seconds',
						INTERVAL '30 seconds',
						10,
						0
					)
				`,
				auctionID,
				initialCloseTime,
			); err != nil {
				t.Fatalf(
					"create rollback auction: %v",
					err,
				)
			}

			// This deferred trigger fails when COMMIT is attempted.
			//
			// At that point the transaction has already inserted
			// the bid and updated the auction, so the failure proves
			// PostgreSQL rolls both changes back together.
			if _, err := adminPool.Exec(
				ctx,
				`
					CREATE OR REPLACE FUNCTION
						day07_force_commit_failure()
					RETURNS trigger
					LANGUAGE plpgsql
					AS $$
					BEGIN
						IF NEW.amount = 77777 THEN
							RAISE EXCEPTION
								'Day 7 forced commit failure';
						END IF;

						RETURN NEW;
					END;
					$$;

					DROP TRIGGER IF EXISTS
						day07_force_commit_failure
					ON bids;

					CREATE CONSTRAINT TRIGGER
						day07_force_commit_failure
					AFTER INSERT
					ON bids
					DEFERRABLE
					INITIALLY DEFERRED
					FOR EACH ROW
					EXECUTE FUNCTION
						day07_force_commit_failure();
				`,
			); err != nil {
				t.Fatalf(
					"create forced-failure trigger: %v",
					err,
				)
			}

			_, appendErr :=
				ledger.
					AppendBidIdempotentWithAntiSnipe(
						ctx,
						postgresstore.BidToPersist{
							AuctionID: auctionID,
							BidderID:  uuid.New(),
							Amount:    77_777,

							IdempotencyKey: uuid.New(),
							StreamEntryID:  "1900000000001-0",
						},
						initialCloseTime.
							Add(-5*time.Second),
					)

			if appendErr == nil {
				t.Fatal(
					"expected forced transaction failure",
				)
			}

			var bidCount int64

			if err := adminPool.QueryRow(
				ctx,
				`
					SELECT COUNT(*)
					FROM bids
					WHERE auction_id = $1
				`,
				auctionID,
			).Scan(&bidCount); err != nil {
				t.Fatalf(
					"count rolled-back bids: %v",
					err,
				)
			}

			if bidCount != 0 {
				t.Fatalf(
					"failed transaction left %d bid rows",
					bidCount,
				)
			}

			var (
				storedCloseTime time.Time
				storedCount     int32
			)

			if err := adminPool.QueryRow(
				ctx,
				`
					SELECT
						effective_close_time,
						extension_count
					FROM auctions
					WHERE id = $1
				`,
				auctionID,
			).Scan(
				&storedCloseTime,
				&storedCount,
			); err != nil {
				t.Fatalf(
					"read rolled-back auction state: %v",
					err,
				)
			}

			if !storedCloseTime.Equal(
				initialCloseTime,
			) {
				t.Fatalf(
					"extension survived failed transaction: got %s expected %s",
					storedCloseTime,
					initialCloseTime,
				)
			}

			if storedCount != 0 {
				t.Fatalf(
					"extension count survived failed transaction: %d",
					storedCount,
				)
			}

			var sequenceRows int64

			if err := adminPool.QueryRow(
				ctx,
				`
					SELECT COUNT(*)
					FROM auction_sequences
					WHERE auction_id = $1
				`,
				auctionID,
			).Scan(&sequenceRows); err != nil {
				t.Fatalf(
					"count rolled-back sequence rows: %v",
					err,
				)
			}

			if sequenceRows != 0 {
				t.Fatalf(
					"failed transaction left %d sequence rows",
					sequenceRows,
				)
			}
		},
	)

	t.Log(
		"Day 7 passed: accepted inside-window bid and extension " +
			"became visible together; forced failure rolled both back; " +
			"duplicate replay applied no second extension",
	)
}

func waitForDay07AdvisoryWaiter(
	ctx context.Context,
	pool *pgxpool.Pool,
) error {
	ticker := time.NewTicker(
		20 * time.Millisecond,
	)
	defer ticker.Stop()

	timeout := time.NewTimer(
		5 * time.Second,
	)
	defer timeout.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-timeout.C:
			return errors.New(
				"anti-snipe transaction did not reach the advisory-lock barrier",
			)

		case <-ticker.C:
			var waiting bool

			if err := pool.QueryRow(
				ctx,
				`
					SELECT EXISTS (
						SELECT 1
						FROM pg_locks
						WHERE locktype = 'advisory'
						  AND granted = false
					)
				`,
			).Scan(&waiting); err != nil {
				return err
			}

			if waiting {
				return nil
			}
		}
	}
}

func assertDay07CommittedState(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	auctionID uuid.UUID,
	expectedBids int64,
	expectedCloseTime time.Time,
	expectedExtensionCount int32,
) {
	t.Helper()

	var bidCount int64

	if err := pool.QueryRow(
		ctx,
		`
			SELECT COUNT(*)
			FROM bids
			WHERE auction_id = $1
		`,
		auctionID,
	).Scan(&bidCount); err != nil {
		t.Fatalf(
			"count Day 7 bids: %v",
			err,
		)
	}

	if bidCount != expectedBids {
		t.Fatalf(
			"expected %d bids, got %d",
			expectedBids,
			bidCount,
		)
	}

	var (
		closeTime      time.Time
		extensionCount int32
	)

	if err := pool.QueryRow(
		ctx,
		`
			SELECT
				effective_close_time,
				extension_count
			FROM auctions
			WHERE id = $1
		`,
		auctionID,
	).Scan(
		&closeTime,
		&extensionCount,
	); err != nil {
		t.Fatalf(
			"read Day 7 auction state: %v",
			err,
		)
	}

	if !closeTime.Equal(expectedCloseTime) {
		t.Fatalf(
			"expected close time %s, got %s",
			expectedCloseTime,
			closeTime,
		)
	}

	if extensionCount != expectedExtensionCount {
		t.Fatalf(
			"expected extension count %d, got %d",
			expectedExtensionCount,
			extensionCount,
		)
	}
}

func cleanupDay07Objects(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
) {
	t.Helper()

	if _, err := pool.Exec(
		ctx,
		`
			DROP TRIGGER IF EXISTS
				day07_wait_on_extension
			ON auctions;

			DROP FUNCTION IF EXISTS
				day07_wait_on_extension();

			DROP TRIGGER IF EXISTS
				day07_force_commit_failure
			ON bids;

			DROP FUNCTION IF EXISTS
				day07_force_commit_failure();
		`,
	); err != nil {
		t.Fatalf(
			"clean Day 7 test objects: %v",
			err,
		)
	}
}
