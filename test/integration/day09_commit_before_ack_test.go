package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"example.com/bidlane/internal/engine"
	postgresstore "example.com/bidlane/internal/store/postgres"
	redisstore "example.com/bidlane/internal/store/redis"
)

func TestDay09PostgresCommitAlwaysPrecedesRedisAck(
	t *testing.T,
) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	postgresDSN := os.Getenv(
		"POSTGRES_ADMIN_DSN",
	)
	if postgresDSN == "" {
		postgresDSN =
			"postgres://bidlane:bidlane@" +
				"127.0.0.1:55432/" +
				"bidlane?sslmode=disable"
	}

	redisAddress := os.Getenv(
		"REDIS_ADDR",
	)
	if redisAddress == "" {
		redisAddress = "localhost:6379"
	}

	adminPool, err := postgresstore.ConnectPool(
		ctx,
		postgresDSN,
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
		postgresDSN,
		"bidlane_engine",
	)
	if err != nil {
		t.Fatalf(
			"connect Engine pool: %v",
			err,
		)
	}
	defer enginePool.Close()

	redisClient := redisstore.NewClient(
		redisAddress,
		"",
		0,
	)
	defer redisClient.Close()

	streams := redisstore.NewStreamStore(
		redisClient,
	)

	if err := streams.Ping(ctx); err != nil {
		t.Fatalf(
			"Redis unavailable at %s: %v",
			redisAddress,
			err,
		)
	}

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
			"reset Day 9 tables: %v",
			err,
		)
	}

	auctionID := uuid.New()
	bidderID := uuid.New()
	idempotencyKey := uuid.New()
	auctionIDText := auctionID.String()

	effectiveCloseTime :=
		time.Now().
			UTC().
			Add(24 * time.Hour)

	if _, err := adminPool.Exec(
		ctx,
		`
			INSERT INTO auctions (
				id,
				effective_close_time
			)
			VALUES ($1, $2)
		`,
		auctionID,
		effectiveCloseTime,
	); err != nil {
		t.Fatalf(
			"create Day 9 auction: %v",
			err,
		)
	}

	if err := streams.DeleteStream(
		ctx,
		auctionIDText,
	); err != nil {
		t.Fatalf(
			"delete old Day 9 stream: %v",
			err,
		)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel :=
			context.WithTimeout(
				context.Background(),
				10*time.Second,
			)
		defer cleanupCancel()

		_ = streams.DeleteStream(
			cleanupCtx,
			auctionIDText,
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
	})

	service := engine.NewService(streams)

	streamEntryID, err :=
		service.ReserveBidWithIdempotencyKey(
			ctx,
			auctionIDText,
			bidderID.String(),
			50_000,
			idempotencyKey.String(),
		)
	if err != nil {
		t.Fatalf(
			"submit Day 9 bid: %v",
			err,
		)
	}

	group :=
		"cg:day09:" + uuid.NewString()

	injectedFailure := errors.New(
		"Day 9 injected failure after commit and before XACK",
	)

	boundaryObserved := false

	consumer := engine.NewLedgerConsumer(
		streams,
		postgresstore.NewLedgerStore(
			enginePool,
		),
		slog.New(
			slog.NewTextHandler(
				io.Discard,
				nil,
			),
		),
		engine.LedgerConsumerConfig{
			AuctionID:   auctionIDText,
			Group:       group,
			Consumer:    "day09-consumer-1",
			BatchSize:   1,
			BlockPeriod: 250 * time.Millisecond,

			BeforeAck: func(
				hookContext context.Context,
				entry redisstore.BidStreamEntry,
			) error {
				if entry.ID != streamEntryID {
					return fmt.Errorf(
						"hook received stream entry %s, expected %s",
						entry.ID,
						streamEntryID,
					)
				}

				// The bid must already be visible from another
				// PostgreSQL connection. This proves COMMIT has
				// completed before the XACK boundary.
				var ledgerRows int64

				if err := adminPool.QueryRow(
					hookContext,
					`
						SELECT COUNT(*)
						FROM bids
						WHERE auction_id = $1
						  AND stream_entry_id = $2
					`,
					auctionID,
					entry.ID,
				).Scan(&ledgerRows); err != nil {
					return fmt.Errorf(
						"query ledger at pre-XACK boundary: %w",
						err,
					)
				}

				if ledgerRows != 1 {
					return fmt.Errorf(
						"PostgreSQL bid is not committed before XACK: rows=%d",
						ledgerRows,
					)
				}

				// The Redis entry must still be pending. This proves
				// XACK has not happened yet.
				pending, err := redisClient.XPending(
					hookContext,
					redisstore.StreamKey(
						auctionIDText,
					),
					group,
				).Result()
				if err != nil {
					return fmt.Errorf(
						"query Redis pending state before XACK: %w",
						err,
					)
				}

				if pending.Count != 1 {
					return fmt.Errorf(
						"expected one pending entry before XACK, got %d",
						pending.Count,
					)
				}

				boundaryObserved = true

				// Stop processing before XACK.
				return injectedFailure
			},
		},
	)

	err = consumer.ProcessExactly(
		ctx,
		1,
	)
	if err == nil {
		t.Fatal(
			"expected injected failure before Redis XACK",
		)
	}

	if !errors.Is(
		err,
		injectedFailure,
	) {
		t.Fatalf(
			"expected injected failure %v, got %v",
			injectedFailure,
			err,
		)
	}

	if !boundaryObserved {
		t.Fatal(
			"commit-before-XACK boundary was not observed",
		)
	}

	// The failure occurred after commit, so the bid must remain
	// safely stored in PostgreSQL.
	var rowsAfterFailure int64

	if err := adminPool.QueryRow(
		ctx,
		`
			SELECT COUNT(*)
			FROM bids
			WHERE auction_id = $1
		`,
		auctionID,
	).Scan(&rowsAfterFailure); err != nil {
		t.Fatalf(
			"count bids after injected failure: %v",
			err,
		)
	}

	if rowsAfterFailure != 1 {
		t.Fatalf(
			"expected one committed bid after failure, got %d",
			rowsAfterFailure,
		)
	}

	// Because XACK did not occur, Redis must retain the entry
	// for crash recovery.
	pendingAfterFailure, err :=
		redisClient.XPending(
			ctx,
			redisstore.StreamKey(
				auctionIDText,
			),
			group,
		).Result()
	if err != nil {
		t.Fatalf(
			"read pending state after failure: %v",
			err,
		)
	}

	if pendingAfterFailure.Count != 1 {
		t.Fatalf(
			"expected one pending entry after failure, got %d",
			pendingAfterFailure.Count,
		)
	}

	// Demonstrate that the pending delivery can still be safely
	// recovered using Day 8 XAUTOCLAIM logic.
	time.Sleep(10 * time.Millisecond)

	restartConsumer := engine.NewLedgerConsumer(
		streams,
		postgresstore.NewLedgerStore(
			enginePool,
		),
		slog.New(
			slog.NewTextHandler(
				io.Discard,
				nil,
			),
		),
		engine.LedgerConsumerConfig{
			AuctionID: auctionIDText,
			Group:     group,
			Consumer: "day09-restart-" +
				uuid.NewString(),
			BatchSize:         1,
			BlockPeriod:       250 * time.Millisecond,
			RecoveryMinIdle:   time.Millisecond,
			RecoveryBatchSize: 1,
		},
	)

	recovered, err :=
		restartConsumer.RecoverPending(ctx)
	if err != nil {
		t.Fatalf(
			"recover Day 9 pending entry: %v",
			err,
		)
	}

	if recovered != 1 {
		t.Fatalf(
			"expected one recovered entry, got %d",
			recovered,
		)
	}

	var finalRows int64

	if err := adminPool.QueryRow(
		ctx,
		`
			SELECT COUNT(*)
			FROM bids
			WHERE auction_id = $1
		`,
		auctionID,
	).Scan(&finalRows); err != nil {
		t.Fatalf(
			"count final Day 9 ledger rows: %v",
			err,
		)
	}

	if finalRows != 1 {
		t.Fatalf(
			"idempotent recovery created duplicates: expected 1 row, got %d",
			finalRows,
		)
	}

	finalPending, err :=
		redisClient.XPending(
			ctx,
			redisstore.StreamKey(
				auctionIDText,
			),
			group,
		).Result()
	if err != nil {
		t.Fatalf(
			"read final pending state: %v",
			err,
		)
	}

	if finalPending.Count != 0 {
		t.Fatalf(
			"expected zero pending entries after recovery, got %d",
			finalPending.Count,
		)
	}

	t.Log(
		"Day 9 passed: PostgreSQL row was committed before the XACK boundary; " +
			"injected failure left the entry pending; recovery acknowledged it without duplication",
	)
}
