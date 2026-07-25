package integration_test

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	postgresstore "example.com/bidlane/internal/store/postgres"
)

func BenchmarkDay03SequenceStrategies(
	b *testing.B,
) {
	ctx := context.Background()

	dsn := os.Getenv("POSTGRES_ADMIN_DSN")
	if dsn == "" {
		dsn = "postgres://bidlane:bidlane@" +
			"127.0.0.1:55432/bidlane?sslmode=disable"
	}

	adminPool, err := postgresstore.ConnectPool(
		ctx,
		dsn,
		"",
	)
	if err != nil {
		b.Fatalf(
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
		b.Fatalf(
			"connect Engine pool: %v",
			err,
		)
	}
	defer enginePool.Close()

	adminLedger := postgresstore.NewLedgerStore(
		adminPool,
	)
	engineLedger := postgresstore.NewLedgerStore(
		enginePool,
	)

	b.Run("select_for_update", func(b *testing.B) {
		resetDay03BenchmarkTables(
			b,
			ctx,
			adminPool,
		)

		auctionID := uuid.New()

		if _, err := adminPool.Exec(
			ctx,
			"INSERT INTO auctions (id) VALUES ($1)",
			auctionID,
		); err != nil {
			b.Fatalf(
				"create row-lock benchmark auction: %v",
				err,
			)
		}

		b.ResetTimer()

		for index := 0; index < b.N; index++ {
			if _, err := engineLedger.AppendBidRowLock(
				ctx,
				postgresstore.BidToPersist{
					AuctionID:      auctionID,
					BidderID:       uuid.New(),
					Amount:         int64(10_000 + index),
					IdempotencyKey: uuid.New(),
					StreamEntryID:  uuid.NewString(),
				},
			); err != nil {
				b.Fatalf(
					"row-lock bid %d: %v",
					index,
					err,
				)
			}
		}
	})

	b.Run("postgres_sequence", func(b *testing.B) {
		resetDay03BenchmarkTables(
			b,
			ctx,
			adminPool,
		)

		auctionID := uuid.New()

		if _, err := adminPool.Exec(
			ctx,
			"INSERT INTO auctions (id) VALUES ($1)",
			auctionID,
		); err != nil {
			b.Fatalf(
				"create sequence benchmark auction: %v",
				err,
			)
		}

		if err := adminLedger.EnsurePostgresSequence(
			ctx,
			auctionID,
		); err != nil {
			b.Fatalf(
				"create benchmark sequence: %v",
				err,
			)
		}

		defer func() {
			_ = adminLedger.DropPostgresSequence(
				context.Background(),
				auctionID,
			)
		}()

		b.ResetTimer()

		for index := 0; index < b.N; index++ {
			if _, err := engineLedger.AppendBidPostgresSequence(
				ctx,
				postgresstore.BidToPersist{
					AuctionID:      auctionID,
					BidderID:       uuid.New(),
					Amount:         int64(10_000 + index),
					IdempotencyKey: uuid.New(),
					StreamEntryID:  uuid.NewString(),
				},
			); err != nil {
				b.Fatalf(
					"sequence bid %d: %v",
					index,
					err,
				)
			}
		}
	})
}

func resetDay03BenchmarkTables(
	b *testing.B,
	ctx context.Context,
	adminPool *pgxpool.Pool,
) {
	b.Helper()

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
		b.Fatalf(
			"reset Day 3 benchmark tables: %v",
			err,
		)
	}
}
