package integration_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"example.com/bidlane/internal/engine"
	postgresstore "example.com/bidlane/internal/store/postgres"
	redisstore "example.com/bidlane/internal/store/redis"
)

func TestDay04DuplicateBidIsPersistedExactlyOnce(
	t *testing.T,
) {
	const duplicateAttempts = 50

	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
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
			"connect PostgreSQL administrator pool: %v",
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
			"connect restricted Engine pool: %v",
			err,
		)
	}
	defer enginePool.Close()

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
			"reset Day 4 tables: %v\n"+
				"Did you apply migrations 000002 and 000003?",
			err,
		)
	}

	auctionID := uuid.New()
	bidderID := uuid.New()
	idempotencyKey := uuid.New()

	if _, err := adminPool.Exec(
		ctx,
		`
			INSERT INTO auctions (id)
			VALUES ($1)
		`,
		auctionID,
	); err != nil {
		t.Fatalf(
			"create Day 4 auction: %v",
			err,
		)
	}

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

	auctionIDText := auctionID.String()

	if err := streams.DeleteStream(
		ctx,
		auctionIDText,
	); err != nil {
		t.Fatalf(
			"delete old Day 4 stream: %v",
			err,
		)
	}

	// This defer runs before the pools and Redis client close.
	defer func() {
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
	}()

	service := engine.NewService(streams)

	start := make(chan struct{})
	errorChannel := make(
		chan error,
		duplicateAttempts,
	)

	var waitGroup sync.WaitGroup
	waitGroup.Add(duplicateAttempts)

	for index := 0; index < duplicateAttempts; index++ {
		index := index

		go func() {
			defer waitGroup.Done()

			<-start

			_, err := service.
				ReserveBidWithIdempotencyKey(
					ctx,
					auctionIDText,
					bidderID.String(),
					50_000,
					idempotencyKey.String(),
				)
			if err != nil {
				errorChannel <- fmt.Errorf(
					"duplicate attempt %d: %w",
					index,
					err,
				)
			}
		}()
	}

	// Release all retry attempts together.
	close(start)

	waitGroup.Wait()
	close(errorChannel)

	for submissionError := range errorChannel {
		t.Fatal(submissionError)
	}

	// There should be 50 physical Redis entries.
	streamLength, err := redisClient.XLen(
		ctx,
		redisstore.StreamKey(auctionIDText),
	).Result()
	if err != nil {
		t.Fatalf(
			"read Redis stream length: %v",
			err,
		)
	}

	if streamLength != duplicateAttempts {
		t.Fatalf(
			"expected %d Redis entries, got %d",
			duplicateAttempts,
			streamLength,
		)
	}

	ledger := postgresstore.NewLedgerStore(
		enginePool,
	)

	logger := slog.New(
		slog.NewTextHandler(
			io.Discard,
			nil,
		),
	)

	firstGroup := "cg:day04:first:" +
		uuid.NewString()

	firstConsumer := engine.NewLedgerConsumer(
		streams,
		ledger,
		logger,
		engine.LedgerConsumerConfig{
			AuctionID:   auctionIDText,
			Group:       firstGroup,
			Consumer:    "day04-consumer-1",
			BatchSize:   10,
			BlockPeriod: 500 * time.Millisecond,
		},
	)

	// First delivery of all 50 Redis entries.
	if err := firstConsumer.ProcessExactly(
		ctx,
		duplicateAttempts,
	); err != nil {
		t.Fatalf(
			"process first Day 4 delivery: %v",
			err,
		)
	}

	assertDay04SingleLedgerRow(
		t,
		ctx,
		adminPool,
		auctionID,
		idempotencyKey,
	)

	firstPending, err := redisClient.XPending(
		ctx,
		redisstore.StreamKey(auctionIDText),
		firstGroup,
	).Result()
	if err != nil {
		t.Fatalf(
			"read first consumer pending entries: %v",
			err,
		)
	}

	if firstPending.Count != 0 {
		t.Fatalf(
			"first consumer group has %d pending entries; duplicates were not all acknowledged",
			firstPending.Count,
		)
	}

	// A new consumer group starts at stream ID 0.
	// It therefore receives the same historical stream entries,
	// replaying all 50 exact Redis entries.
	secondGroup := "cg:day04:replay:" +
		uuid.NewString()

	replayConsumer := engine.NewLedgerConsumer(
		streams,
		ledger,
		logger,
		engine.LedgerConsumerConfig{
			AuctionID:   auctionIDText,
			Group:       secondGroup,
			Consumer:    "day04-replay-consumer",
			BatchSize:   10,
			BlockPeriod: 500 * time.Millisecond,
		},
	)

	if err := replayConsumer.ProcessExactly(
		ctx,
		duplicateAttempts,
	); err != nil {
		t.Fatalf(
			"replay the same Redis entries: %v",
			err,
		)
	}

	// Replaying every stream entry must still leave one row
	// and must not advance the sequence counter.
	assertDay04SingleLedgerRow(
		t,
		ctx,
		adminPool,
		auctionID,
		idempotencyKey,
	)

	secondPending, err := redisClient.XPending(
		ctx,
		redisstore.StreamKey(auctionIDText),
		secondGroup,
	).Result()
	if err != nil {
		t.Fatalf(
			"read replay consumer pending entries: %v",
			err,
		)
	}

	if secondPending.Count != 0 {
		t.Fatalf(
			"replay consumer group has %d pending entries",
			secondPending.Count,
		)
	}

	t.Logf(
		"Day 4 passed: %d Redis deliveries plus replay, "+
			"one PostgreSQL row, sequence counter 1, "+
			"all entries acknowledged",
		duplicateAttempts,
	)
}

func assertDay04SingleLedgerRow(
	t *testing.T,
	ctx context.Context,
	adminPool *pgxpool.Pool,
	auctionID uuid.UUID,
	idempotencyKey uuid.UUID,
) {
	t.Helper()

	var (
		rowCount                int64
		distinctIdempotencyKeys int64
		minimumSequence         int64
		maximumSequence         int64
	)

	if err := adminPool.QueryRow(
		ctx,
		`
			SELECT
				COUNT(*),
				COUNT(DISTINCT idempotency_key),
				MIN(sequence_no),
				MAX(sequence_no)
			FROM bids
			WHERE auction_id = $1
		`,
		auctionID,
	).Scan(
		&rowCount,
		&distinctIdempotencyKeys,
		&minimumSequence,
		&maximumSequence,
	); err != nil {
		t.Fatalf(
			"query Day 4 ledger state: %v",
			err,
		)
	}

	if rowCount != 1 {
		t.Fatalf(
			"expected exactly one ledger row, got %d",
			rowCount,
		)
	}

	if distinctIdempotencyKeys != 1 {
		t.Fatalf(
			"expected one idempotency key, got %d",
			distinctIdempotencyKeys,
		)
	}

	if minimumSequence != 1 ||
		maximumSequence != 1 {
		t.Fatalf(
			"expected only sequence 1, got min=%d max=%d",
			minimumSequence,
			maximumSequence,
		)
	}

	var storedKey uuid.UUID

	if err := adminPool.QueryRow(
		ctx,
		`
			SELECT idempotency_key
			FROM bids
			WHERE auction_id = $1
		`,
		auctionID,
	).Scan(&storedKey); err != nil {
		t.Fatalf(
			"read stored idempotency key: %v",
			err,
		)
	}

	if storedKey != idempotencyKey {
		t.Fatalf(
			"stored idempotency key %s, expected %s",
			storedKey,
			idempotencyKey,
		)
	}

	var counterValue int64

	if err := adminPool.QueryRow(
		ctx,
		`
			SELECT last_seq
			FROM auction_sequences
			WHERE auction_id = $1
		`,
		auctionID,
	).Scan(&counterValue); err != nil {
		t.Fatalf(
			"read Day 4 sequence counter: %v",
			err,
		)
	}

	if counterValue != 1 {
		t.Fatalf(
			"duplicate attempts consumed sequence numbers: expected counter 1, got %d",
			counterValue,
		)
	}
}
