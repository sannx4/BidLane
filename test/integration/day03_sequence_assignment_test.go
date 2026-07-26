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

	"example.com/bidlane/internal/engine"
	postgresstore "example.com/bidlane/internal/store/postgres"
	redisstore "example.com/bidlane/internal/store/redis"
)

func TestDay03OneThousandBidsReceiveGapFreeSequences(
	t *testing.T,
) {
	const totalBids = 1_000

	ctx, cancel := context.WithTimeout(
		context.Background(),
		90*time.Second,
	)
	defer cancel()

	postgresDSN := os.Getenv("POSTGRES_ADMIN_DSN")
	if postgresDSN == "" {
		postgresDSN = "postgres://bidlane:bidlane@" +
			"127.0.0.1:55432/bidlane?sslmode=disable"
	}

	redisAddress := os.Getenv("REDIS_ADDR")
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
			"clean Day 3 PostgreSQL tables: %v\n"+
				"Did you apply migration 000002?",
			err,
		)
	}

	auctionID := uuid.New()

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
		time.Now().UTC().Add(24*time.Hour),
	); err != nil {
		t.Fatalf(
			"create Day 3 auction: %v",
			err,
		)
	}

	redisClient := redisstore.NewClient(
		redisAddress,
		"",
		0,
	)
	defer redisClient.Close()

	streams := redisstore.NewStreamStore(redisClient)

	if err := streams.Ping(ctx); err != nil {
		t.Fatalf(
			"Redis unavailable at %s: %v",
			redisAddress,
			err,
		)
	}

	auctionIDText := auctionID.String()

	// Remove any old local stream using the same key.
	if err := streams.DeleteStream(
		ctx,
		auctionIDText,
	); err != nil {
		t.Fatalf(
			"delete pre-existing Day 3 stream: %v",
			err,
		)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
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

	start := make(chan struct{})
	errorsChannel := make(chan error, totalBids)

	var waitGroup sync.WaitGroup
	waitGroup.Add(totalBids)

	for index := 0; index < totalBids; index++ {
		index := index

		go func() {
			defer waitGroup.Done()

			<-start

			bidderID := uuid.NewString()
			amount := int64(10_000 + index)

			if _, err := service.ReserveBid(
				ctx,
				auctionIDText,
				bidderID,
				amount,
			); err != nil {
				errorsChannel <- fmt.Errorf(
					"reserve bid %d: %w",
					index,
					err,
				)
			}
		}()
	}

	// Release all 1,000 bid goroutines together.
	close(start)

	waitGroup.Wait()
	close(errorsChannel)

	for submissionError := range errorsChannel {
		t.Fatal(submissionError)
	}

	ledger := postgresstore.NewLedgerStore(enginePool)

	logger := slog.New(
		slog.NewTextHandler(
			io.Discard,
			nil,
		),
	)

	consumerGroup := "cg:day03:" + uuid.NewString()

	consumer := engine.NewLedgerConsumer(
		streams,
		ledger,
		logger,
		engine.LedgerConsumerConfig{
			AuctionID:   auctionIDText,
			Group:       consumerGroup,
			Consumer:    "day03-consumer-1",
			BatchSize:   100,
			BlockPeriod: 500 * time.Millisecond,
		},
	)

	if err := consumer.ProcessExactly(
		ctx,
		totalBids,
	); err != nil {
		t.Fatalf(
			"process 1,000 bids into PostgreSQL: %v",
			err,
		)
	}

	var (
		rowCount              int64
		distinctSequenceCount int64
		distinctStreamCount   int64
		minimumSequence       int64
		maximumSequence       int64
		sequenceSum           int64
	)

	if err := adminPool.QueryRow(
		ctx,
		`
			SELECT
				COUNT(*),
				COUNT(DISTINCT sequence_no),
				COUNT(DISTINCT stream_entry_id),
				MIN(sequence_no),
				MAX(sequence_no),
				SUM(sequence_no)
			FROM bids
			WHERE auction_id = $1
		`,
		auctionID,
	).Scan(
		&rowCount,
		&distinctSequenceCount,
		&distinctStreamCount,
		&minimumSequence,
		&maximumSequence,
		&sequenceSum,
	); err != nil {
		t.Fatalf(
			"query Day 3 sequence statistics: %v",
			err,
		)
	}

	expectedSum := int64(
		totalBids * (totalBids + 1) / 2,
	)

	if rowCount != totalBids {
		t.Fatalf(
			"expected %d ledger rows, got %d",
			totalBids,
			rowCount,
		)
	}

	if distinctSequenceCount != totalBids {
		t.Fatalf(
			"expected %d distinct sequences, got %d",
			totalBids,
			distinctSequenceCount,
		)
	}

	if distinctStreamCount != totalBids {
		t.Fatalf(
			"expected %d distinct stream IDs, got %d",
			totalBids,
			distinctStreamCount,
		)
	}

	if minimumSequence != 1 {
		t.Fatalf(
			"expected minimum sequence 1, got %d",
			minimumSequence,
		)
	}

	if maximumSequence != totalBids {
		t.Fatalf(
			"expected maximum sequence %d, got %d",
			totalBids,
			maximumSequence,
		)
	}

	if sequenceSum != expectedSum {
		t.Fatalf(
			"expected sequence sum %d, got %d",
			expectedSum,
			sequenceSum,
		)
	}

	// Independently search for missing values in 1..1000.
	var gapCount int64

	if err := adminPool.QueryRow(
		ctx,
		`
			SELECT COUNT(*)
			FROM generate_series(
				1::BIGINT,
				$2::BIGINT
			) AS expected(sequence_no)
			LEFT JOIN bids
			  ON bids.auction_id = $1
			 AND bids.sequence_no = expected.sequence_no
			WHERE bids.sequence_no IS NULL
		`,
		auctionID,
		totalBids,
	).Scan(&gapCount); err != nil {
		t.Fatalf(
			"query ledger sequence gaps: %v",
			err,
		)
	}

	if gapCount != 0 {
		t.Fatalf(
			"ledger contains %d sequence gaps",
			gapCount,
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
			"read final auction counter: %v",
			err,
		)
	}

	if counterValue != totalBids {
		t.Fatalf(
			"expected counter %d, got %d",
			totalBids,
			counterValue,
		)
	}

	t.Logf(
		"Day 3 passed: %d immutable bids, sequences 1..%d, "+
			"zero gaps, zero duplicates, counter=%d",
		rowCount,
		maximumSequence,
		counterValue,
	)
}
