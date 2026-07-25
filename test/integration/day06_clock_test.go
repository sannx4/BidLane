package integration_test

import (
	"context"
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

type day06RejectionSink struct {
	events []engine.BidRejectedEvent
}

func (s *day06RejectionSink) EmitBidRejected(
	_ context.Context,
	event engine.BidRejectedEvent,
) error {
	s.events = append(
		s.events,
		event,
	)

	return nil
}

func TestDay06LaggedConsumerUsesIngressTimestamp(
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
			"reset Day 6 tables: %v\n"+
				"Did you apply migration 000004?",
			err,
		)
	}

	auctionID := uuid.New()
	bidderID := uuid.New()
	idempotencyKey := uuid.New()

	// The bid will be submitted immediately.
	// The auction closes roughly 1.5 seconds later.
	effectiveCloseTime := time.Now().
		UTC().
		Add(1500 * time.Millisecond)

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
			"create Day 6 auction: %v",
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
			"delete old Day 6 stream: %v",
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
			"submit pre-close bid: %v",
			err,
		)
	}

	submittedAt, err :=
		redisstore.StreamEntryTime(
			streamEntryID,
		)
	if err != nil {
		t.Fatalf(
			"derive ingress timestamp: %v",
			err,
		)
	}

	if !submittedAt.Before(
		effectiveCloseTime,
	) {
		t.Fatalf(
			"test setup failed: bid ingress %s was not before close %s",
			submittedAt,
			effectiveCloseTime,
		)
	}

	t.Logf(
		"bid entered Redis before close: submitted_at=%s close_time=%s",
		submittedAt.Format(time.RFC3339Nano),
		effectiveCloseTime.Format(time.RFC3339Nano),
	)

	// Artificial consumer lag.
	//
	// When processing begins, the auction is already closed.
	time.Sleep(2 * time.Second)

	if time.Now().UTC().Before(
		effectiveCloseTime,
	) {
		t.Fatal(
			"test setup failed: consumer was not delayed past close",
		)
	}

	rejectionSink := &day06RejectionSink{}

	validator, err := engine.NewBidValidator(
		rejectionSink,
	)
	if err != nil {
		t.Fatalf(
			"create Day 6 validator: %v",
			err,
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

	var processingTime time.Time

	consumer := engine.NewLedgerConsumer(
		streams,
		ledger,
		logger,
		engine.LedgerConsumerConfig{
			AuctionID: auctionIDText,
			Group: "cg:day06:" +
				uuid.NewString(),
			Consumer:    "day06-lagged-consumer",
			BatchSize:   1,
			BlockPeriod: 500 * time.Millisecond,

			ValidateEntry: func(
				ctx context.Context,
				entry redisstore.BidStreamEntry,
			) (engine.BidValidationResult, error) {
				processingTime = time.Now().UTC()

				ingressTime, err :=
					redisstore.StreamEntryTime(
						entry.ID,
					)
				if err != nil {
					return engine.BidValidationResult{},
						err
				}

				return validator.Validate(
					ctx,
					engine.BidAttempt{
						AuctionID: entry.AuctionID,
						BidderID:  entry.BidderID,
						Amount:    entry.Amount,
						IdempotencyKey: entry.
							IdempotencyKey,
						CorrelationID: entry.
							CorrelationID,
						StreamEntryID: entry.ID,
						SubmittedAt:   ingressTime,
					},
					engine.AuctionValidationSnapshot{
						Exists:             true,
						State:              engine.AuctionStateOpen,
						CurrentPrice:       49_000,
						Increment:          1_000,
						BidderRegistered:   true,
						EffectiveCloseTime: effectiveCloseTime,
					},
				)
			},
		},
	)

	if err := consumer.ProcessExactly(
		ctx,
		1,
	); err != nil {
		t.Fatalf(
			"process lagged bid: %v",
			err,
		)
	}

	if !processingTime.After(
		effectiveCloseTime,
	) {
		t.Fatalf(
			"consumer processed at %s, expected processing after close %s",
			processingTime,
			effectiveCloseTime,
		)
	}

	if len(rejectionSink.events) != 0 {
		t.Fatalf(
			"pre-close bid was rejected: %+v",
			rejectionSink.events,
		)
	}

	var (
		bidCount       int64
		sequenceNumber int64
		storedStreamID string
	)

	if err := adminPool.QueryRow(
		ctx,
		`
			SELECT
				COUNT(*),
				MIN(sequence_no),
				MIN(stream_entry_id)
			FROM bids
			WHERE auction_id = $1
		`,
		auctionID,
	).Scan(
		&bidCount,
		&sequenceNumber,
		&storedStreamID,
	); err != nil {
		t.Fatalf(
			"query Day 6 ledger result: %v",
			err,
		)
	}

	if bidCount != 1 {
		t.Fatalf(
			"expected one accepted ledger bid, got %d",
			bidCount,
		)
	}

	if sequenceNumber != 1 {
		t.Fatalf(
			"expected sequence 1, got %d",
			sequenceNumber,
		)
	}

	if storedStreamID != streamEntryID {
		t.Fatalf(
			"stored stream ID %q, expected %q",
			storedStreamID,
			streamEntryID,
		)
	}

	t.Logf(
		"Day 6 passed: bid submitted at %s, auction closed at %s, consumer processed at %s, bid still accepted as sequence %d",
		submittedAt.Format(time.RFC3339Nano),
		effectiveCloseTime.Format(time.RFC3339Nano),
		processingTime.Format(time.RFC3339Nano),
		sequenceNumber,
	)
}
