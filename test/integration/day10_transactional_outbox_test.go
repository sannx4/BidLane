package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"example.com/bidlane/internal/engine"
	outboxrelay "example.com/bidlane/internal/relay"
	postgresstore "example.com/bidlane/internal/store/postgres"
	redisstore "example.com/bidlane/internal/store/redis"
)

func TestDay10TransactionalOutboxPublishesAtLeastOnce(
	t *testing.T,
) {
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

	relayPool, err := postgresstore.ConnectPool(
		ctx,
		postgresDSN,
		"bidlane_outbox_relay",
	)
	if err != nil {
		t.Fatalf(
			"connect outbox relay pool: %v\n"+
				"Did you apply migration 000006?",
			err,
		)
	}
	defer relayPool.Close()

	if _, err := adminPool.Exec(
		ctx,
		`
		TRUNCATE TABLE
			outbox,
			bids,
			auction_sequences,
			auctions
		CASCADE
		`,
	); err != nil {
		t.Fatalf(
			"reset Day 10 tables: %v\n"+
				"Did you apply migration 000006?",
			err,
		)
	}

	redisClient := redisstore.NewClient(
		redisAddress,
		"",
		0,
	)
	defer redisClient.Close()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatalf(
			"Redis unavailable at %s: %v",
			redisAddress,
			err,
		)
	}

	auctionID := uuid.New()
	bidderID := uuid.New()
	idempotencyKey := uuid.New()
	auctionIDText := auctionID.String()

	effectiveCloseTime := time.Now().
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
			"create Day 10 auction: %v",
			err,
		)
	}

	if err := redisClient.Del(
		ctx,
		redisstore.StreamKey(auctionIDText),
	).Err(); err != nil {
		t.Fatalf(
			"delete old Day 10 stream: %v",
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

		_ = redisClient.Del(
			cleanupCtx,
			redisstore.StreamKey(auctionIDText),
		).Err()

		_, _ = adminPool.Exec(
			cleanupCtx,
			`
			TRUNCATE TABLE
				outbox,
				bids,
				auction_sequences,
				auctions
			CASCADE
			`,
		)
	})

	// Submit one physical Redis Stream bid.
	service := engine.NewService(
		redisstore.NewStreamStore(redisClient),
	)

	_, err = service.ReserveBidWithIdempotencyKey(
		ctx,
		auctionIDText,
		bidderID.String(),
		50_000,
		idempotencyKey.String(),
	)
	if err != nil {
		t.Fatalf(
			"reserve Day 10 bid: %v",
			err,
		)
	}

	// Process through the real LedgerConsumer.
	streams := redisstore.NewStreamStore(
		redisClient,
	)

	ledger := postgresstore.NewLedgerStore(
		enginePool,
	)

	logger := slog.New(
		slog.NewTextHandler(
			io.Discard,
			nil,
		),
	)

	consumer := engine.NewLedgerConsumer(
		streams,
		ledger,
		logger,
		engine.LedgerConsumerConfig{
			AuctionID:   auctionIDText,
			Group:       "cg:day10:" + uuid.NewString(),
			Consumer:    "day10-engine-1",
			BatchSize:   10,
			BlockPeriod: 500 * time.Millisecond,
		},
	)

	if err := consumer.ProcessExactly(
		ctx,
		1,
	); err != nil {
		t.Fatalf(
			"process Day 10 bid: %v",
			err,
		)
	}

	// Bid and outbox event must both exist after one transaction.
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
			"count Day 10 bid rows: %v",
			err,
		)
	}

	if bidCount != 1 {
		t.Fatalf(
			"expected one immutable bid, got %d",
			bidCount,
		)
	}

	var outboxCount int64
	if err := adminPool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM outbox
		WHERE aggregate_id = $1
		  AND event_type = 'BidAccepted'
		`,
		auctionID,
	).Scan(&outboxCount); err != nil {
		t.Fatalf(
			"count Day 10 outbox rows: %v",
			err,
		)
	}

	if outboxCount != 1 {
		t.Fatalf(
			"expected one BidAccepted outbox event, got %d",
			outboxCount,
		)
	}

	channel := "bidlane:test:day10:" +
		uuid.NewString()

	// Pub/Sub subscribers receive messages only after subscription.
	subscription := redisClient.Subscribe(
		ctx,
		channel,
	)
	defer subscription.Close()

	if _, err := subscription.Receive(ctx); err != nil {
		t.Fatalf(
			"confirm Day 10 Redis subscription: %v",
			err,
		)
	}

	messages := subscription.Channel()

	outboxStore := postgresstore.NewOutboxStore(
		relayPool,
	)

	publisher := redisstore.NewPubSubPublisher(
		redisClient,
	)

	simulatedRelayCrash := errors.New(
		"simulated relay crash after Redis publish before PostgreSQL mark",
	)

	crashingRelay, err := outboxrelay.NewOutboxRelay(
		outboxStore,
		publisher,
		outboxrelay.Config{
			Channel:      channel,
			BatchSize:    1,
			PollInterval: 10 * time.Millisecond,

			AfterPublishBeforeMark: func(
				_ postgresstore.OutboxEvent,
			) error {
				return simulatedRelayCrash
			},
		},
	)
	if err != nil {
		t.Fatalf(
			"create crashing Day 10 relay: %v",
			err,
		)
	}

	published, err := crashingRelay.ProcessOne(ctx)
	if err == nil {
		t.Fatal(
			"expected simulated relay crash",
		)
	}

	if !errors.Is(err, simulatedRelayCrash) {
		t.Fatalf(
			"expected simulated relay crash %v, got %v",
			simulatedRelayCrash,
			err,
		)
	}

	if published {
		t.Fatal(
			"failed relay transaction was unexpectedly reported committed",
		)
	}

	// Redis received the first publication.
	firstEvent := receiveDay10PublishedEvent(
		t,
		ctx,
		messages,
	)

	// The database transaction rolled back, so the event remains pending.
	assertDay10OutboxPublishedState(
		t,
		ctx,
		adminPool,
		firstEvent.EventID,
		false,
		0,
	)

	// Restart the relay without the failure hook.
	restartedRelay, err := outboxrelay.NewOutboxRelay(
		outboxStore,
		publisher,
		outboxrelay.Config{
			Channel:      channel,
			BatchSize:    1,
			PollInterval: 10 * time.Millisecond,
		},
	)
	if err != nil {
		t.Fatalf(
			"create restarted Day 10 relay: %v",
			err,
		)
	}

	published, err = restartedRelay.ProcessOne(ctx)
	if err != nil {
		t.Fatalf(
			"restart Day 10 relay: %v",
			err,
		)
	}

	if !published {
		t.Fatal(
			"restarted relay did not republish pending event",
		)
	}

	secondEvent := receiveDay10PublishedEvent(
		t,
		ctx,
		messages,
	)

	// Both publications represent the same logical outbox event.
	if firstEvent.EventID != secondEvent.EventID {
		t.Fatalf(
			"expected duplicate publication of event %s, got %s",
			firstEvent.EventID,
			secondEvent.EventID,
		)
	}

	if firstEvent.EventType !=
		postgresstore.BidAcceptedEventType {
		t.Fatalf(
			"expected event type %q, got %q",
			postgresstore.BidAcceptedEventType,
			firstEvent.EventType,
		)
	}

	if firstEvent.AggregateID != auctionID {
		t.Fatalf(
			"published auction %s, expected %s",
			firstEvent.AggregateID,
			auctionID,
		)
	}

	if firstEvent.AggregateSequence != 1 {
		t.Fatalf(
			"expected sequence 1, got %d",
			firstEvent.AggregateSequence,
		)
	}

	assertDay10OutboxPublishedState(
		t,
		ctx,
		adminPool,
		firstEvent.EventID,
		true,
		1,
	)

	// Demonstrate the downstream idempotency requirement.
	//
	// Redis delivered two physical messages, but an idempotent
	// downstream consumer performs one logical side effect.
	seenEventIDs := make(
		map[uuid.UUID]struct{},
	)

	logicalSideEffects := 0

	for _, event := range []outboxrelay.PublishedEvent{
		firstEvent,
		secondEvent,
	} {
		if _, alreadyProcessed :=
			seenEventIDs[event.EventID]; alreadyProcessed {
			continue
		}

		seenEventIDs[event.EventID] = struct{}{}
		logicalSideEffects++
	}

	if logicalSideEffects != 1 {
		t.Fatalf(
			"expected one idempotent downstream side effect, got %d",
			logicalSideEffects,
		)
	}

	t.Log(
		"Day 10 passed: bid and outbox committed atomically; " +
			"relay crash caused duplicate publication, no event loss; " +
			"downstream event-ID deduplication produced one side effect",
	)
}

func receiveDay10PublishedEvent(
	t *testing.T,
	ctx context.Context,
	messages <-chan *redis.Message,
) outboxrelay.PublishedEvent {
	t.Helper()

	select {
	case <-ctx.Done():
		t.Fatalf(
			"waiting for Day 10 Redis publication: %v",
			ctx.Err(),
		)

	case message := <-messages:
		if message == nil {
			t.Fatal(
				"received nil Day 10 Redis message",
			)
		}

		var event outboxrelay.PublishedEvent

		if err := json.Unmarshal(
			[]byte(message.Payload),
			&event,
		); err != nil {
			t.Fatalf(
				"decode Day 10 published event: %v",
				err,
			)
		}

		return event
	}

	return outboxrelay.PublishedEvent{}
}

func assertDay10OutboxPublishedState(
	t *testing.T,
	ctx context.Context,
	adminPool *pgxpool.Pool,
	eventID uuid.UUID,
	expectedPublished bool,
	expectedAttempts int32,
) {
	t.Helper()

	var (
		published       bool
		publishAttempts int32
	)

	if err := adminPool.QueryRow(
		ctx,
		`
		SELECT
			published_at IS NOT NULL,
			publish_attempts
		FROM outbox
		WHERE id = $1
		`,
		eventID,
	).Scan(
		&published,
		&publishAttempts,
	); err != nil {
		t.Fatalf(
			"read Day 10 outbox state: %v",
			err,
		)
	}

	if published != expectedPublished {
		t.Fatalf(
			"event published state %v, expected %v",
			published,
			expectedPublished,
		)
	}

	if publishAttempts != expectedAttempts {
		t.Fatalf(
			"event publish attempts %d, expected %d",
			publishAttempts,
			expectedAttempts,
		)
	}
}
