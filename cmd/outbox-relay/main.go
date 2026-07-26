package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example.com/bidlane/internal/relay"
	postgresstore "example.com/bidlane/internal/store/postgres"
	redisstore "example.com/bidlane/internal/store/redis"
)

func main() {
	logger := slog.New(
		slog.NewJSONHandler(
			os.Stdout,
			nil,
		),
	)

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer cancel()

	postgresDSN := requiredEnvironment(
		"POSTGRES_DSN",
	)

	redisAddress := requiredEnvironment(
		"REDIS_ADDR",
	)

	relayPool, err := postgresstore.ConnectPool(
		ctx,
		postgresDSN,
		"bidlane_outbox_relay",
	)
	if err != nil {
		logger.Error(
			"connect outbox relay PostgreSQL pool",
			"error",
			err,
		)
		os.Exit(1)
	}
	defer relayPool.Close()

	redisClient := redisstore.NewClient(
		redisAddress,
		"",
		0,
	)
	defer redisClient.Close()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.Error(
			"connect outbox relay to Redis",
			"error",
			err,
		)
		os.Exit(1)
	}

	outboxStore := postgresstore.NewOutboxStore(
		relayPool,
	)

	publisher := redisstore.NewPubSubPublisher(
		redisClient,
	)

	outboxRelay, err := relay.NewOutboxRelay(
		outboxStore,
		publisher,
		relay.Config{
			Channel:      relay.DefaultBidEventsChannel,
			BatchSize:    100,
			PollInterval: 200 * time.Millisecond,
		},
	)
	if err != nil {
		logger.Error(
			"create outbox relay",
			"error",
			err,
		)
		os.Exit(1)
	}

	logger.Info(
		"outbox relay started",
		"channel",
		relay.DefaultBidEventsChannel,
	)

	if err := outboxRelay.Run(ctx); err != nil {
		logger.Error(
			"outbox relay stopped with error",
			"error",
			err,
		)
		os.Exit(1)
	}

	logger.Info(
		"outbox relay stopped",
	)
}

func requiredEnvironment(
	name string,
) string {
	value := os.Getenv(name)

	if value == "" {
		panic(
			fmt.Sprintf(
				"required environment variable %s is missing",
				name,
			),
		)
	}

	return value
}
