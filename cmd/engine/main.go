package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"example.com/bidlane/internal/engine"
	redisstore "example.com/bidlane/internal/store/redis"
)

func main() {
	logger := slog.New(
		slog.NewJSONHandler(os.Stdout, nil),
	)
	slog.SetDefault(logger)

	redisAddress := requiredEnvironment("REDIS_ADDR")
	redisPassword := os.Getenv("REDIS_PASSWORD")
	redisDatabase := requiredIntegerEnvironment("REDIS_DB")

	auctionID := requiredEnvironment("AUCTION_ID")
	consumerGroup := requiredEnvironment("CONSUMER_GROUP")
	consumerName := requiredEnvironment("CONSUMER_NAME")

	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer cancel()

	client := redisstore.NewClient(
		redisAddress,
		redisPassword,
		redisDatabase,
	)
	defer client.Close()

	streams := redisstore.NewStreamStore(client)

	if err := streams.Ping(ctx); err != nil {
		slog.Error(
			"Redis connection failed",
			"error", err,
		)
		os.Exit(1)
	}

	consumer := engine.NewConsumer(
		streams,
		engine.ConsumerConfig{
			AuctionID:   auctionID,
			Group:       consumerGroup,
			Consumer:    consumerName,
			BatchSize:   20,
			BlockPeriod: 2 * time.Second,
		},
	)

	if err := consumer.Run(ctx); err != nil {
		slog.Error(
			"consumer stopped unexpectedly",
			"error", err,
		)
		os.Exit(1)
	}
}

func requiredEnvironment(name string) string {
	value := os.Getenv(name)

	if value == "" {
		slog.Error(
			"required environment variable is missing",
			"name", name,
		)
		os.Exit(1)
	}

	return value
}

func requiredIntegerEnvironment(name string) int {
	rawValue := requiredEnvironment(name)

	value, err := strconv.Atoi(rawValue)
	if err != nil {
		slog.Error(
			"environment variable must be an integer",
			"name", name,
			"value", rawValue,
			"error", err,
		)
		os.Exit(1)
	}

	return value
}
