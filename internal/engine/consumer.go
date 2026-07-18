package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	redisstore "example.com/bidlane/internal/store/redis"
)

type ConsumerConfig struct {
	AuctionID   string
	Group       string
	Consumer    string
	BatchSize   int64
	BlockPeriod time.Duration
}

type Consumer struct {
	streams *redisstore.StreamStore
	config  ConsumerConfig
}

func NewConsumer(
	streams *redisstore.StreamStore,
	config ConsumerConfig,
) *Consumer {
	return &Consumer{
		streams: streams,
		config:  config,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	if err := c.streams.EnsureConsumerGroup(
		ctx,
		c.config.AuctionID,
		c.config.Group,
	); err != nil {
		return err
	}

	slog.Info(
		"consumer started",
		"auction_id", c.config.AuctionID,
		"group", c.config.Group,
		"consumer", c.config.Consumer,
	)

	for {
		if ctx.Err() != nil {
			return nil
		}

		entries, err := c.streams.ReadGroup(
			ctx,
			c.config.AuctionID,
			c.config.Group,
			c.config.Consumer,
			c.config.BatchSize,
			c.config.BlockPeriod,
		)

		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return fmt.Errorf("consume bid entries: %w", err)
		}

		for _, entry := range entries {
			slog.Info(
				"bid stream entry",
				"stream_id", entry.ID,
				"auction_id", entry.AuctionID,
				"bidder_id", entry.BidderID,
				"amount", entry.Amount,
				"idempotency_key", entry.IdempotencyKey,
				"correlation_id", entry.CorrelationID,
			)

			// Day 1 has no PostgreSQL transaction.
			// From Day 2 onward, PostgreSQL COMMIT must happen before this XACK.
			if err := c.streams.Ack(
				ctx,
				entry.AuctionID,
				c.config.Group,
				entry.ID,
			); err != nil {
				return err
			}
		}
	}
}
