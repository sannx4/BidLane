package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	postgresstore "example.com/bidlane/internal/store/postgres"
	redisstore "example.com/bidlane/internal/store/redis"
)

type LedgerConsumerConfig struct {
	AuctionID   string
	Group       string
	Consumer    string
	BatchSize   int64
	BlockPeriod time.Duration

	ValidateEntry func(
		context.Context,
		redisstore.BidStreamEntry,
	) (BidValidationResult, error)

	// Entries that have remained pending for at least this long
	// may be taken from a failed consumer.
	//
	// A zero value uses a production-oriented default of 30 seconds.
	RecoveryMinIdle time.Duration

	// Number of pending entries requested per XAUTOCLAIM call.
	//
	// A non-positive value uses BatchSize.
	RecoveryBatchSize int64

	// Failure-injection hook used by Day 8.
	//
	// It runs only after PostgreSQL has committed and immediately
	// before Redis XACK.
	//
	// Production configuration leaves this nil.
	AfterPersistBeforeAck func(
		redisstore.BidStreamEntry,
		postgresstore.AppendBidResult,
	)

	BeforeAck func(
		context.Context,
		redisstore.BidStreamEntry,
	) error
}

type LedgerConsumer struct {
	streams *redisstore.StreamStore
	ledger  *postgresstore.LedgerStore
	logger  *slog.Logger
	config  LedgerConsumerConfig
}

func NewLedgerConsumer(
	streams *redisstore.StreamStore,
	ledger *postgresstore.LedgerStore,
	logger *slog.Logger,
	config LedgerConsumerConfig,
) *LedgerConsumer {
	if logger == nil {
		logger = slog.Default()
	}

	return &LedgerConsumer{
		streams: streams,
		ledger:  ledger,
		logger:  logger,
		config:  config,
	}
}

// Run continuously consumes bids until the context is cancelled.
func (c *LedgerConsumer) Run(
	ctx context.Context,
) error {
	if err := c.ensureGroup(ctx); err != nil {
		return err
	}

	for {
		// Recover abandoned work before requesting fresh work.
		if _, err := c.recoverPending(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return fmt.Errorf(
				"recover pending entries: %w",
				err,
			)
		}

		_, err := c.processBatch(
			ctx,
			c.config.BatchSize,
		)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return err
		}
	}
}

// ProcessExactly is used by the Day 3 integration test.
//
// It returns only after exactly expectedCount entries have been
// committed to PostgreSQL and acknowledged in Redis.
func (c *LedgerConsumer) ProcessExactly(
	ctx context.Context,
	expectedCount int,
) error {
	if expectedCount <= 0 {
		return fmt.Errorf(
			"expected count must be positive",
		)
	}

	if err := c.ensureGroup(ctx); err != nil {
		return err
	}

	processed := 0

	for processed < expectedCount {
		remaining := expectedCount - processed
		readCount := c.config.BatchSize

		if int64(remaining) < readCount {
			readCount = int64(remaining)
		}

		batchProcessed, err := c.processBatch(
			ctx,
			readCount,
		)
		if err != nil {
			return err
		}

		processed += batchProcessed
	}

	return nil
}

func (c *LedgerConsumer) ensureGroup(
	ctx context.Context,
) error {
	if c.config.AuctionID == "" {
		return fmt.Errorf(
			"ledger consumer auction ID is required",
		)
	}

	if c.config.Group == "" {
		return fmt.Errorf(
			"ledger consumer group is required",
		)
	}

	if c.config.Consumer == "" {
		return fmt.Errorf(
			"ledger consumer name is required",
		)
	}

	if c.config.BatchSize <= 0 {
		return fmt.Errorf(
			"ledger consumer batch size must be positive",
		)
	}

	if err := c.streams.EnsureConsumerGroup(
		ctx,
		c.config.AuctionID,
		c.config.Group,
	); err != nil {
		return fmt.Errorf(
			"ensure ledger consumer group: %w",
			err,
		)
	}

	return nil
}

func (c *LedgerConsumer) processBatch(
	ctx context.Context,
	count int64,
) (int, error) {
	entries, err := c.streams.ReadGroup(
		ctx,
		c.config.AuctionID,
		c.config.Group,
		c.config.Consumer,
		count,
		c.config.BlockPeriod,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"read bid stream group: %w",
			err,
		)
	}

	return c.processEntries(
		ctx,
		entries,
	)
}

func (c *LedgerConsumer) processEntries(
	ctx context.Context,
	entries []redisstore.BidStreamEntry,
) (int, error) {
	processed := 0

	for _, entry := range entries {
		if c.config.ValidateEntry != nil {
			validation, err :=
				c.config.ValidateEntry(
					ctx,
					entry,
				)
			if err != nil {
				// Rejection evidence may not have been recorded.
				// Leave the entry pending for retry.
				return processed, fmt.Errorf(
					"validate stream entry %s: %w",
					entry.ID,
					err,
				)
			}

			if !validation.Accepted {
				if err := c.streams.Ack(
					ctx,
					c.config.AuctionID,
					c.config.Group,
					entry.ID,
				); err != nil {
					return processed, fmt.Errorf(
						"ack rejected Redis entry %s: %w",
						entry.ID,
						err,
					)
				}

				processed++

				reason := BidRejectionReason(
					"unknown",
				)

				if validation.Rejection != nil {
					reason =
						validation.
							Rejection.
							ReasonCode
				}

				c.logger.Info(
					"bid rejected and acknowledged",
					"auction_id",
					entry.AuctionID,
					"stream_entry_id",
					entry.ID,
					"reason",
					reason,
				)

				continue
			}
		}

		result, err := c.persistEntry(
			ctx,
			entry,
		)
		if err != nil {
			// PostgreSQL did not commit.
			// Leave the Redis entry pending.
			return processed, err
		}

		// Day 8 crash point:
		//
		// PostgreSQL has committed successfully, but Redis has not
		// yet received XACK.
		if c.config.AfterPersistBeforeAck != nil {
			c.config.AfterPersistBeforeAck(
				entry,
				result,
			)
		}

		if c.config.BeforeAck != nil {
			if err := c.config.BeforeAck(
				ctx,
				entry,
			); err != nil {
				return processed, fmt.Errorf(
					"before Redis XACK for entry %s: %w",
					entry.ID,
					err,
				)
			}
		}

		if err := c.streams.Ack(
			ctx,
			c.config.AuctionID,
			c.config.Group,
			entry.ID,
		); err != nil {
			return processed, fmt.Errorf(
				"ack committed Redis entry %s: %w",
				entry.ID,
				err,
			)
		}

		processed++

		if result.Inserted {
			c.logger.Info(
				"bid committed to immutable ledger",
				"auction_id",
				entry.AuctionID,
				"stream_entry_id",
				entry.ID,
				"sequence_no",
				result.SequenceNumber,
				"bidder_id",
				entry.BidderID,
				"amount",
				entry.Amount,
				"idempotency_key",
				entry.IdempotencyKey,
			)
		} else {
			c.logger.Info(
				"duplicate crash-recovery delivery acknowledged",
				"auction_id",
				entry.AuctionID,
				"stream_entry_id",
				entry.ID,
				"existing_sequence_no",
				result.SequenceNumber,
				"idempotency_key",
				entry.IdempotencyKey,
			)
		}
	}

	return processed, nil
}

func (c *LedgerConsumer) RecoverPending(
	ctx context.Context,
) (int, error) {
	if err := c.ensureGroup(ctx); err != nil {
		return 0, err
	}

	return c.recoverPending(ctx)
}

func (c *LedgerConsumer) recoverPending(
	ctx context.Context,
) (int, error) {
	batchSize := c.config.RecoveryBatchSize

	if batchSize <= 0 {
		batchSize = c.config.BatchSize
	}

	if batchSize <= 0 {
		return 0, fmt.Errorf(
			"recovery batch size must be positive",
		)
	}

	minIdle := c.config.RecoveryMinIdle

	if minIdle <= 0 {
		minIdle = 30 * time.Second
	}

	start := "0-0"
	totalProcessed := 0

	for {
		entries, nextStart, err :=
			c.streams.AutoClaimPending(
				ctx,
				c.config.AuctionID,
				c.config.Group,
				c.config.Consumer,
				minIdle,
				start,
				batchSize,
			)
		if err != nil {
			return totalProcessed,
				fmt.Errorf(
					"recover pending Redis entries from cursor %s: %w",
					start,
					err,
				)
		}

		processed, err := c.processEntries(
			ctx,
			entries,
		)
		if err != nil {
			return totalProcessed, err
		}

		totalProcessed += processed

		// Redis returns 0-0 when the PEL scan is complete.
		if nextStart == "0-0" {
			return totalProcessed, nil
		}

		if nextStart == start &&
			len(entries) == 0 {
			return totalProcessed,
				fmt.Errorf(
					"XAUTOCLAIM cursor did not advance from %s",
					start,
				)
		}

		start = nextStart
	}
}

func (c *LedgerConsumer) persistEntry(
	ctx context.Context,
	entry redisstore.BidStreamEntry,
) (postgresstore.AppendBidResult, error) {
	if entry.AuctionID != c.config.AuctionID {
		return postgresstore.AppendBidResult{},
			fmt.Errorf(
				"stream entry auction %q does not match consumer auction %q",
				entry.AuctionID,
				c.config.AuctionID,
			)
	}

	auctionID, err := uuid.Parse(
		entry.AuctionID,
	)
	if err != nil {
		return postgresstore.AppendBidResult{},
			fmt.Errorf(
				"parse auction ID %q: %w",
				entry.AuctionID,
				err,
			)
	}

	bidderID, err := uuid.Parse(
		entry.BidderID,
	)
	if err != nil {
		return postgresstore.AppendBidResult{},
			fmt.Errorf(
				"parse bidder ID %q: %w",
				entry.BidderID,
				err,
			)
	}

	idempotencyKey, err := uuid.Parse(
		entry.IdempotencyKey,
	)
	if err != nil {
		return postgresstore.AppendBidResult{},
			fmt.Errorf(
				"parse idempotency key %q: %w",
				entry.IdempotencyKey,
				err,
			)
	}

	submittedAt, err :=
		redisstore.StreamEntryTime(entry.ID)
	if err != nil {
		return postgresstore.AppendBidResult{},
			fmt.Errorf(
				"derive authoritative ingress timestamp from stream ID %q: %w",
				entry.ID,
				err,
			)
	}

	antiSnipeResult, err :=
		c.ledger.AppendBidIdempotentWithAntiSnipe(
			ctx,
			postgresstore.BidToPersist{
				AuctionID:      auctionID,
				BidderID:       bidderID,
				Amount:         entry.Amount,
				IdempotencyKey: idempotencyKey,
				StreamEntryID:  entry.ID,
			},
			submittedAt,
		)
	if err != nil {
		return postgresstore.AppendBidResult{},
			err
	}

	return antiSnipeResult.AppendBidResult, nil
}
