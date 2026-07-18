package redisstore

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

// BidIngress represents the data appended at Redis Stream ingress.
//
// The Redis Stream ID returned by XADD provides the definite stream order.
// Later phases will persist this information into PostgreSQL.
type BidIngress struct {
	AuctionID      string
	BidderID       string
	Amount         int64
	IdempotencyKey string
	CorrelationID  string
}

// BidStreamEntry represents an entry read back from the Redis Stream.
type BidStreamEntry struct {
	ID string
	BidIngress
}

// StreamStore owns all Redis Stream operations.
//
// Redis is only the ordered ingress log. PostgreSQL becomes the source of
// truth starting on Day 2.
type StreamStore struct {
	client *redisclient.Client
}

func NewClient(address, password string, database int) *redisclient.Client {
	return redisclient.NewClient(&redisclient.Options{
		Addr:     address,
		Password: password,
		DB:       database,
	})
}

func NewStreamStore(client *redisclient.Client) *StreamStore {
	return &StreamStore{
		client: client,
	}
}

// StreamKey returns the documented Redis key format:
//
//	auction:{auctionID}:bids
func StreamKey(auctionID string) string {
	return fmt.Sprintf("auction:%s:bids", auctionID)
}

func (s *StreamStore) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

// AppendBid performs the XADD operation.
//
// ID "*" tells Redis to assign the next stream ID. That ID establishes the
// definite order of this bid relative to every other bid in this stream.
func (s *StreamStore) AppendBid(
	ctx context.Context,
	bid BidIngress,
) (string, error) {
	streamID, err := s.client.XAdd(ctx, &redisclient.XAddArgs{
		Stream: StreamKey(bid.AuctionID),
		ID:     "*",
		Values: map[string]any{
			"auction_id":      bid.AuctionID,
			"bidder_id":       bid.BidderID,
			"amount":          bid.Amount,
			"idempotency_key": bid.IdempotencyKey,
			"correlation_id":  bid.CorrelationID,
		},
	}).Result()

	if err != nil {
		return "", fmt.Errorf("append bid to Redis Stream: %w", err)
	}

	return streamID, nil
}

// EnsureConsumerGroup creates the consumer group and stream when absent.
//
// The starting ID "0" allows the group to read every existing entry in the
// stream. In Day 1, the group normally exists before bids are submitted.
func (s *StreamStore) EnsureConsumerGroup(
	ctx context.Context,
	auctionID string,
	group string,
) error {
	err := s.client.XGroupCreateMkStream(
		ctx,
		StreamKey(auctionID),
		group,
		"0",
	).Err()

	if err == nil {
		return nil
	}

	// Redis returns BUSYGROUP when the consumer group already exists.
	if strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}

	return fmt.Errorf("create consumer group: %w", err)
}

// ReadGroup reads new entries using XREADGROUP.
//
// The ">" ID means: give this consumer entries that have never been delivered
// to another consumer in this group.
func (s *StreamStore) ReadGroup(
	ctx context.Context,
	auctionID string,
	group string,
	consumer string,
	count int64,
	block time.Duration,
) ([]BidStreamEntry, error) {
	streams, err := s.client.XReadGroup(ctx, &redisclient.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams: []string{
			StreamKey(auctionID),
			">",
		},
		Count: count,
		Block: block,
	}).Result()

	if err == redisclient.Nil {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("read Redis consumer group: %w", err)
	}

	entries := make([]BidStreamEntry, 0)

	for _, stream := range streams {
		for _, message := range stream.Messages {
			entry, parseErr := parseMessage(message)

			if parseErr != nil {
				return nil, parseErr
			}

			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// Ack acknowledges entries after they have been processed.
//
// On Day 1, "processed" means printed or inspected by the test.
// Starting with PostgreSQL persistence, the mandatory order becomes:
//
//	PostgreSQL COMMIT
//	then
//	XACK
func (s *StreamStore) Ack(
	ctx context.Context,
	auctionID string,
	group string,
	entryIDs ...string,
) error {
	if len(entryIDs) == 0 {
		return nil
	}

	if err := s.client.XAck(
		ctx,
		StreamKey(auctionID),
		group,
		entryIDs...,
	).Err(); err != nil {
		return fmt.Errorf("acknowledge stream entries: %w", err)
	}

	return nil
}

// Range returns the complete persisted Redis Stream in Redis ID order.
//
// XRANGE is useful for proving that every observer sees the same stored order.
func (s *StreamStore) Range(
	ctx context.Context,
	auctionID string,
) ([]BidStreamEntry, error) {
	messages, err := s.client.XRange(
		ctx,
		StreamKey(auctionID),
		"-",
		"+",
	).Result()

	if err != nil {
		return nil, fmt.Errorf("read full Redis Stream: %w", err)
	}

	entries := make([]BidStreamEntry, 0, len(messages))

	for _, message := range messages {
		entry, parseErr := parseMessage(message)

		if parseErr != nil {
			return nil, parseErr
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

func (s *StreamStore) DeleteStream(
	ctx context.Context,
	auctionID string,
) error {
	if err := s.client.Del(ctx, StreamKey(auctionID)).Err(); err != nil {
		return fmt.Errorf("delete Redis Stream: %w", err)
	}

	return nil
}

func parseMessage(message redisclient.XMessage) (BidStreamEntry, error) {
	auctionID, err := fieldString(message.Values, "auction_id")
	if err != nil {
		return BidStreamEntry{}, err
	}

	bidderID, err := fieldString(message.Values, "bidder_id")
	if err != nil {
		return BidStreamEntry{}, err
	}

	amountText, err := fieldString(message.Values, "amount")
	if err != nil {
		return BidStreamEntry{}, err
	}

	amount, err := strconv.ParseInt(amountText, 10, 64)
	if err != nil {
		return BidStreamEntry{}, fmt.Errorf(
			"parse amount %q: %w",
			amountText,
			err,
		)
	}

	idempotencyKey, err := fieldString(
		message.Values,
		"idempotency_key",
	)
	if err != nil {
		return BidStreamEntry{}, err
	}

	correlationID, err := fieldString(
		message.Values,
		"correlation_id",
	)
	if err != nil {
		return BidStreamEntry{}, err
	}

	return BidStreamEntry{
		ID: message.ID,
		BidIngress: BidIngress{
			AuctionID:      auctionID,
			BidderID:       bidderID,
			Amount:         amount,
			IdempotencyKey: idempotencyKey,
			CorrelationID:  correlationID,
		},
	}, nil
}

func fieldString(
	values map[string]any,
	key string,
) (string, error) {
	value, exists := values[key]

	if !exists {
		return "", fmt.Errorf("stream entry is missing field %q", key)
	}

	switch typedValue := value.(type) {
	case string:
		return typedValue, nil
	case []byte:
		return string(typedValue), nil
	default:
		return fmt.Sprint(typedValue), nil
	}
}
