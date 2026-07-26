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
// Later phases persist this information into PostgreSQL.
type BidIngress struct {
	AuctionID      string
	BidderID       string
	Amount         int64
	IdempotencyKey string
	CorrelationID  string
}

// BidStreamEntry represents an entry read from the Redis Stream.
type BidStreamEntry struct {
	ID string
	BidIngress
}

// StreamStore owns Redis Stream operations.
//
// Redis is the ordered ingress log. PostgreSQL is the permanent
// source of truth.
type StreamStore struct {
	client *redisclient.Client
}

// NewClient creates a Redis client.
func NewClient(
	address string,
	password string,
	database int,
) *redisclient.Client {
	return redisclient.NewClient(
		&redisclient.Options{
			Addr:     address,
			Password: password,
			DB:       database,
		},
	)
}

// NewStreamStore creates the Redis Stream storage adapter.
func NewStreamStore(
	client *redisclient.Client,
) *StreamStore {
	return &StreamStore{
		client: client,
	}
}

// StreamKey returns the Redis key for one auction's bid stream.
//
// Example:
//
//	auction:550e8400-e29b-41d4-a716-446655440000:bids
func StreamKey(
	auctionID string,
) string {
	return fmt.Sprintf(
		"auction:%s:bids",
		auctionID,
	)
}

// Ping verifies that Redis is reachable.
func (s *StreamStore) Ping(
	ctx context.Context,
) error {
	if s == nil || s.client == nil {
		return fmt.Errorf(
			"Redis Stream store is not configured",
		)
	}

	if err := s.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf(
			"ping Redis: %w",
			err,
		)
	}

	return nil
}

// AppendBid appends one bid using Redis XADD.
//
// ID "*" tells Redis to assign the next Stream ID. That ID establishes
// the definite ingress order of this bid relative to every other bid
// in the same auction stream.
func (s *StreamStore) AppendBid(
	ctx context.Context,
	bid BidIngress,
) (string, error) {
	if s == nil || s.client == nil {
		return "", fmt.Errorf(
			"Redis Stream store is not configured",
		)
	}

	if bid.AuctionID == "" {
		return "", fmt.Errorf(
			"auction ID is required",
		)
	}

	if bid.BidderID == "" {
		return "", fmt.Errorf(
			"bidder ID is required",
		)
	}

	if bid.Amount <= 0 {
		return "", fmt.Errorf(
			"bid amount must be positive",
		)
	}

	if bid.IdempotencyKey == "" {
		return "", fmt.Errorf(
			"idempotency key is required",
		)
	}

	if bid.CorrelationID == "" {
		return "", fmt.Errorf(
			"correlation ID is required",
		)
	}

	streamID, err := s.client.XAdd(
		ctx,
		&redisclient.XAddArgs{
			Stream: StreamKey(bid.AuctionID),
			ID:     "*",
			Values: map[string]any{
				"auction_id":      bid.AuctionID,
				"bidder_id":       bid.BidderID,
				"amount":          bid.Amount,
				"idempotency_key": bid.IdempotencyKey,
				"correlation_id":  bid.CorrelationID,
			},
		},
	).Result()
	if err != nil {
		return "", fmt.Errorf(
			"append bid to Redis Stream: %w",
			err,
		)
	}

	return streamID, nil
}

// EnsureConsumerGroup creates the consumer group and stream when absent.
//
// Starting at ID "0" allows the group to receive all existing stream
// entries. MKSTREAM creates the stream when it does not yet exist.
func (s *StreamStore) EnsureConsumerGroup(
	ctx context.Context,
	auctionID string,
	group string,
) error {
	if s == nil || s.client == nil {
		return fmt.Errorf(
			"Redis Stream store is not configured",
		)
	}

	if auctionID == "" {
		return fmt.Errorf(
			"auction ID is required",
		)
	}

	if group == "" {
		return fmt.Errorf(
			"consumer group is required",
		)
	}

	err := s.client.XGroupCreateMkStream(
		ctx,
		StreamKey(auctionID),
		group,
		"0",
	).Err()

	if err == nil {
		return nil
	}

	// Redis returns BUSYGROUP when the group already exists.
	// Existing groups are acceptable.
	if strings.Contains(
		err.Error(),
		"BUSYGROUP",
	) {
		return nil
	}

	return fmt.Errorf(
		"create Redis consumer group %q: %w",
		group,
		err,
	)
}

// ReadGroup reads previously undelivered entries using XREADGROUP.
//
// The ">" ID means:
//
//	Give this consumer entries that have not previously been delivered
//	to another consumer in this consumer group.
func (s *StreamStore) ReadGroup(
	ctx context.Context,
	auctionID string,
	group string,
	consumer string,
	count int64,
	block time.Duration,
) ([]BidStreamEntry, error) {
	if s == nil || s.client == nil {
		return nil, fmt.Errorf(
			"Redis Stream store is not configured",
		)
	}

	if auctionID == "" {
		return nil, fmt.Errorf(
			"auction ID is required",
		)
	}

	if group == "" {
		return nil, fmt.Errorf(
			"consumer group is required",
		)
	}

	if consumer == "" {
		return nil, fmt.Errorf(
			"consumer name is required",
		)
	}

	if count <= 0 {
		return nil, fmt.Errorf(
			"XREADGROUP count must be positive",
		)
	}

	if block < 0 {
		return nil, fmt.Errorf(
			"XREADGROUP block duration cannot be negative",
		)
	}

	streams, err := s.client.XReadGroup(
		ctx,
		&redisclient.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams: []string{
				StreamKey(auctionID),
				">",
			},
			Count: count,
			Block: block,
		},
	).Result()

	if err == redisclient.Nil {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf(
			"read Redis consumer group: %w",
			err,
		)
	}

	entries := make(
		[]BidStreamEntry,
		0,
	)

	for _, stream := range streams {
		for _, message := range stream.Messages {
			entry, parseErr := parseMessage(
				message,
			)
			if parseErr != nil {
				return nil, fmt.Errorf(
					"parse Redis Stream entry %s: %w",
					message.ID,
					parseErr,
				)
			}

			entries = append(
				entries,
				entry,
			)
		}
	}

	return entries, nil
}

// Ack acknowledges entries after successful processing.
//
// The mandatory correctness order is:
//
//	PostgreSQL COMMIT
//	then
//	Redis XACK
//
// XACK must never occur before PostgreSQL commits.
func (s *StreamStore) Ack(
	ctx context.Context,
	auctionID string,
	group string,
	entryIDs ...string,
) error {
	if s == nil || s.client == nil {
		return fmt.Errorf(
			"Redis Stream store is not configured",
		)
	}

	if auctionID == "" {
		return fmt.Errorf(
			"auction ID is required",
		)
	}

	if group == "" {
		return fmt.Errorf(
			"consumer group is required",
		)
	}

	if len(entryIDs) == 0 {
		return nil
	}

	if err := s.client.XAck(
		ctx,
		StreamKey(auctionID),
		group,
		entryIDs...,
	).Err(); err != nil {
		return fmt.Errorf(
			"acknowledge Redis Stream entries: %w",
			err,
		)
	}

	return nil
}

// AutoClaimPending reclaims abandoned pending entries using XAUTOCLAIM.
//
// A consumer may crash after PostgreSQL commits but before Redis XACK.
// In that case, the entry remains in the Pending Entries List. A
// restarted consumer uses XAUTOCLAIM to take ownership and reprocess it.
//
// PostgreSQL idempotency makes that replay safe.

// Range returns the complete Redis Stream in Redis ID order.
//
// XRANGE is useful for proving that every observer sees the same
// persisted ingress order.
func (s *StreamStore) Range(
	ctx context.Context,
	auctionID string,
) ([]BidStreamEntry, error) {
	if s == nil || s.client == nil {
		return nil, fmt.Errorf(
			"Redis Stream store is not configured",
		)
	}

	if auctionID == "" {
		return nil, fmt.Errorf(
			"auction ID is required",
		)
	}

	messages, err := s.client.XRange(
		ctx,
		StreamKey(auctionID),
		"-",
		"+",
	).Result()
	if err != nil {
		return nil, fmt.Errorf(
			"read full Redis Stream: %w",
			err,
		)
	}

	entries := make(
		[]BidStreamEntry,
		0,
		len(messages),
	)

	for _, message := range messages {
		entry, parseErr := parseMessage(
			message,
		)
		if parseErr != nil {
			return nil, fmt.Errorf(
				"parse Redis Stream entry %s: %w",
				message.ID,
				parseErr,
			)
		}

		entries = append(
			entries,
			entry,
		)
	}

	return entries, nil
}

// DeleteStream removes one auction's Redis Stream.
//
// This is mainly used by integration-test cleanup.
func (s *StreamStore) DeleteStream(
	ctx context.Context,
	auctionID string,
) error {
	if s == nil || s.client == nil {
		return fmt.Errorf(
			"Redis Stream store is not configured",
		)
	}

	if auctionID == "" {
		return fmt.Errorf(
			"auction ID is required",
		)
	}

	if err := s.client.Del(
		ctx,
		StreamKey(auctionID),
	).Err(); err != nil {
		return fmt.Errorf(
			"delete Redis Stream: %w",
			err,
		)
	}

	return nil
}

// parseMessage converts a generic Redis XMessage into a strongly
// typed BidStreamEntry.
func parseMessage(
	message redisclient.XMessage,
) (BidStreamEntry, error) {
	if message.ID == "" {
		return BidStreamEntry{}, fmt.Errorf(
			"Redis Stream entry ID is missing",
		)
	}

	auctionID, err := fieldString(
		message.Values,
		"auction_id",
	)
	if err != nil {
		return BidStreamEntry{}, err
	}

	bidderID, err := fieldString(
		message.Values,
		"bidder_id",
	)
	if err != nil {
		return BidStreamEntry{}, err
	}

	amountText, err := fieldString(
		message.Values,
		"amount",
	)
	if err != nil {
		return BidStreamEntry{}, err
	}

	amount, err := strconv.ParseInt(
		amountText,
		10,
		64,
	)
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

// fieldString extracts one Redis field as text.
func fieldString(
	values map[string]any,
	key string,
) (string, error) {
	value, exists := values[key]
	if !exists {
		return "", fmt.Errorf(
			"stream entry is missing field %q",
			key,
		)
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
