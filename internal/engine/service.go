package engine

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"

	redisstore "example.com/bidlane/internal/store/redis"
)

// Service contains the bid-ingress use cases.
type Service struct {
	streams *redisstore.StreamStore
}

func NewService(streams *redisstore.StreamStore) *Service {
	return &Service{
		streams: streams,
	}
}

// ReserveBid appends one bid to the auction's Redis Stream.
//
// Day 1 responsibility:
//   - perform basic shape validation
//   - generate metadata
//   - XADD the bid
//   - return the Redis Stream ID
//
// It does NOT yet:
//   - validate auction state
//   - validate close time
//   - validate deposit entitlement
//   - validate bid increment
//   - write to PostgreSQL
//   - declare the bid accepted
func (s *Service) ReserveBid(
	ctx context.Context,
	auctionID string,
	bidderID string,
	amount int64,
) (string, error) {
	if strings.TrimSpace(auctionID) == "" {
		return "", errors.New("auctionID is required")
	}

	if strings.TrimSpace(bidderID) == "" {
		return "", errors.New("bidderID is required")
	}

	if amount <= 0 {
		return "", errors.New("amount must be greater than zero")
	}

	return s.streams.AppendBid(ctx, redisstore.BidIngress{
		AuctionID:      auctionID,
		BidderID:       bidderID,
		Amount:         amount,
		IdempotencyKey: uuid.NewString(),
		CorrelationID:  uuid.NewString(),
	})
}
