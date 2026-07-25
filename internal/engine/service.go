package engine

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	redisstore "example.com/bidlane/internal/store/redis"
)

type Service struct {
	streams *redisstore.StreamStore
}

func NewService(
	streams *redisstore.StreamStore,
) *Service {
	return &Service{
		streams: streams,
	}
}

// ReserveBid represents a new logical bid.
// It generates a new idempotency key automatically.
func (s *Service) ReserveBid(
	ctx context.Context,
	auctionID string,
	bidderID string,
	amount int64,
) (string, error) {
	return s.reserveBid(
		ctx,
		auctionID,
		bidderID,
		amount,
		uuid.NewString(),
	)
}

// ReserveBidWithIdempotencyKey represents a retryable submission.
//
// The caller supplies the same idempotency key when retrying the
// same logical bid.
func (s *Service) ReserveBidWithIdempotencyKey(
	ctx context.Context,
	auctionID string,
	bidderID string,
	amount int64,
	idempotencyKey string,
) (string, error) {
	if _, err := uuid.Parse(idempotencyKey); err != nil {
		return "", fmt.Errorf(
			"invalid idempotency key %q: %w",
			idempotencyKey,
			err,
		)
	}

	return s.reserveBid(
		ctx,
		auctionID,
		bidderID,
		amount,
		idempotencyKey,
	)
}

func (s *Service) reserveBid(
	ctx context.Context,
	auctionID string,
	bidderID string,
	amount int64,
	idempotencyKey string,
) (string, error) {
	if auctionID == "" {
		return "", fmt.Errorf(
			"auction ID is required",
		)
	}

	if bidderID == "" {
		return "", fmt.Errorf(
			"bidder ID is required",
		)
	}

	if amount <= 0 {
		return "", fmt.Errorf(
			"bid amount must be positive",
		)
	}

	if idempotencyKey == "" {
		return "", fmt.Errorf(
			"idempotency key is required",
		)
	}

	return s.streams.AppendBid(
		ctx,
		redisstore.BidIngress{
			AuctionID:      auctionID,
			BidderID:       bidderID,
			Amount:         amount,
			IdempotencyKey: idempotencyKey,
			CorrelationID:  uuid.NewString(),
		},
	)
}
