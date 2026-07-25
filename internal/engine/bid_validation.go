package engine

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const maxInt64 int64 = 1<<63 - 1

const BidRejectedEventType = "bid_rejected"

type AuctionState string

const (
	AuctionStateDraft     AuctionState = "DRAFT"
	AuctionStateScheduled AuctionState = "SCHEDULED"
	AuctionStateOpen      AuctionState = "OPEN"
	AuctionStatePaused    AuctionState = "PAUSED"
	AuctionStateClosing   AuctionState = "CLOSING"
	AuctionStateClosed    AuctionState = "CLOSED"
	AuctionStateCancelled AuctionState = "CANCELLED"
	AuctionStateSettled   AuctionState = "SETTLED"
)

type BidRejectionReason string

const (
	RejectionAuctionIDRequired BidRejectionReason = "auction_id_required"

	RejectionBidderIDRequired BidRejectionReason = "bidder_id_required"

	RejectionIdempotencyKeyRequired BidRejectionReason = "idempotency_key_required"

	RejectionInvalidAmount BidRejectionReason = "invalid_amount"

	RejectionSubmittedAtRequired BidRejectionReason = "submitted_at_required"

	RejectionAuctionNotFound BidRejectionReason = "auction_not_found"

	RejectionEffectiveCloseTimeRequired BidRejectionReason = "effective_close_time_required"

	RejectionAuctionNotOpen BidRejectionReason = "auction_not_open"

	RejectionBidderNotRegistered BidRejectionReason = "bidder_not_registered"

	RejectionBidAtOrAfterClose BidRejectionReason = "bid_at_or_after_close"

	RejectionInvalidCurrentPrice BidRejectionReason = "invalid_current_price"

	RejectionInvalidIncrement BidRejectionReason = "invalid_increment"

	RejectionPricingOverflow BidRejectionReason = "pricing_overflow"

	RejectionBidBelowMinimum BidRejectionReason = "bid_below_minimum"
)

type BidAttempt struct {
	AuctionID      string
	BidderID       string
	Amount         int64
	IdempotencyKey string
	CorrelationID  string
	StreamEntryID  string

	// Authoritative submission time derived from the Redis Stream ID.
	SubmittedAt time.Time
}

type AuctionValidationSnapshot struct {
	Exists           bool
	State            AuctionState
	CurrentPrice     int64
	Increment        int64
	BidderRegistered bool

	EffectiveCloseTime time.Time
}

type BidRejectedEvent struct {
	EventType      string
	ReasonCode     BidRejectionReason
	AuctionID      string
	BidderID       string
	IdempotencyKey string
	CorrelationID  string
	StreamEntryID  string
	AuctionState   AuctionState

	SubmittedAmount int64
	CurrentPrice    int64
	Increment       int64
	RequiredMinimum int64

	SubmittedAt        time.Time
	EffectiveCloseTime time.Time
}

type BidValidationResult struct {
	Accepted      bool
	MinimumAmount int64
	Rejection     *BidRejectedEvent
}

type BidRejectedEventSink interface {
	EmitBidRejected(
		ctx context.Context,
		event BidRejectedEvent,
	) error
}

type BidValidator struct {
	eventSink BidRejectedEventSink
}

func NewBidValidator(
	eventSink BidRejectedEventSink,
) (*BidValidator, error) {
	if eventSink == nil {
		return nil, errors.New(
			"bid rejected event sink is required",
		)
	}

	return &BidValidator{
		eventSink: eventSink,
	}, nil
}

func (v *BidValidator) Validate(
	ctx context.Context,
	attempt BidAttempt,
	auction AuctionValidationSnapshot,
) (BidValidationResult, error) {
	result := EvaluateBid(
		attempt,
		auction,
	)

	if result.Rejection == nil {
		return result, nil
	}

	if err := v.eventSink.EmitBidRejected(
		ctx,
		*result.Rejection,
	); err != nil {
		return result, fmt.Errorf(
			"emit bid rejection event: %w",
			err,
		)
	}

	return result, nil
}

func EvaluateBid(
	attempt BidAttempt,
	auction AuctionValidationSnapshot,
) BidValidationResult {
	reject := func(
		reason BidRejectionReason,
		minimumAmount int64,
	) BidValidationResult {
		event := BidRejectedEvent{
			EventType:      BidRejectedEventType,
			ReasonCode:     reason,
			AuctionID:      attempt.AuctionID,
			BidderID:       attempt.BidderID,
			IdempotencyKey: attempt.IdempotencyKey,
			CorrelationID:  attempt.CorrelationID,
			StreamEntryID:  attempt.StreamEntryID,
			AuctionState:   auction.State,

			SubmittedAmount: attempt.Amount,
			CurrentPrice:    auction.CurrentPrice,
			Increment:       auction.Increment,
			RequiredMinimum: minimumAmount,

			SubmittedAt:        attempt.SubmittedAt,
			EffectiveCloseTime: auction.EffectiveCloseTime,
		}

		return BidValidationResult{
			Accepted:      false,
			MinimumAmount: minimumAmount,
			Rejection:     &event,
		}
	}

	if attempt.AuctionID == "" {
		return reject(
			RejectionAuctionIDRequired,
			0,
		)
	}

	if attempt.BidderID == "" {
		return reject(
			RejectionBidderIDRequired,
			0,
		)
	}

	if attempt.IdempotencyKey == "" {
		return reject(
			RejectionIdempotencyKeyRequired,
			0,
		)
	}

	if attempt.Amount <= 0 {
		return reject(
			RejectionInvalidAmount,
			0,
		)
	}

	if attempt.SubmittedAt.IsZero() {
		return reject(
			RejectionSubmittedAtRequired,
			0,
		)
	}

	if !auction.Exists {
		return reject(
			RejectionAuctionNotFound,
			0,
		)
	}

	if auction.EffectiveCloseTime.IsZero() {
		return reject(
			RejectionEffectiveCloseTimeRequired,
			0,
		)
	}

	if auction.State != AuctionStateOpen {
		return reject(
			RejectionAuctionNotOpen,
			0,
		)
	}

	if !auction.BidderRegistered {
		return reject(
			RejectionBidderNotRegistered,
			0,
		)
	}

	// I3:
	// submitted_at must be strictly before effective_close_time.
	//
	// Equality is rejected.
	if !attempt.SubmittedAt.Before(
		auction.EffectiveCloseTime,
	) {
		return reject(
			RejectionBidAtOrAfterClose,
			0,
		)
	}

	if auction.CurrentPrice < 0 {
		return reject(
			RejectionInvalidCurrentPrice,
			0,
		)
	}

	if auction.Increment <= 0 {
		return reject(
			RejectionInvalidIncrement,
			0,
		)
	}

	if auction.CurrentPrice >
		maxInt64-auction.Increment {
		return reject(
			RejectionPricingOverflow,
			0,
		)
	}

	minimumAmount :=
		auction.CurrentPrice + auction.Increment

	// I6:
	// The bid must increase the previous price by at least
	// the configured increment.
	if attempt.Amount < minimumAmount {
		return reject(
			RejectionBidBelowMinimum,
			minimumAmount,
		)
	}

	return BidValidationResult{
		Accepted:      true,
		MinimumAmount: minimumAmount,
		Rejection:     nil,
	}
}
