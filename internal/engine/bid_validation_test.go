package engine

import (
	"context"
	"errors"
	"testing"
	"time"
)

var closeTime = time.Date(
	2030,
	time.January,
	1,
	12,
	0,
	0,
	0,
	time.UTC,
)

type recordingBidRejectedSink struct {
	events []BidRejectedEvent
	err    error
}

func (s *recordingBidRejectedSink) EmitBidRejected(
	_ context.Context,
	event BidRejectedEvent,
) error {
	if s.err != nil {
		return s.err
	}

	s.events = append(
		s.events,
		event,
	)

	return nil
}

func validBidAttempt() BidAttempt {
	return BidAttempt{
		AuctionID:      "auction-123",
		BidderID:       "bidder-456",
		Amount:         110,
		IdempotencyKey: "idempotency-789",
		CorrelationID:  "correlation-001",
		StreamEntryID:  "1893499199000-0",
		SubmittedAt:    closeTime.Add(-time.Second),
	}
}

func validAuctionSnapshot() AuctionValidationSnapshot {
	return AuctionValidationSnapshot{
		Exists:             true,
		State:              AuctionStateOpen,
		CurrentPrice:       100,
		Increment:          10,
		BidderRegistered:   true,
		EffectiveCloseTime: closeTime,
	}
}

func TestBidValidatorRejectsInvalidBids(
	t *testing.T,
) {
	tests := []struct {
		name            string
		changeAttempt   func(*BidAttempt)
		changeAuction   func(*AuctionValidationSnapshot)
		expectedReason  BidRejectionReason
		expectedMinimum int64
	}{
		{
			name: "auction ID missing",
			changeAttempt: func(
				attempt *BidAttempt,
			) {
				attempt.AuctionID = ""
			},
			expectedReason: RejectionAuctionIDRequired,
		},
		{
			name: "bidder ID missing",
			changeAttempt: func(
				attempt *BidAttempt,
			) {
				attempt.BidderID = ""
			},
			expectedReason: RejectionBidderIDRequired,
		},
		{
			name: "idempotency key missing",
			changeAttempt: func(
				attempt *BidAttempt,
			) {
				attempt.IdempotencyKey = ""
			},
			expectedReason: RejectionIdempotencyKeyRequired,
		},
		{
			name: "amount equals zero",
			changeAttempt: func(
				attempt *BidAttempt,
			) {
				attempt.Amount = 0
			},
			expectedReason: RejectionInvalidAmount,
		},
		{
			name: "amount is negative",
			changeAttempt: func(
				attempt *BidAttempt,
			) {
				attempt.Amount = -1
			},
			expectedReason: RejectionInvalidAmount,
		},
		{
			name: "submitted timestamp missing",
			changeAttempt: func(
				attempt *BidAttempt,
			) {
				attempt.SubmittedAt = time.Time{}
			},
			expectedReason: RejectionSubmittedAtRequired,
		},
		{
			name: "auction does not exist",
			changeAuction: func(
				auction *AuctionValidationSnapshot,
			) {
				auction.Exists = false
			},
			expectedReason: RejectionAuctionNotFound,
		},
		{
			name: "effective close time missing",
			changeAuction: func(
				auction *AuctionValidationSnapshot,
			) {
				auction.EffectiveCloseTime = time.Time{}
			},
			expectedReason: RejectionEffectiveCloseTimeRequired,
		},
		{
			name: "auction is DRAFT",
			changeAuction: func(
				auction *AuctionValidationSnapshot,
			) {
				auction.State = AuctionStateDraft
			},
			expectedReason: RejectionAuctionNotOpen,
		},
		{
			name: "auction is CLOSED",
			changeAuction: func(
				auction *AuctionValidationSnapshot,
			) {
				auction.State = AuctionStateClosed
			},
			expectedReason: RejectionAuctionNotOpen,
		},
		{
			name: "bidder is not registered",
			changeAuction: func(
				auction *AuctionValidationSnapshot,
			) {
				auction.BidderRegistered = false
			},
			expectedReason: RejectionBidderNotRegistered,
		},
		{
			name: "bid submitted exactly at close",
			changeAttempt: func(
				attempt *BidAttempt,
			) {
				attempt.SubmittedAt = closeTime
			},
			expectedReason: RejectionBidAtOrAfterClose,
		},
		{
			name: "bid submitted after close",
			changeAttempt: func(
				attempt *BidAttempt,
			) {
				attempt.SubmittedAt =
					closeTime.Add(time.Millisecond)
			},
			expectedReason: RejectionBidAtOrAfterClose,
		},
		{
			name: "current price is negative",
			changeAuction: func(
				auction *AuctionValidationSnapshot,
			) {
				auction.CurrentPrice = -1
			},
			expectedReason: RejectionInvalidCurrentPrice,
		},
		{
			name: "increment equals zero",
			changeAuction: func(
				auction *AuctionValidationSnapshot,
			) {
				auction.Increment = 0
			},
			expectedReason: RejectionInvalidIncrement,
		},
		{
			name: "increment is negative",
			changeAuction: func(
				auction *AuctionValidationSnapshot,
			) {
				auction.Increment = -10
			},
			expectedReason: RejectionInvalidIncrement,
		},
		{
			name: "pricing calculation overflows",
			changeAuction: func(
				auction *AuctionValidationSnapshot,
			) {
				auction.CurrentPrice = maxInt64 - 5
				auction.Increment = 10
			},
			expectedReason: RejectionPricingOverflow,
		},
		{
			name: "bid is below required minimum",
			changeAttempt: func(
				attempt *BidAttempt,
			) {
				attempt.Amount = 109
			},
			expectedReason:  RejectionBidBelowMinimum,
			expectedMinimum: 110,
		},
	}

	if len(tests) < 12 {
		t.Fatalf(
			"expected at least 12 rejection paths, got %d",
			len(tests),
		)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			attempt := validBidAttempt()
			auction := validAuctionSnapshot()

			if test.changeAttempt != nil {
				test.changeAttempt(&attempt)
			}

			if test.changeAuction != nil {
				test.changeAuction(&auction)
			}

			sink := &recordingBidRejectedSink{}

			validator, err := NewBidValidator(sink)
			if err != nil {
				t.Fatalf(
					"create validator: %v",
					err,
				)
			}

			result, err := validator.Validate(
				context.Background(),
				attempt,
				auction,
			)
			if err != nil {
				t.Fatalf(
					"validate rejected bid: %v",
					err,
				)
			}

			if result.Accepted {
				t.Fatal(
					"invalid bid was accepted",
				)
			}

			if result.Rejection == nil {
				t.Fatal(
					"rejected bid has no rejection event",
				)
			}

			if result.Rejection.ReasonCode !=
				test.expectedReason {
				t.Fatalf(
					"expected reason %q, got %q",
					test.expectedReason,
					result.Rejection.ReasonCode,
				)
			}

			if result.MinimumAmount !=
				test.expectedMinimum {
				t.Fatalf(
					"expected minimum %d, got %d",
					test.expectedMinimum,
					result.MinimumAmount,
				)
			}

			if len(sink.events) != 1 {
				t.Fatalf(
					"expected one emitted event, got %d",
					len(sink.events),
				)
			}

			emitted := sink.events[0]

			if emitted.EventType !=
				BidRejectedEventType {
				t.Fatalf(
					"expected event type %q, got %q",
					BidRejectedEventType,
					emitted.EventType,
				)
			}

			if emitted.ReasonCode !=
				test.expectedReason {
				t.Fatalf(
					"expected emitted reason %q, got %q",
					test.expectedReason,
					emitted.ReasonCode,
				)
			}

			if !emitted.SubmittedAt.Equal(
				attempt.SubmittedAt,
			) {
				t.Fatalf(
					"event submitted time %s, expected %s",
					emitted.SubmittedAt,
					attempt.SubmittedAt,
				)
			}

			if !emitted.EffectiveCloseTime.Equal(
				auction.EffectiveCloseTime,
			) {
				t.Fatalf(
					"event close time %s, expected %s",
					emitted.EffectiveCloseTime,
					auction.EffectiveCloseTime,
				)
			}
		})
	}
}

func TestBidValidatorAcceptsValidBids(
	t *testing.T,
) {
	tests := []struct {
		name          string
		currentPrice  int64
		increment     int64
		amount        int64
		expectedFloor int64
	}{
		{
			name:          "amount equals required minimum",
			currentPrice:  100,
			increment:     10,
			amount:        110,
			expectedFloor: 110,
		},
		{
			name:          "amount exceeds required minimum",
			currentPrice:  100,
			increment:     10,
			amount:        250,
			expectedFloor: 110,
		},
		{
			name:          "first price step from zero",
			currentPrice:  0,
			increment:     1,
			amount:        1,
			expectedFloor: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			attempt := validBidAttempt()
			attempt.Amount = test.amount

			auction := validAuctionSnapshot()
			auction.CurrentPrice =
				test.currentPrice
			auction.Increment =
				test.increment

			sink := &recordingBidRejectedSink{}

			validator, err := NewBidValidator(sink)
			if err != nil {
				t.Fatalf(
					"create validator: %v",
					err,
				)
			}

			result, err := validator.Validate(
				context.Background(),
				attempt,
				auction,
			)
			if err != nil {
				t.Fatalf(
					"validate accepted bid: %v",
					err,
				)
			}

			if !result.Accepted {
				t.Fatalf(
					"valid bid was rejected: %+v",
					result.Rejection,
				)
			}

			if result.Rejection != nil {
				t.Fatalf(
					"accepted bid returned rejection: %+v",
					result.Rejection,
				)
			}

			if result.MinimumAmount !=
				test.expectedFloor {
				t.Fatalf(
					"expected minimum %d, got %d",
					test.expectedFloor,
					result.MinimumAmount,
				)
			}

			if len(sink.events) != 0 {
				t.Fatalf(
					"accepted bid emitted %d events",
					len(sink.events),
				)
			}
		})
	}
}

func TestBidValidatorReturnsEventSinkFailure(
	t *testing.T,
) {
	expectedError := errors.New(
		"rejection event storage unavailable",
	)

	sink := &recordingBidRejectedSink{
		err: expectedError,
	}

	validator, err := NewBidValidator(sink)
	if err != nil {
		t.Fatalf(
			"create validator: %v",
			err,
		)
	}

	attempt := validBidAttempt()
	attempt.Amount = 109

	result, err := validator.Validate(
		context.Background(),
		attempt,
		validAuctionSnapshot(),
	)

	if err == nil {
		t.Fatal(
			"expected event sink failure",
		)
	}

	if !errors.Is(err, expectedError) {
		t.Fatalf(
			"expected wrapped error %v, got %v",
			expectedError,
			err,
		)
	}

	if result.Accepted {
		t.Fatal(
			"invalid bid was accepted",
		)
	}

	if result.Rejection == nil {
		t.Fatal(
			"rejection details were lost",
		)
	}
}

func TestNewBidValidatorRequiresEventSink(
	t *testing.T,
) {
	validator, err := NewBidValidator(nil)

	if err == nil {
		t.Fatal(
			"expected missing event sink error",
		)
	}

	if validator != nil {
		t.Fatal(
			"validator must be nil",
		)
	}
}
