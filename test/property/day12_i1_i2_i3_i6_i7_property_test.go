package property_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"pgregory.net/rapid"

	"example.com/bidlane/internal/engine"
	postgresstore "example.com/bidlane/internal/store/postgres"
)

type day12Model struct {
	OpeningPrice int64
	CurrentPrice int64
	Increment    int64

	InitialCloseTime   time.Time
	EffectiveCloseTime time.Time
	ExtensionWindow    time.Duration
	ExtensionInterval  time.Duration
	ExtensionCount     int32
	MaxExtensions      int32
}

func TestDay12GeneratedHistoriesPreserveI1I2I3I6I7(
	t *testing.T,
) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		80*time.Minute,
	)
	defer cancel()

	postgresDSN := os.Getenv("POSTGRES_ADMIN_DSN")
	if postgresDSN == "" {
		postgresDSN =
			"postgres://bidlane:bidlane@" +
				"127.0.0.1:55432/" +
				"bidlane?sslmode=disable"
	}

	adminPool, err := postgresstore.ConnectPool(
		ctx,
		postgresDSN,
		"",
	)
	if err != nil {
		t.Fatalf(
			"connect Day 12 administrator pool: %v",
			err,
		)
	}
	defer adminPool.Close()

	resetDay11Tables(
		t,
		ctx,
		adminPool,
	)

	enginePool, err := postgresstore.ConnectPool(
		ctx,
		postgresDSN,
		"bidlane_engine",
	)
	if err != nil {
		t.Fatalf(
			"connect Day 12 Engine pool: %v",
			err,
		)
	}
	defer enginePool.Close()

	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			30*time.Second,
		)
		defer cleanupCancel()

		_, _ = adminPool.Exec(
			cleanupCtx,
			`
			TRUNCATE TABLE
				outbox,
				bids,
				auction_sequences,
				auctions
			CASCADE
			`,
		)
	}()

	ledger := postgresstore.NewLedgerStore(
		enginePool,
	)

	rapid.Check(t, func(rt *rapid.T) {
		historyCtx, historyCancel := context.WithTimeout(
			ctx,
			20*time.Second,
		)
		defer historyCancel()

		auctionID := uuid.New()

		startOffsetMilliseconds := rapid.IntRange(
			0,
			60_000,
		).Draw(
			rt,
			"start_offset_milliseconds",
		)

		logicalClock := time.Date(
			2035,
			time.January,
			1,
			12,
			0,
			0,
			0,
			time.UTC,
		).Add(
			time.Duration(startOffsetMilliseconds) * time.Millisecond,
		)

		openingPrice := rapid.Int64Range(
			1_000,
			20_000,
		).Draw(
			rt,
			"opening_price",
		)

		increment := rapid.Int64Range(
			10,
			500,
		).Draw(
			rt,
			"bid_increment",
		)

		maxExtensions := int32(
			rapid.IntRange(
				1,
				4,
			).Draw(
				rt,
				"max_extensions",
			),
		)

		extensionWindow := 30 * time.Second
		extensionInterval := 30 * time.Second
		initialCloseTime := logicalClock.Add(2 * time.Minute)

		if _, err := adminPool.Exec(
			historyCtx,
			`
			INSERT INTO auctions (
				id,
				effective_close_time,
				extension_window,
				extension_interval,
				max_extensions,
				extension_count
			)
			VALUES (
				$1,
				$2,
				INTERVAL '30 seconds',
				INTERVAL '30 seconds',
				$3,
				0
			)
			`,
			auctionID,
			initialCloseTime,
			maxExtensions,
		); err != nil {
			rt.Fatalf(
				"create Day 12 generated auction: %v",
				err,
			)
		}

		if _, err := adminPool.Exec(
			historyCtx,
			`
			INSERT INTO auction_sequences (
				auction_id,
				last_seq
			)
			VALUES ($1, 0)
			`,
			auctionID,
		); err != nil {
			rt.Fatalf(
				"create Day 12 generated sequence row: %v",
				err,
			)
		}

		model := day12Model{
			OpeningPrice:       openingPrice,
			CurrentPrice:       openingPrice,
			Increment:          increment,
			InitialCloseTime:   initialCloseTime,
			EffectiveCloseTime: initialCloseTime,
			ExtensionWindow:    extensionWindow,
			ExtensionInterval:  extensionInterval,
			ExtensionCount:     0,
			MaxExtensions:      maxExtensions,
		}

		expectedBids := make(
			map[uuid.UUID]day11LogicalBid,
		)

		committedBids := make(
			[]day11LogicalBid,
			0,
		)

		acceptedCloseBefore := make(
			map[uuid.UUID]time.Time,
		)

		rejectedBids := make(
			map[uuid.UUID]engine.BidRejectionReason,
		)

		nextBidOrdinal := 1

		// Begin every history with one definitely valid bid.
		initialExtra := rapid.Int64Range(
			0,
			1_000,
		).Draw(
			rt,
			"initial_valid_extra",
		)

		initialBid := newDay12LogicalBid(
			auctionID,
			nextBidOrdinal,
			model.CurrentPrice+model.Increment+initialExtra,
			logicalClock,
		)
		nextBidOrdinal++

		applyDay12AcceptedBid(
			rt,
			historyCtx,
			ledger,
			&model,
			initialBid,
			expectedBids,
			&committedBids,
			acceptedCloseBefore,
		)

		// Guaranteed I6 negative probe: exactly one unit below minimum.
		belowMinimum := model.CurrentPrice + model.Increment - 1
		underBid := newDay12LogicalBid(
			auctionID,
			nextBidOrdinal,
			belowMinimum,
			logicalClock,
		)
		nextBidOrdinal++

		applyDay12RejectedBid(
			rt,
			&model,
			underBid,
			engine.RejectionBidBelowMinimum,
			rejectedBids,
		)

		// Random Phase-0 style history: bid / crash replay / tick.
		// Keep these actions outside the extension window; I7 is then
		// exercised deliberately below so every generated history covers it.
		actionCount := rapid.IntRange(
			1,
			12,
		).Draw(
			rt,
			"action_count",
		)

		for actionIndex := 0; actionIndex < actionCount; actionIndex++ {
			actionKind := rapid.IntRange(
				0,
				3,
			).Draw(
				rt,
				fmt.Sprintf(
					"action_%d_kind",
					actionIndex,
				),
			)

			switch actionKind {
			case 0:
				// Valid BID.
				extra := rapid.Int64Range(
					0,
					1_000,
				).Draw(
					rt,
					fmt.Sprintf(
						"action_%d_valid_extra",
						actionIndex,
					),
				)

				bid := newDay12LogicalBid(
					auctionID,
					nextBidOrdinal,
					model.CurrentPrice+model.Increment+extra,
					logicalClock,
				)
				nextBidOrdinal++

				applyDay12AcceptedBid(
					rt,
					historyCtx,
					ledger,
					&model,
					bid,
					expectedBids,
					&committedBids,
					acceptedCloseBefore,
				)

			case 1:
				// Invalid I6 BID.
				bid := newDay12LogicalBid(
					auctionID,
					nextBidOrdinal,
					model.CurrentPrice+model.Increment-1,
					logicalClock,
				)
				nextBidOrdinal++

				applyDay12RejectedBid(
					rt,
					&model,
					bid,
					engine.RejectionBidBelowMinimum,
					rejectedBids,
				)

			case 2:
				// CRASH/replay of an already committed logical bid.
				replayIndex := rapid.IntRange(
					0,
					len(committedBids)-1,
				).Draw(
					rt,
					fmt.Sprintf(
						"action_%d_replay_index",
						actionIndex,
					),
				)

				applyDay12Replay(
					rt,
					historyCtx,
					ledger,
					&model,
					committedBids[replayIndex],
				)

			case 3:
				// TICK, capped one second before the extension window.
				advanceMilliseconds := rapid.IntRange(
					1,
					10_000,
				).Draw(
					rt,
					fmt.Sprintf(
						"action_%d_tick_milliseconds",
						actionIndex,
					),
				)

				candidate := logicalClock.Add(
					time.Duration(advanceMilliseconds) * time.Millisecond,
				)

				safeLatest := model.EffectiveCloseTime.
					Add(-model.ExtensionWindow).
					Add(-time.Second)

				if candidate.After(safeLatest) {
					logicalClock = safeLatest
				} else {
					logicalClock = candidate
				}
			}
		}

		// Guaranteed I7 positive probes. Move to the middle of the
		// current extension window and accept one valid bid per allowed
		// extension until max_extensions is reached.
		for model.ExtensionCount < model.MaxExtensions {
			logicalClock = model.EffectiveCloseTime.Add(
				-model.ExtensionWindow / 2,
			)

			extra := rapid.Int64Range(
				0,
				1_000,
			).Draw(
				rt,
				fmt.Sprintf(
					"extension_%d_valid_extra",
					model.ExtensionCount,
				),
			)

			bid := newDay12LogicalBid(
				auctionID,
				nextBidOrdinal,
				model.CurrentPrice+model.Increment+extra,
				logicalClock,
			)
			nextBidOrdinal++

			result := applyDay12AcceptedBid(
				rt,
				historyCtx,
				ledger,
				&model,
				bid,
				expectedBids,
				&committedBids,
				acceptedCloseBefore,
			)

			if !result.Extended {
				rt.Fatalf(
					"I7 violated: eligible inside-window bid did not extend",
				)
			}
		}

		// Guaranteed I7 bound probe. This bid is inside the window but
		// max_extensions has already been reached, so it must be accepted
		// without extending again.
		logicalClock = model.EffectiveCloseTime.Add(-time.Second)
		capProbe := newDay12LogicalBid(
			auctionID,
			nextBidOrdinal,
			model.CurrentPrice+model.Increment,
			logicalClock,
		)
		nextBidOrdinal++

		capResult := applyDay12AcceptedBid(
			rt,
			historyCtx,
			ledger,
			&model,
			capProbe,
			expectedBids,
			&committedBids,
			acceptedCloseBefore,
		)

		if capResult.Extended {
			rt.Fatalf(
				"I7 violated: bid extended after max_extensions=%d",
				model.MaxExtensions,
			)
		}

		// Guaranteed I3 boundary probe. Equality with close time is late.
		logicalClock = model.EffectiveCloseTime
		lateBid := newDay12LogicalBid(
			auctionID,
			nextBidOrdinal,
			model.CurrentPrice+model.Increment+1_000,
			logicalClock,
		)

		applyDay12LateBid(
			rt,
			historyCtx,
			ledger,
			&model,
			lateBid,
			rejectedBids,
		)

		// Reuse the proven Day 11 I1 + I2 oracle.
		assertDay11I1AndI2(
			rt,
			historyCtx,
			adminPool,
			auctionID,
			expectedBids,
		)

		assertDay12I3I6I7(
			rt,
			historyCtx,
			adminPool,
			auctionID,
			model,
			expectedBids,
			acceptedCloseBefore,
			rejectedBids,
		)
	})
}

func newDay12LogicalBid(
	auctionID uuid.UUID,
	ordinal int,
	amount int64,
	submittedAt time.Time,
) day11LogicalBid {
	return day11LogicalBid{
		PersistedBid: postgresstore.BidToPersist{
			AuctionID:      auctionID,
			BidderID:       uuid.New(),
			Amount:         amount,
			IdempotencyKey: uuid.New(),
			StreamEntryID: fmt.Sprintf(
				"day12-%s-%06d",
				auctionID.String(),
				ordinal,
			),
			CorrelationID: uuid.NewString(),
		},
		SubmittedAt: submittedAt,
	}
}

func day12Attempt(
	bid day11LogicalBid,
) engine.BidAttempt {
	return engine.BidAttempt{
		AuctionID:      bid.PersistedBid.AuctionID.String(),
		BidderID:       bid.PersistedBid.BidderID.String(),
		Amount:         bid.PersistedBid.Amount,
		IdempotencyKey: bid.PersistedBid.IdempotencyKey.String(),
		CorrelationID:  bid.PersistedBid.CorrelationID,
		StreamEntryID:  bid.PersistedBid.StreamEntryID,
		SubmittedAt:    bid.SubmittedAt,
	}
}

func day12Snapshot(
	model day12Model,
) engine.AuctionValidationSnapshot {
	return engine.AuctionValidationSnapshot{
		Exists:             true,
		State:              engine.AuctionStateOpen,
		CurrentPrice:       model.CurrentPrice,
		Increment:          model.Increment,
		BidderRegistered:   true,
		EffectiveCloseTime: model.EffectiveCloseTime,
	}
}

func applyDay12AcceptedBid(
	rt *rapid.T,
	ctx context.Context,
	ledger *postgresstore.LedgerStore,
	model *day12Model,
	bid day11LogicalBid,
	expected map[uuid.UUID]day11LogicalBid,
	committed *[]day11LogicalBid,
	acceptedCloseBefore map[uuid.UUID]time.Time,
) postgresstore.AntiSnipeAppendResult {
	closeBefore := model.EffectiveCloseTime
	countBefore := model.ExtensionCount

	validation := engine.EvaluateBid(
		day12Attempt(bid),
		day12Snapshot(*model),
	)

	if !validation.Accepted || validation.Rejection != nil {
		reason := engine.BidRejectionReason("")
		if validation.Rejection != nil {
			reason = validation.Rejection.ReasonCode
		}

		rt.Fatalf(
			"expected valid bid to pass I3/I6 validation: amount=%d reason=%s",
			bid.PersistedBid.Amount,
			reason,
		)
	}

	expectedExtended :=
		bid.SubmittedAt.Before(closeBefore) &&
			!bid.SubmittedAt.Before(
				closeBefore.Add(-model.ExtensionWindow),
			) &&
			countBefore < model.MaxExtensions

	expectedCloseAfter := closeBefore
	expectedCountAfter := countBefore

	if expectedExtended {
		expectedCloseAfter = expectedCloseAfter.Add(
			model.ExtensionInterval,
		)
		expectedCountAfter++
	}

	result, err := ledger.AppendBidIdempotentWithAntiSnipe(
		ctx,
		bid.PersistedBid,
		bid.SubmittedAt,
	)
	if err != nil {
		rt.Fatalf(
			"accepted Day 12 bid failed persistence: amount=%d key=%s error=%v",
			bid.PersistedBid.Amount,
			bid.PersistedBid.IdempotencyKey,
			err,
		)
	}

	if !result.Inserted {
		rt.Fatalf(
			"new Day 12 valid bid was treated as replay: key=%s",
			bid.PersistedBid.IdempotencyKey,
		)
	}

	if result.SequenceNumber <= 0 {
		rt.Fatalf(
			"I1 violated: accepted bid received invalid sequence %d",
			result.SequenceNumber,
		)
	}

	if result.Extended != expectedExtended {
		rt.Fatalf(
			"I7 violated: extended=%v expected=%v submitted=%s close_before=%s count_before=%d max=%d",
			result.Extended,
			expectedExtended,
			bid.SubmittedAt,
			closeBefore,
			countBefore,
			model.MaxExtensions,
		)
	}

	if !result.EffectiveCloseTime.Equal(expectedCloseAfter) {
		rt.Fatalf(
			"I7 violated: close after bid=%s expected=%s",
			result.EffectiveCloseTime,
			expectedCloseAfter,
		)
	}

	if result.ExtensionCount != expectedCountAfter {
		rt.Fatalf(
			"I7 violated: extension_count=%d expected=%d",
			result.ExtensionCount,
			expectedCountAfter,
		)
	}

	if result.ExtensionCount > model.MaxExtensions {
		rt.Fatalf(
			"I7 violated: extension_count=%d exceeds max_extensions=%d",
			result.ExtensionCount,
			model.MaxExtensions,
		)
	}

	acceptedCloseBefore[bid.PersistedBid.IdempotencyKey] = closeBefore
	expected[bid.PersistedBid.IdempotencyKey] = bid
	*committed = append(*committed, bid)

	model.CurrentPrice = bid.PersistedBid.Amount
	model.EffectiveCloseTime = expectedCloseAfter
	model.ExtensionCount = expectedCountAfter

	return result
}

func applyDay12RejectedBid(
	rt *rapid.T,
	model *day12Model,
	bid day11LogicalBid,
	expectedReason engine.BidRejectionReason,
	rejected map[uuid.UUID]engine.BidRejectionReason,
) {
	validation := engine.EvaluateBid(
		day12Attempt(bid),
		day12Snapshot(*model),
	)

	if validation.Accepted || validation.Rejection == nil {
		rt.Fatalf(
			"expected rejected bid to fail validation: amount=%d expected_reason=%s",
			bid.PersistedBid.Amount,
			expectedReason,
		)
	}

	if validation.Rejection.ReasonCode != expectedReason {
		rt.Fatalf(
			"wrong rejection reason: got=%s expected=%s amount=%d",
			validation.Rejection.ReasonCode,
			expectedReason,
			bid.PersistedBid.Amount,
		)
	}

	if expectedReason == engine.RejectionBidBelowMinimum {
		expectedMinimum := model.CurrentPrice + model.Increment
		if validation.MinimumAmount != expectedMinimum {
			rt.Fatalf(
				"I6 violated: validator minimum=%d expected=%d",
				validation.MinimumAmount,
				expectedMinimum,
			)
		}
	}

	rejected[bid.PersistedBid.IdempotencyKey] = expectedReason
}

func applyDay12LateBid(
	rt *rapid.T,
	ctx context.Context,
	ledger *postgresstore.LedgerStore,
	model *day12Model,
	bid day11LogicalBid,
	rejected map[uuid.UUID]engine.BidRejectionReason,
) {
	validation := engine.EvaluateBid(
		day12Attempt(bid),
		day12Snapshot(*model),
	)

	if validation.Accepted || validation.Rejection == nil {
		rt.Fatalf(
			"I3 violated: bid at close time passed validator: submitted=%s close=%s",
			bid.SubmittedAt,
			model.EffectiveCloseTime,
		)
	}

	if validation.Rejection.ReasonCode != engine.RejectionBidAtOrAfterClose {
		rt.Fatalf(
			"I3 violated: late bid rejected for wrong reason: got=%s",
			validation.Rejection.ReasonCode,
		)
	}

	// Probe the authoritative PostgreSQL transaction too. Even if a
	// caller bypasses the Go validator, the ledger must reject a late bid.
	result, err := ledger.AppendBidIdempotentWithAntiSnipe(
		ctx,
		bid.PersistedBid,
		bid.SubmittedAt,
	)

	if !errors.Is(err, postgresstore.ErrBidAtOrAfterClose) {
		rt.Fatalf(
			"I3 violated: authoritative ledger accepted/wrongly rejected late bid: result=%+v error=%v",
			result,
			err,
		)
	}

	rejected[bid.PersistedBid.IdempotencyKey] =
		engine.RejectionBidAtOrAfterClose
}

func applyDay12Replay(
	rt *rapid.T,
	ctx context.Context,
	ledger *postgresstore.LedgerStore,
	model *day12Model,
	bid day11LogicalBid,
) {
	result, err := ledger.AppendBidIdempotentWithAntiSnipe(
		ctx,
		bid.PersistedBid,
		bid.SubmittedAt,
	)
	if err != nil {
		rt.Fatalf(
			"Day 12 crash replay failed: key=%s error=%v",
			bid.PersistedBid.IdempotencyKey,
			err,
		)
	}

	if result.Inserted {
		rt.Fatalf(
			"I1/idempotency violated: replay inserted duplicate key=%s sequence=%d",
			bid.PersistedBid.IdempotencyKey,
			result.SequenceNumber,
		)
	}

	if result.Extended {
		rt.Fatalf(
			"I7 violated: replay extended auction again key=%s",
			bid.PersistedBid.IdempotencyKey,
		)
	}

	if !result.EffectiveCloseTime.Equal(model.EffectiveCloseTime) {
		rt.Fatalf(
			"I7 violated: replay changed/returned wrong close time: got=%s expected=%s",
			result.EffectiveCloseTime,
			model.EffectiveCloseTime,
		)
	}

	if result.ExtensionCount != model.ExtensionCount {
		rt.Fatalf(
			"I7 violated: replay changed/returned wrong extension count: got=%d expected=%d",
			result.ExtensionCount,
			model.ExtensionCount,
		)
	}
}

func assertDay12I3I6I7(
	rt *rapid.T,
	ctx context.Context,
	adminPool *pgxpool.Pool,
	auctionID uuid.UUID,
	model day12Model,
	expected map[uuid.UUID]day11LogicalBid,
	acceptedCloseBefore map[uuid.UUID]time.Time,
	rejected map[uuid.UUID]engine.BidRejectionReason,
) {
	ledgerRows, err := readDay11Ledger(
		ctx,
		adminPool,
		auctionID,
	)
	if err != nil {
		rt.Fatalf(
			"read Day 12 ledger: %v",
			err,
		)
	}

	storedKeys := make(
		map[uuid.UUID]struct{},
		len(ledgerRows),
	)

	previousPrice := model.OpeningPrice

	for _, stored := range ledgerRows {
		storedKeys[stored.IdempotencyKey] = struct{}{}

		logicalBid, exists := expected[stored.IdempotencyKey]
		if !exists {
			rt.Fatalf(
				"unexpected Day 12 accepted bid key=%s",
				stored.IdempotencyKey,
			)
		}

		// I6: every accepted ledger transition must advance by at least
		// the configured increment.
		requiredMinimum := previousPrice + model.Increment
		if stored.Amount < requiredMinimum {
			rt.Fatalf(
				"I6 violated: sequence=%d amount=%d previous=%d increment=%d required_minimum=%d",
				stored.SequenceNumber,
				stored.Amount,
				previousPrice,
				model.Increment,
				requiredMinimum,
			)
		}
		previousPrice = stored.Amount

		// I3: each accepted bid must have been strictly before the
		// effective close time that existed when it was accepted.
		closeBefore, exists := acceptedCloseBefore[stored.IdempotencyKey]
		if !exists {
			rt.Fatalf(
				"I3 oracle missing close snapshot for key=%s",
				stored.IdempotencyKey,
			)
		}

		if !logicalBid.SubmittedAt.Before(closeBefore) {
			rt.Fatalf(
				"I3 violated: accepted key=%s submitted=%s close_before=%s",
				stored.IdempotencyKey,
				logicalBid.SubmittedAt,
				closeBefore,
			)
		}
	}

	if previousPrice != model.CurrentPrice {
		rt.Fatalf(
			"I6 violated: ledger final price=%d model current price=%d",
			previousPrice,
			model.CurrentPrice,
		)
	}

	// Every generated invalid/late bid must be absent from the ledger.
	for key, reason := range rejected {
		if _, exists := storedKeys[key]; exists {
			rt.Fatalf(
				"rejected bid entered ledger: key=%s reason=%s",
				key,
				reason,
			)
		}
	}

	var (
		actualCloseTime      time.Time
		actualExtensionCount int32
		actualMaxExtensions  int32
	)

	if err := adminPool.QueryRow(
		ctx,
		`
		SELECT
			effective_close_time,
			extension_count,
			max_extensions
		FROM auctions
		WHERE id = $1
		`,
		auctionID,
	).Scan(
		&actualCloseTime,
		&actualExtensionCount,
		&actualMaxExtensions,
	); err != nil {
		rt.Fatalf(
			"read Day 12 auction extension state: %v",
			err,
		)
	}

	// I7: database state must exactly match the independent model.
	if !actualCloseTime.Equal(model.EffectiveCloseTime) {
		rt.Fatalf(
			"I7 violated: database close=%s model close=%s",
			actualCloseTime,
			model.EffectiveCloseTime,
		)
	}

	if actualExtensionCount != model.ExtensionCount {
		rt.Fatalf(
			"I7 violated: database extension_count=%d model=%d",
			actualExtensionCount,
			model.ExtensionCount,
		)
	}

	if actualMaxExtensions != model.MaxExtensions {
		rt.Fatalf(
			"I7 violated: database max_extensions=%d model=%d",
			actualMaxExtensions,
			model.MaxExtensions,
		)
	}

	if actualExtensionCount > actualMaxExtensions {
		rt.Fatalf(
			"I7 violated: extension_count=%d exceeds max_extensions=%d",
			actualExtensionCount,
			actualMaxExtensions,
		)
	}

	// Every history deliberately exhausts the extension allowance and
	// then submits one more inside-window bid, so reaching the cap is
	// itself part of the property rather than an unexercised branch.
	if actualExtensionCount != actualMaxExtensions {
		rt.Fatalf(
			"I7 coverage failure: expected to reach extension cap %d, got %d",
			actualMaxExtensions,
			actualExtensionCount,
		)
	}

	expectedCloseFromCount := model.InitialCloseTime.Add(
		time.Duration(actualExtensionCount) * model.ExtensionInterval,
	)

	if !actualCloseTime.Equal(expectedCloseFromCount) {
		rt.Fatalf(
			"I7 violated: close=%s does not equal initial_close + extension_count*interval=%s",
			actualCloseTime,
			expectedCloseFromCount,
		)
	}
}
