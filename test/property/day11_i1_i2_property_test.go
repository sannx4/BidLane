package property_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"pgregory.net/rapid"

	postgresstore "example.com/bidlane/internal/store/postgres"
)

// day11LogicalBid is the model's representation of one logical bid.
//
// A crash replay copies this complete value, including the same
// idempotency key.
type day11LogicalBid struct {
	PersistedBid postgresstore.BidToPersist
	SubmittedAt  time.Time
}

// day11AppendOutcome records what happened when one goroutine called
// the real PostgreSQL ledger transaction.
type day11AppendOutcome struct {
	Bid    day11LogicalBid
	Result postgresstore.AntiSnipeAppendResult
	Err    error
}

// day11LedgerRow is the immutable database representation read by
// the property assertions.
type day11LedgerRow struct {
	SequenceNumber int64
	Amount         int64
	BidderID       uuid.UUID
	IdempotencyKey uuid.UUID
	StreamEntryID  string
}

func TestDay11GeneratedHistoriesPreserveI1AndI2(
	t *testing.T,
) {
	// The 10,000-history run is deliberately a long integration test.
	ctx, cancel := context.WithTimeout(
		context.Background(),
		80*time.Minute,
	)
	defer cancel()

	postgresDSN := os.Getenv(
		"POSTGRES_ADMIN_DSN",
	)
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
			"connect Day 11 administrator pool: %v",
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
			"connect Day 11 Engine pool: %v",
			err,
		)
	}
	defer enginePool.Close()

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel :=
			context.WithTimeout(
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
	})

	ledger := postgresstore.NewLedgerStore(
		enginePool,
	)

	rapid.Check(t, func(rt *rapid.T) {
		historyCtx, historyCancel :=
			context.WithTimeout(
				ctx,
				20*time.Second,
			)
		defer historyCancel()

		auctionID := uuid.New()

		startOffsetMilliseconds :=
			rapid.IntRange(
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
			time.Duration(
				startOffsetMilliseconds,
			) * time.Millisecond,
		)

		// Day 11 is proving I1 and I2.
		//
		// The close time is deliberately far away so Day 12 can add
		// late-bid and extension properties without changing the
		// action generator.
		effectiveCloseTime :=
			logicalClock.Add(24 * time.Hour)

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
				10,
				0
			)
			`,
			auctionID,
			effectiveCloseTime,
		); err != nil {
			rt.Fatalf(
				"create generated auction: %v",
				err,
			)
		}

		// Pre-create the counter row.
		//
		// Without this, concurrent transactions could serialize while
		// competing to create the first auction_sequences row. That
		// would hide the deliberate missing-FOR-UPDATE mutation.
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
				"create generated sequence row: %v",
				err,
			)
		}

		expectedBids := make(
			map[uuid.UUID]day11LogicalBid,
		)

		committedBids := make(
			[]day11LogicalBid,
			0,
		)

		pendingBids := make(
			[]day11LogicalBid,
			0,
		)

		nextBidOrdinal := 1

		// Every generated history begins with a concurrent wave.
		//
		// This makes the row-lock property exercise real contention
		// rather than accidentally testing only sequential execution.
		initialWaveSize :=
			rapid.IntRange(
				2,
				5,
			).Draw(
				rt,
				"initial_wave_size",
			)

		initialWave := make(
			[]day11LogicalBid,
			0,
			initialWaveSize,
		)

		for index := 0; index < initialWaveSize; index++ {
			jitter := rapid.Int64Range(
				0,
				99,
			).Draw(
				rt,
				fmt.Sprintf(
					"initial_bid_%d_jitter",
					index,
				),
			)

			bid := newDay11LogicalBid(
				auctionID,
				nextBidOrdinal,
				jitter,
				logicalClock,
			)

			nextBidOrdinal++
			initialWave = append(
				initialWave,
				bid,
			)
		}

		applyDay11NewBidWave(
			rt,
			historyCtx,
			ledger,
			initialWave,
		)

		recordDay11CommittedBids(
			expectedBids,
			&committedBids,
			initialWave,
		)

		actionCount := rapid.IntRange(
			1,
			12,
		).Draw(
			rt,
			"action_count",
		)

		for actionIndex := 0; actionIndex < actionCount; actionIndex++ {
			// Distribution:
			//
			// 0,1,2 = BID
			// 3     = CRASH
			// 4     = TICK
			actionKind := rapid.IntRange(
				0,
				4,
			).Draw(
				rt,
				fmt.Sprintf(
					"action_%d_kind",
					actionIndex,
				),
			)

			switch actionKind {
			case 0, 1, 2:
				// BID action.
				jitter := rapid.Int64Range(
					0,
					99,
				).Draw(
					rt,
					fmt.Sprintf(
						"action_%d_bid_jitter",
						actionIndex,
					),
				)

				bid := newDay11LogicalBid(
					auctionID,
					nextBidOrdinal,
					jitter,
					logicalClock,
				)

				nextBidOrdinal++

				pendingBids = append(
					pendingBids,
					bid,
				)

			case 3:
				// CRASH action.
				//
				// A crash after PostgreSQL commit but before Redis XACK
				// causes the same logical bid to be delivered again.
				if len(committedBids) == 0 {
					continue
				}

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

				replayCopies := rapid.IntRange(
					1,
					3,
				).Draw(
					rt,
					fmt.Sprintf(
						"action_%d_replay_copies",
						actionIndex,
					),
				)

				replayWave := make(
					[]day11LogicalBid,
					replayCopies,
				)

				for index := range replayWave {
					replayWave[index] =
						committedBids[replayIndex]
				}

				applyDay11ReplayWave(
					rt,
					historyCtx,
					ledger,
					replayWave,
				)

			case 4:
				// TICK action.
				advanceMilliseconds :=
					rapid.IntRange(
						1,
						5_000,
					).Draw(
						rt,
						fmt.Sprintf(
							"action_%d_tick_milliseconds",
							actionIndex,
						),
					)

				logicalClock = logicalClock.Add(
					time.Duration(
						advanceMilliseconds,
					) * time.Millisecond,
				)

				// A tick also gives the consumer an opportunity to
				// process all bids currently waiting in the wave.
				if len(pendingBids) > 0 {
					applyDay11NewBidWave(
						rt,
						historyCtx,
						ledger,
						pendingBids,
					)

					recordDay11CommittedBids(
						expectedBids,
						&committedBids,
						pendingBids,
					)

					pendingBids = nil
				}
			}
		}

		// Drain bids remaining after the last generated action.
		if len(pendingBids) > 0 {
			applyDay11NewBidWave(
				rt,
				historyCtx,
				ledger,
				pendingBids,
			)

			recordDay11CommittedBids(
				expectedBids,
				&committedBids,
				pendingBids,
			)
		}

		assertDay11I1AndI2(
			rt,
			historyCtx,
			adminPool,
			auctionID,
			expectedBids,
		)
	})
}

func newDay11LogicalBid(
	auctionID uuid.UUID,
	ordinal int,
	jitter int64,
	submittedAt time.Time,
) day11LogicalBid {
	// Each ordinal owns a separate range of 100 values.
	//
	// Therefore, generated amounts are always unique:
	//
	// ordinal 1: 10100..10199
	// ordinal 2: 10200..10299
	// ordinal 3: 10300..10399
	amount := int64(10_000) +
		int64(ordinal*100) +
		jitter

	return day11LogicalBid{
		PersistedBid: postgresstore.BidToPersist{
			AuctionID:      auctionID,
			BidderID:       uuid.New(),
			Amount:         amount,
			IdempotencyKey: uuid.New(),
			StreamEntryID: fmt.Sprintf(
				"day11-%s-%06d",
				auctionID.String(),
				ordinal,
			),
			CorrelationID: uuid.NewString(),
		},
		SubmittedAt: submittedAt,
	}
}

func applyDay11NewBidWave(
	rt *rapid.T,
	ctx context.Context,
	ledger *postgresstore.LedgerStore,
	bids []day11LogicalBid,
) {
	outcomes := runDay11ConcurrentWave(
		ctx,
		ledger,
		bids,
	)

	for _, outcome := range outcomes {
		if outcome.Err != nil {
			rt.Fatalf(
				"new valid bid failed: "+
					"amount=%d key=%s error=%v",
				outcome.Bid.PersistedBid.Amount,
				outcome.Bid.PersistedBid.
					IdempotencyKey,
				outcome.Err,
			)
		}

		if !outcome.Result.Inserted {
			rt.Fatalf(
				"new valid bid was treated as a replay: "+
					"amount=%d key=%s sequence=%d",
				outcome.Bid.PersistedBid.Amount,
				outcome.Bid.PersistedBid.
					IdempotencyKey,
				outcome.Result.SequenceNumber,
			)
		}

		if outcome.Result.SequenceNumber <= 0 {
			rt.Fatalf(
				"new valid bid received invalid sequence %d",
				outcome.Result.SequenceNumber,
			)
		}
	}
}

func applyDay11ReplayWave(
	rt *rapid.T,
	ctx context.Context,
	ledger *postgresstore.LedgerStore,
	bids []day11LogicalBid,
) {
	outcomes := runDay11ConcurrentWave(
		ctx,
		ledger,
		bids,
	)

	for _, outcome := range outcomes {
		if outcome.Err != nil {
			rt.Fatalf(
				"crash replay failed: key=%s error=%v",
				outcome.Bid.PersistedBid.
					IdempotencyKey,
				outcome.Err,
			)
		}

		if outcome.Result.Inserted {
			rt.Fatalf(
				"crash replay created a duplicate bid: "+
					"key=%s sequence=%d",
				outcome.Bid.PersistedBid.
					IdempotencyKey,
				outcome.Result.SequenceNumber,
			)
		}

		if outcome.Result.SequenceNumber <= 0 {
			rt.Fatalf(
				"crash replay returned invalid sequence %d",
				outcome.Result.SequenceNumber,
			)
		}
	}
}

func runDay11ConcurrentWave(
	ctx context.Context,
	ledger *postgresstore.LedgerStore,
	bids []day11LogicalBid,
) []day11AppendOutcome {
	start := make(chan struct{})

	outcomeChannel := make(
		chan day11AppendOutcome,
		len(bids),
	)

	var waitGroup sync.WaitGroup
	waitGroup.Add(len(bids))

	for _, logicalBid := range bids {
		logicalBid := logicalBid

		go func() {
			defer waitGroup.Done()

			// All transactions in this wave begin together.
			<-start

			result, err :=
				ledger.
					AppendBidIdempotentWithAntiSnipe(
						ctx,
						logicalBid.PersistedBid,
						logicalBid.SubmittedAt,
					)

			outcomeChannel <- day11AppendOutcome{
				Bid:    logicalBid,
				Result: result,
				Err:    err,
			}
		}()
	}

	close(start)

	waitGroup.Wait()
	close(outcomeChannel)

	outcomes := make(
		[]day11AppendOutcome,
		0,
		len(bids),
	)

	for outcome := range outcomeChannel {
		outcomes = append(
			outcomes,
			outcome,
		)
	}

	return outcomes
}

func recordDay11CommittedBids(
	expected map[uuid.UUID]day11LogicalBid,
	committed *[]day11LogicalBid,
	bids []day11LogicalBid,
) {
	for _, bid := range bids {
		expected[bid.PersistedBid.IdempotencyKey] = bid

		*committed = append(
			*committed,
			bid,
		)
	}
}

func assertDay11I1AndI2(
	rt *rapid.T,
	ctx context.Context,
	adminPool *pgxpool.Pool,
	auctionID uuid.UUID,
	expected map[uuid.UUID]day11LogicalBid,
) {
	firstObserver, err := readDay11Ledger(
		ctx,
		adminPool,
		auctionID,
	)
	if err != nil {
		rt.Fatalf(
			"first ledger observer: %v",
			err,
		)
	}

	secondObserver, err := readDay11Ledger(
		ctx,
		adminPool,
		auctionID,
	)
	if err != nil {
		rt.Fatalf(
			"second ledger observer: %v",
			err,
		)
	}

	// I1: every observer must see exactly the same ordered ledger.
	if !reflect.DeepEqual(firstObserver, secondObserver) {
		rt.Fatalf(
			"I1 violated: observers disagree:\n"+
				"first=%+v\n"+
				"second=%+v",
			firstObserver,
			secondObserver,
		)
	}

	if len(firstObserver) != len(expected) {
		rt.Fatalf(
			"I1 violated: expected %d logical bids, "+
				"ledger contains %d",
			len(expected),
			len(firstObserver),
		)
	}

	remainingExpected := make(
		map[uuid.UUID]day11LogicalBid,
		len(expected),
	)

	for key, bid := range expected {
		remainingExpected[key] = bid
	}

	seenSequences := make(
		map[int64]struct{},
		len(firstObserver),
	)

	seenStreamEntries := make(
		map[string]struct{},
		len(firstObserver),
	)

	for index, stored := range firstObserver {
		expectedSequence := int64(index + 1)

		// I1: permanent order must be exactly 1..N.
		if stored.SequenceNumber != expectedSequence {
			rt.Fatalf(
				"I1 violated: ledger position %d "+
					"contains sequence %d; expected %d",
				index,
				stored.SequenceNumber,
				expectedSequence,
			)
		}

		if _, duplicate := seenSequences[stored.SequenceNumber]; duplicate {
			rt.Fatalf(
				"I1 violated: duplicate sequence %d",
				stored.SequenceNumber,
			)
		}

		seenSequences[stored.SequenceNumber] = struct{}{}

		if _, duplicate := seenStreamEntries[stored.StreamEntryID]; duplicate {
			rt.Fatalf(
				"I1 violated: duplicate stream entry %q",
				stored.StreamEntryID,
			)
		}

		seenStreamEntries[stored.StreamEntryID] = struct{}{}

		expectedBid, exists := remainingExpected[stored.IdempotencyKey]
		if !exists {
			rt.Fatalf(
				"I1 violated: unexpected ledger bid "+
					"with idempotency key %s",
				stored.IdempotencyKey,
			)
		}

		if stored.Amount != expectedBid.PersistedBid.Amount {
			rt.Fatalf(
				"ledger amount %d does not match "+
					"model amount %d for key %s",
				stored.Amount,
				expectedBid.PersistedBid.Amount,
				stored.IdempotencyKey,
			)
		}

		if stored.BidderID != expectedBid.PersistedBid.BidderID {
			rt.Fatalf(
				"ledger bidder %s does not match "+
					"model bidder %s for key %s",
				stored.BidderID,
				expectedBid.PersistedBid.BidderID,
				stored.IdempotencyKey,
			)
		}

		delete(
			remainingExpected,
			stored.IdempotencyKey,
		)
	}

	if len(remainingExpected) != 0 {
		rt.Fatalf(
			"I1 violated: %d generated bids are missing "+
				"from the ledger",
			len(remainingExpected),
		)
	}

	var finalCounter int64

	if err := adminPool.QueryRow(
		ctx,
		`
		SELECT last_seq
		FROM auction_sequences
		WHERE auction_id = $1
		`,
		auctionID,
	).Scan(&finalCounter); err != nil {
		rt.Fatalf(
			"read final sequence counter: %v",
			err,
		)
	}

	if finalCounter != int64(len(expected)) {
		rt.Fatalf(
			"I1 violated: sequence counter=%d, "+
				"ledger rows=%d",
			finalCounter,
			len(expected),
		)
	}

	// Day 10 outbox must contain exactly one BidAccepted event
	// for every unique committed bid.
	var (
		outboxRows              int64
		distinctOutboxSequences int64
	)

	if err := adminPool.QueryRow(
		ctx,
		`
		SELECT
			COUNT(*),
			COUNT(DISTINCT aggregate_sequence)
		FROM outbox
		WHERE aggregate_id = $1
		  AND event_type = 'BidAccepted'
		`,
		auctionID,
	).Scan(
		&outboxRows,
		&distinctOutboxSequences,
	); err != nil {
		rt.Fatalf(
			"read Day 11 outbox state: %v",
			err,
		)
	}

	if outboxRows != int64(len(expected)) {
		rt.Fatalf(
			"outbox row mismatch: expected %d, got %d",
			len(expected),
			outboxRows,
		)
	}

	if distinctOutboxSequences != int64(len(expected)) {
		rt.Fatalf(
			"outbox sequence mismatch: expected %d "+
				"distinct sequences, got %d",
			len(expected),
			distinctOutboxSequences,
		)
	}

	// Independent in-memory oracle for I2.
	var (
		expectedWinner day11LogicalBid
		winnerExists   bool
	)

	for _, bid := range expected {
		if !winnerExists ||
			bid.PersistedBid.Amount >
				expectedWinner.PersistedBid.Amount {
			expectedWinner = bid
			winnerExists = true
		}
	}

	if !winnerExists {
		rt.Fatalf(
			"I2 check has no generated valid bids",
		)
	}

	var (
		actualWinnerBidder   uuid.UUID
		actualWinnerAmount   int64
		actualWinnerSequence int64
		actualWinnerKey      uuid.UUID
	)

	if err := adminPool.QueryRow(
		ctx,
		`
		SELECT
			bidder_id,
			amount,
			sequence_no,
			idempotency_key
		FROM bids
		WHERE auction_id = $1
		ORDER BY
			amount DESC,
			sequence_no ASC
		LIMIT 1
		`,
		auctionID,
	).Scan(
		&actualWinnerBidder,
		&actualWinnerAmount,
		&actualWinnerSequence,
		&actualWinnerKey,
	); err != nil {
		rt.Fatalf(
			"query actual winner: %v",
			err,
		)
	}

	// I2: the stored winner must equal the model's maximum bid.
	if actualWinnerAmount != expectedWinner.PersistedBid.Amount {
		rt.Fatalf(
			"I2 violated: actual winner amount=%d, "+
				"model maximum=%d",
			actualWinnerAmount,
			expectedWinner.PersistedBid.Amount,
		)
	}

	if actualWinnerBidder != expectedWinner.PersistedBid.BidderID {
		rt.Fatalf(
			"I2 violated: actual winner bidder=%s, "+
				"expected bidder=%s",
			actualWinnerBidder,
			expectedWinner.PersistedBid.BidderID,
		)
	}

	if actualWinnerKey != expectedWinner.PersistedBid.IdempotencyKey {
		rt.Fatalf(
			"I2 violated: actual winner key=%s, "+
				"expected key=%s",
			actualWinnerKey,
			expectedWinner.PersistedBid.IdempotencyKey,
		)
	}

	if actualWinnerSequence < 1 ||
		actualWinnerSequence > int64(len(expected)) {
		rt.Fatalf(
			"I2 violated: winner has invalid sequence %d",
			actualWinnerSequence,
		)
	}
}
func readDay11Ledger(
	ctx context.Context,
	adminPool *pgxpool.Pool,
	auctionID uuid.UUID,
) ([]day11LedgerRow, error) {
	rows, err := adminPool.Query(
		ctx,
		`
		SELECT
			sequence_no,
			amount,
			bidder_id,
			idempotency_key,
			stream_entry_id
		FROM bids
		WHERE auction_id = $1
		ORDER BY sequence_no
		`,
		auctionID,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"query ordered ledger: %w",
			err,
		)
	}
	defer rows.Close()

	result := make(
		[]day11LedgerRow,
		0,
	)

	for rows.Next() {
		var stored day11LedgerRow

		if err := rows.Scan(
			&stored.SequenceNumber,
			&stored.Amount,
			&stored.BidderID,
			&stored.IdempotencyKey,
			&stored.StreamEntryID,
		); err != nil {
			return nil, fmt.Errorf(
				"scan ordered ledger row: %w",
				err,
			)
		}

		result = append(
			result,
			stored,
		)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate ordered ledger: %w",
			err,
		)
	}

	return result, nil
}

func resetDay11Tables(
	t *testing.T,
	ctx context.Context,
	adminPool *pgxpool.Pool,
) {
	t.Helper()

	if _, err := adminPool.Exec(
		ctx,
		`
		TRUNCATE TABLE
			outbox,
			bids,
			auction_sequences,
			auctions
		CASCADE
		`,
	); err != nil {
		t.Fatalf(
			"reset Day 11 tables: %v\n"+
				"Did you apply all migrations through Day 10?",
			err,
		)
	}
}
