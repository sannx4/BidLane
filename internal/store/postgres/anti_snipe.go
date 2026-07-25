package postgresstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

var ErrBidAtOrAfterClose = errors.New(
	"bid submitted at or after effective close time",
)

// AntiSnipeAppendResult reports both the bid result and
// whether this accepted bid extended the auction.
type AntiSnipeAppendResult struct {
	AppendBidResult

	Extended           bool
	EffectiveCloseTime time.Time
	ExtensionCount     int32
}

// AppendBidIdempotentWithAntiSnipe performs all correctness-bearing
// changes in one PostgreSQL transaction:
//
//  1. Lock the auction.
//  2. Validate the Redis ingress timestamp.
//  3. Assign a gap-free sequence number.
//  4. Insert the immutable bid.
//  5. Advance the sequence counter.
//  6. Extend the auction when eligible.
//  7. Commit everything together.
//
// A duplicate idempotency key does not consume a sequence number
// and does not extend the auction again.
func (s *LedgerStore) AppendBidIdempotentWithAntiSnipe(
	ctx context.Context,
	bid BidToPersist,
	submittedAt time.Time,
) (AntiSnipeAppendResult, error) {
	if submittedAt.IsZero() {
		return AntiSnipeAppendResult{},
			errors.New(
				"authoritative bid submission time is required",
			)
	}

	tx, err := s.pool.BeginTx(
		ctx,
		pgx.TxOptions{
			IsoLevel: pgx.ReadCommitted,
		},
	)
	if err != nil {
		return AntiSnipeAppendResult{},
			fmt.Errorf(
				"begin anti-snipe transaction: %w",
				err,
			)
	}

	defer func() {
		// Harmless after a successful commit.
		_ = tx.Rollback(context.Background())
	}()

	// Lock the auction first.
	//
	// This prevents two concurrent bid transactions from reading
	// and modifying the close time independently.
	var (
		currentCloseTime time.Time
		extensionCount   int32
	)

	if err := tx.QueryRow(
		ctx,
		`
			SELECT
				effective_close_time,
				extension_count
			FROM auctions
			WHERE id = $1
			FOR UPDATE
		`,
		bid.AuctionID,
	).Scan(
		&currentCloseTime,
		&extensionCount,
	); err != nil {
		return AntiSnipeAppendResult{},
			fmt.Errorf(
				"lock auction for anti-snipe processing: %w",
				err,
			)
	}

	var existingSequence int64

	err = tx.QueryRow(
		ctx,
		`
		SELECT sequence_no
		FROM bids
		WHERE auction_id = $1
		  AND idempotency_key = $2
	`,
		bid.AuctionID,
		bid.IdempotencyKey,
	).Scan(&existingSequence)

	switch {
	case err == nil:
		if err := tx.Commit(ctx); err != nil {
			return AntiSnipeAppendResult{},
				fmt.Errorf(
					"commit crash-recovery duplicate observation: %w",
					err,
				)
		}

		return AntiSnipeAppendResult{
			AppendBidResult: AppendBidResult{
				SequenceNumber: existingSequence,
				Inserted:       false,
			},
			Extended:           false,
			EffectiveCloseTime: currentCloseTime,
			ExtensionCount:     extensionCount,
		}, nil

	case errors.Is(err, pgx.ErrNoRows):
		// This logical bid does not already exist.
		// Continue with normal late-bid validation and insertion.

	default:
		return AntiSnipeAppendResult{},
			fmt.Errorf(
				"check existing bid before crash recovery: %w",
				err,
			)
	}

	// I3 is checked again inside the authoritative transaction.
	//
	// submitted_at must be strictly before close time.
	if !submittedAt.Before(currentCloseTime) {
		return AntiSnipeAppendResult{},
			ErrBidAtOrAfterClose
	}

	// Ensure this auction has a sequence counter.
	if _, err := tx.Exec(
		ctx,
		`
			INSERT INTO auction_sequences (
				auction_id,
				last_seq
			)
			VALUES ($1, 0)
			ON CONFLICT (auction_id)
			DO NOTHING
		`,
		bid.AuctionID,
	); err != nil {
		return AntiSnipeAppendResult{},
			fmt.Errorf(
				"ensure auction sequence row: %w",
				err,
			)
	}

	// Lock the sequence counter after locking the auction.
	//
	// The lock order is always:
	//
	// auction row -> sequence row
	var lastSequence int64

	if err := tx.QueryRow(
		ctx,
		`
			SELECT last_seq
			FROM auction_sequences
			WHERE auction_id = $1
			FOR UPDATE
		`,
		bid.AuctionID,
	).Scan(&lastSequence); err != nil {
		return AntiSnipeAppendResult{},
			fmt.Errorf(
				"lock auction sequence row: %w",
				err,
			)
	}

	nextSequence := lastSequence + 1

	// Insert the bid only if this logical operation has not
	// already been persisted.
	var insertedSequence int64

	err = tx.QueryRow(
		ctx,
		`
			INSERT INTO bids (
				auction_id,
				sequence_no,
				amount,
				bidder_id,
				idempotency_key,
				stream_entry_id
			)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (
				auction_id,
				idempotency_key
			)
			DO NOTHING
			RETURNING sequence_no
		`,
		bid.AuctionID,
		nextSequence,
		bid.Amount,
		bid.BidderID,
		bid.IdempotencyKey,
		bid.StreamEntryID,
	).Scan(&insertedSequence)

	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// This was a duplicate delivery.
		//
		// Do not advance the sequence counter.
		// Do not extend the auction again.
		var existingSequence int64

		if err := tx.QueryRow(
			ctx,
			`
				SELECT sequence_no
				FROM bids
				WHERE auction_id = $1
				  AND idempotency_key = $2
			`,
			bid.AuctionID,
			bid.IdempotencyKey,
		).Scan(&existingSequence); err != nil {
			return AntiSnipeAppendResult{},
				fmt.Errorf(
					"read existing idempotent bid: %w",
					err,
				)
		}

		if err := tx.Commit(ctx); err != nil {
			return AntiSnipeAppendResult{},
				fmt.Errorf(
					"commit duplicate bid observation: %w",
					err,
				)
		}

		return AntiSnipeAppendResult{
			AppendBidResult: AppendBidResult{
				SequenceNumber: existingSequence,
				Inserted:       false,
			},
			Extended:           false,
			EffectiveCloseTime: currentCloseTime,
			ExtensionCount:     extensionCount,
		}, nil

	case err != nil:
		return AntiSnipeAppendResult{},
			fmt.Errorf(
				"insert anti-snipe bid: %w",
				err,
			)
	}

	// Advance the sequence counter only after a new bid row
	// has been inserted.
	commandTag, err := tx.Exec(
		ctx,
		`
			UPDATE auction_sequences
			SET last_seq = $2
			WHERE auction_id = $1
			  AND last_seq = $3
		`,
		bid.AuctionID,
		nextSequence,
		lastSequence,
	)
	if err != nil {
		return AntiSnipeAppendResult{},
			fmt.Errorf(
				"advance auction sequence: %w",
				err,
			)
	}

	if commandTag.RowsAffected() != 1 {
		return AntiSnipeAppendResult{},
			fmt.Errorf(
				"advance auction sequence: expected one row, updated %d",
				commandTag.RowsAffected(),
			)
	}

	// Extend only when:
	//
	// submitted_at < effective_close_time
	// submitted_at >= effective_close_time - extension_window
	// extension_count < max_extensions
	//
	// This UPDATE is inside the same transaction as the bid insert.
	var (
		newCloseTime      time.Time
		newExtensionCount int32
		extended          bool
	)

	err = tx.QueryRow(
		ctx,
		`
			UPDATE auctions
			SET
				effective_close_time =
					effective_close_time +
					extension_interval,

				extension_count =
					extension_count + 1
			WHERE id = $1
			  AND $2 < effective_close_time
			  AND $2 >=
			      effective_close_time -
			      extension_window
			  AND extension_count < max_extensions
			RETURNING
				effective_close_time,
				extension_count
		`,
		bid.AuctionID,
		submittedAt,
	).Scan(
		&newCloseTime,
		&newExtensionCount,
	)

	switch {
	case err == nil:
		extended = true

	case errors.Is(err, pgx.ErrNoRows):
		// The bid was accepted but it was outside the extension
		// window, or the maximum extension count was reached.
		extended = false
		newCloseTime = currentCloseTime
		newExtensionCount = extensionCount

	default:
		return AntiSnipeAppendResult{},
			fmt.Errorf(
				"apply atomic anti-snipe extension: %w",
				err,
			)
	}

	// The bid, sequence counter and extension become visible
	// together only after this commit succeeds.
	if err := tx.Commit(ctx); err != nil {
		return AntiSnipeAppendResult{},
			fmt.Errorf(
				"commit anti-snipe transaction: %w",
				err,
			)
	}

	return AntiSnipeAppendResult{
		AppendBidResult: AppendBidResult{
			SequenceNumber: insertedSequence,
			Inserted:       true,
		},
		Extended:           extended,
		EffectiveCloseTime: newCloseTime,
		ExtensionCount:     newExtensionCount,
	}, nil
}
