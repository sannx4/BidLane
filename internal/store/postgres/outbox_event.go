package postgresstore

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const BidAcceptedEventType = "BidAccepted"

// insertBidAcceptedOutbox appends the event inside the same
// PostgreSQL transaction that inserts the immutable bid.
//
// It must be called before tx.Commit().
func insertBidAcceptedOutbox(
	ctx context.Context,
	tx pgx.Tx,
	bid BidToPersist,
	sequenceNumber int64,
	submittedAt time.Time,
	effectiveCloseTime time.Time,
	extended bool,
	extensionCount int32,
) error {
	commandTag, err := tx.Exec(
		ctx,
		`
	INSERT INTO outbox (
		event_type,
		aggregate_type,
		aggregate_id,
		aggregate_sequence,
		payload
	)
	VALUES (
		'BidAccepted',
		'auction',
		$1::uuid,
		$2::bigint,
		jsonb_build_object(
			'version', 1,
			'auction_id', $1::uuid,
			'sequence_no', $2::bigint,
			'bidder_id', $3::uuid,
			'amount', $4::bigint,
			'idempotency_key', $5::uuid,
			'stream_entry_id', $6::text,
			'correlation_id', $7::text,
			'submitted_at', $8::timestamptz,
			'effective_close_time', $9::timestamptz,
			'extended', $10::boolean,
			'extension_count', $11::integer
		)
	)
	
	`,
		bid.AuctionID,
		sequenceNumber,
		bid.BidderID,
		bid.Amount,
		bid.IdempotencyKey,
		bid.StreamEntryID,
		bid.CorrelationID,
		submittedAt,
		effectiveCloseTime,
		extended,
		extensionCount,
	)
	if err != nil {
		return fmt.Errorf(
			"insert BidAccepted outbox event: %w",
			err,
		)
	}

	// The new bid path must always create exactly one outbox row.
	//
	// A zero-row result would indicate an unexpected conflict or
	// inconsistent state.
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf(
			"insert BidAccepted outbox event: expected one row, inserted %d",
			commandTag.RowsAffected(),
		)
	}

	return nil
}
