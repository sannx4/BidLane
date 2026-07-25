package postgresstore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type BidToPersist struct {
	AuctionID      uuid.UUID
	BidderID       uuid.UUID
	Amount         int64
	IdempotencyKey uuid.UUID
	StreamEntryID  string
}

// AppendBidResult tells the consumer whether a new row was
// inserted or an existing logical bid was found.
type AppendBidResult struct {
	SequenceNumber int64
	Inserted       bool
}

type LedgerStore struct {
	pool *pgxpool.Pool
}

func NewLedgerStore(
	pool *pgxpool.Pool,
) *LedgerStore {
	return &LedgerStore{
		pool: pool,
	}
}

// AppendBidRowLock preserves the Day 3 API.
//
// For a duplicate logical bid, it returns the original sequence
// number rather than creating a new ledger row.
func (s *LedgerStore) AppendBidRowLock(
	ctx context.Context,
	bid BidToPersist,
) (int64, error) {
	result, err := s.AppendBidIdempotentRowLock(
		ctx,
		bid,
	)
	if err != nil {
		return 0, err
	}

	return result.SequenceNumber, nil
}

// AppendBidIdempotentRowLock performs sequence assignment and
// idempotent insertion inside one PostgreSQL transaction.
//
// A duplicate does not advance auction_sequences.last_seq.
func (s *LedgerStore) AppendBidIdempotentRowLock(
	ctx context.Context,
	bid BidToPersist,
) (AppendBidResult, error) {
	tx, err := s.pool.BeginTx(
		ctx,
		pgx.TxOptions{
			IsoLevel: pgx.ReadCommitted,
		},
	)
	if err != nil {
		return AppendBidResult{}, fmt.Errorf(
			"begin idempotent ledger transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	// Create the counter row for the auction if this is its
	// first bid.
	if _, err := tx.Exec(
		ctx,
		`
			INSERT INTO auction_sequences (
				auction_id,
				last_seq
			)
			VALUES ($1, 0)
			ON CONFLICT (auction_id) DO NOTHING
		`,
		bid.AuctionID,
	); err != nil {
		return AppendBidResult{}, fmt.Errorf(
			"ensure auction sequence row: %w",
			err,
		)
	}

	// Serialize all sequence assignments for this auction.
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
		return AppendBidResult{}, fmt.Errorf(
			"lock auction sequence row: %w",
			err,
		)
	}

	nextSequence := lastSequence + 1

	// Try to append the logical bid.
	//
	// If the idempotency key already exists for this auction,
	// PostgreSQL returns no row because of DO NOTHING.
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
	case err == nil:
		// A new bid row was inserted.
		//
		// Advance the counter only after the insert succeeds.
		commandTag, updateErr := tx.Exec(
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
		if updateErr != nil {
			return AppendBidResult{}, fmt.Errorf(
				"advance auction sequence: %w",
				updateErr,
			)
		}

		if commandTag.RowsAffected() != 1 {
			return AppendBidResult{}, fmt.Errorf(
				"advance auction sequence: expected one row, updated %d",
				commandTag.RowsAffected(),
			)
		}

		if err := tx.Commit(ctx); err != nil {
			return AppendBidResult{}, fmt.Errorf(
				"commit new bid: %w",
				err,
			)
		}

		return AppendBidResult{
			SequenceNumber: insertedSequence,
			Inserted:       true,
		}, nil

	case errors.Is(err, pgx.ErrNoRows):
		// The unique idempotency index found the same logical
		// bid already persisted.
		//
		// Do not advance the sequence counter.
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
			return AppendBidResult{}, fmt.Errorf(
				"read existing idempotent bid: %w",
				err,
			)
		}

		if err := tx.Commit(ctx); err != nil {
			return AppendBidResult{}, fmt.Errorf(
				"commit duplicate bid observation: %w",
				err,
			)
		}

		return AppendBidResult{
			SequenceNumber: existingSequence,
			Inserted:       false,
		}, nil

	default:
		return AppendBidResult{}, fmt.Errorf(
			"insert idempotent bid: %w",
			err,
		)
	}
}

// EnsurePostgresSequence preserves the Day 3 benchmark strategy.
func (s *LedgerStore) EnsurePostgresSequence(
	ctx context.Context,
	auctionID uuid.UUID,
) error {
	sequenceName := perAuctionSequenceName(
		auctionID,
	)

	if _, err := s.pool.Exec(
		ctx,
		fmt.Sprintf(
			`
				CREATE SEQUENCE IF NOT EXISTS public.%s
				AS BIGINT
				START WITH 1
				INCREMENT BY 1
				MINVALUE 1
				CACHE 1
			`,
			sequenceName,
		),
	); err != nil {
		return fmt.Errorf(
			"create per-auction PostgreSQL sequence: %w",
			err,
		)
	}

	if _, err := s.pool.Exec(
		ctx,
		fmt.Sprintf(
			`
				GRANT USAGE, SELECT
				ON SEQUENCE public.%s
				TO bidlane_engine
			`,
			sequenceName,
		),
	); err != nil {
		return fmt.Errorf(
			"grant per-auction sequence permission: %w",
			err,
		)
	}

	return nil
}

// AppendBidPostgresSequence is retained for the Day 3 benchmark.
// It is not the production idempotent path.
func (s *LedgerStore) AppendBidPostgresSequence(
	ctx context.Context,
	bid BidToPersist,
) (int64, error) {
	tx, err := s.pool.BeginTx(
		ctx,
		pgx.TxOptions{},
	)
	if err != nil {
		return 0, fmt.Errorf(
			"begin PostgreSQL-sequence transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	qualifiedName := "public." +
		perAuctionSequenceName(bid.AuctionID)

	var nextSequence int64

	if err := tx.QueryRow(
		ctx,
		"SELECT nextval($1::regclass)",
		qualifiedName,
	).Scan(&nextSequence); err != nil {
		return 0, fmt.Errorf(
			"obtain native PostgreSQL sequence value: %w",
			err,
		)
	}

	if err := insertBid(
		ctx,
		tx,
		bid,
		nextSequence,
	); err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf(
			"commit PostgreSQL-sequence transaction: %w",
			err,
		)
	}

	return nextSequence, nil
}

func (s *LedgerStore) DropPostgresSequence(
	ctx context.Context,
	auctionID uuid.UUID,
) error {
	sequenceName := perAuctionSequenceName(
		auctionID,
	)

	if _, err := s.pool.Exec(
		ctx,
		fmt.Sprintf(
			"DROP SEQUENCE IF EXISTS public.%s",
			sequenceName,
		),
	); err != nil {
		return fmt.Errorf(
			"drop per-auction PostgreSQL sequence: %w",
			err,
		)
	}

	return nil
}

func insertBid(
	ctx context.Context,
	tx pgx.Tx,
	bid BidToPersist,
	sequenceNumber int64,
) error {
	if _, err := tx.Exec(
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
		`,
		bid.AuctionID,
		sequenceNumber,
		bid.Amount,
		bid.BidderID,
		bid.IdempotencyKey,
		bid.StreamEntryID,
	); err != nil {
		return fmt.Errorf(
			"insert immutable bid ledger row: %w",
			err,
		)
	}

	return nil
}

func perAuctionSequenceName(
	auctionID uuid.UUID,
) string {
	return "bid_seq_" + strings.ReplaceAll(
		auctionID.String(),
		"-",
		"_",
	)
}
