package postgresstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// OutboxEvent is one committed event waiting to be published.
type OutboxEvent struct {
	ID                uuid.UUID
	EventType         string
	AggregateType     string
	AggregateID       uuid.UUID
	AggregateSequence int64
	Payload           json.RawMessage
	CreatedAt         time.Time
	PublishAttempts   int32
}

type OutboxStore struct {
	pool *pgxpool.Pool
}

func NewOutboxStore(
	pool *pgxpool.Pool,
) *OutboxStore {
	return &OutboxStore{
		pool: pool,
	}
}

// PublishNext locks one unpublished event, publishes it and then
// marks it as published.
//
// The PostgreSQL row lock is deliberately held across publication:
//
// 1. SELECT ... FOR UPDATE SKIP LOCKED
// 2. Publish to Redis
// 3. Mark published
// 4. COMMIT
//
// If the relay crashes after step 2 but before step 3, PostgreSQL
// rolls the transaction back. The event stays unpublished and is
// published again after restart.
//
// Therefore publication is at-least-once, not exactly-once.
func (s *OutboxStore) PublishNext(
	ctx context.Context,
	publish func(
		context.Context,
		OutboxEvent,
	) error,
	afterPublishBeforeMark func(
		OutboxEvent,
	) error,
) (bool, error) {
	if publish == nil {
		return false, errors.New(
			"outbox publish function is required",
		)
	}

	tx, err := s.pool.BeginTx(
		ctx,
		pgx.TxOptions{
			IsoLevel: pgx.ReadCommitted,
		},
	)
	if err != nil {
		return false, fmt.Errorf(
			"begin outbox relay transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	var (
		event   OutboxEvent
		payload []byte
	)

	err = tx.QueryRow(
		ctx,
		`
		SELECT
			id,
			event_type,
			aggregate_type,
			aggregate_id,
			aggregate_sequence,
			payload,
			created_at,
			publish_attempts
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY
			created_at,
			id
		FOR UPDATE SKIP LOCKED
		LIMIT 1
		`,
	).Scan(
		&event.ID,
		&event.EventType,
		&event.AggregateType,
		&event.AggregateID,
		&event.AggregateSequence,
		&payload,
		&event.CreatedAt,
		&event.PublishAttempts,
	)

	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil

	case err != nil:
		return false, fmt.Errorf(
			"lock next unpublished outbox event: %w",
			err,
		)
	}

	event.Payload = append(
		json.RawMessage(nil),
		payload...,
	)

	// External operation: Redis PUBLISH.
	if err := publish(ctx, event); err != nil {
		return false, fmt.Errorf(
			"publish outbox event %s: %w",
			event.ID,
			err,
		)
	}

	// Day 10 crash-injection boundary:
	//
	// Redis has received the publication.
	// PostgreSQL has not yet marked the row published.
	if afterPublishBeforeMark != nil {
		if err := afterPublishBeforeMark(event); err != nil {
			return false, fmt.Errorf(
				"after publishing outbox event %s: %w",
				event.ID,
				err,
			)
		}
	}

	commandTag, err := tx.Exec(
		ctx,
		`
		UPDATE outbox
		SET
			published_at = clock_timestamp(),
			publish_attempts = publish_attempts + 1
		WHERE id = $1
		  AND published_at IS NULL
		`,
		event.ID,
	)
	if err != nil {
		return false, fmt.Errorf(
			"mark outbox event %s published: %w",
			event.ID,
			err,
		)
	}

	if commandTag.RowsAffected() != 1 {
		return false, fmt.Errorf(
			"mark outbox event %s published: expected one row, updated %d",
			event.ID,
			commandTag.RowsAffected(),
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf(
			"commit outbox publication %s: %w",
			event.ID,
			err,
		)
	}

	return true, nil
}
