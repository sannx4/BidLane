package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	postgresstore "example.com/bidlane/internal/store/postgres"
)

const DefaultBidEventsChannel = "bidlane:events:bids"

type Publisher interface {
	Publish(
		ctx context.Context,
		channel string,
		message []byte,
	) error
}

// PublishedEvent is the stable envelope sent to downstream
// consumers.
//
// Downstream services must deduplicate using EventID.
type PublishedEvent struct {
	EventID           uuid.UUID       `json:"event_id"`
	EventType         string          `json:"event_type"`
	AggregateType     string          `json:"aggregate_type"`
	AggregateID       uuid.UUID       `json:"aggregate_id"`
	AggregateSequence int64           `json:"aggregate_sequence"`
	Payload           json.RawMessage `json:"payload"`
	CreatedAt         time.Time       `json:"created_at"`
}

type Config struct {
	Channel      string
	BatchSize    int
	PollInterval time.Duration

	// Day 10 failure-injection hook.
	//
	// Production configuration leaves this nil.
	AfterPublishBeforeMark func(
		postgresstore.OutboxEvent,
	) error
}

type OutboxRelay struct {
	outbox    *postgresstore.OutboxStore
	publisher Publisher
	config    Config
}

func NewOutboxRelay(
	outbox *postgresstore.OutboxStore,
	publisher Publisher,
	config Config,
) (*OutboxRelay, error) {
	if outbox == nil {
		return nil, errors.New(
			"outbox store is required",
		)
	}

	if publisher == nil {
		return nil, errors.New(
			"outbox publisher is required",
		)
	}

	if config.Channel == "" {
		config.Channel = DefaultBidEventsChannel
	}

	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}

	if config.PollInterval <= 0 {
		config.PollInterval = 200 * time.Millisecond
	}

	return &OutboxRelay{
		outbox:    outbox,
		publisher: publisher,
		config:    config,
	}, nil
}

// ProcessOne publishes at most one event.
func (r *OutboxRelay) ProcessOne(
	ctx context.Context,
) (bool, error) {
	return r.outbox.PublishNext(
		ctx,
		func(
			ctx context.Context,
			event postgresstore.OutboxEvent,
		) error {
			envelope := PublishedEvent{
				EventID:           event.ID,
				EventType:         event.EventType,
				AggregateType:     event.AggregateType,
				AggregateID:       event.AggregateID,
				AggregateSequence: event.AggregateSequence,
				Payload:           event.Payload,
				CreatedAt:         event.CreatedAt,
			}

			message, err := json.Marshal(envelope)
			if err != nil {
				return fmt.Errorf(
					"encode outbox event %s: %w",
					event.ID,
					err,
				)
			}

			if err := r.publisher.Publish(
				ctx,
				r.config.Channel,
				message,
			); err != nil {
				return err
			}

			return nil
		},
		r.config.AfterPublishBeforeMark,
	)
}

// Run continuously polls PostgreSQL and publishes pending events.
func (r *OutboxRelay) Run(
	ctx context.Context,
) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		processed := 0

		for processed < r.config.BatchSize {
			published, err := r.ProcessOne(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}

				return err
			}

			if !published {
				break
			}

			processed++
		}

		// Continue immediately while work exists.
		if processed > 0 {
			continue
		}

		timer := time.NewTimer(
			r.config.PollInterval,
		)

		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}

			return nil

		case <-timer.C:
		}
	}
}
