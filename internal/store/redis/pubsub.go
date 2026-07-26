package redisstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type PubSubPublisher struct {
	client *redis.Client
}

func NewPubSubPublisher(
	client *redis.Client,
) *PubSubPublisher {
	return &PubSubPublisher{
		client: client,
	}
}

func (p *PubSubPublisher) Publish(
	ctx context.Context,
	channel string,
	message []byte,
) error {
	if p == nil || p.client == nil {
		return errors.New(
			"Redis Pub/Sub publisher is not configured",
		)
	}

	if channel == "" {
		return errors.New(
			"Redis Pub/Sub channel is required",
		)
	}

	if len(message) == 0 {
		return errors.New(
			"Redis Pub/Sub message is empty",
		)
	}

	if err := p.client.Publish(
		ctx,
		channel,
		message,
	).Err(); err != nil {
		return fmt.Errorf(
			"publish Redis message to %q: %w",
			channel,
			err,
		)
	}

	return nil
}
