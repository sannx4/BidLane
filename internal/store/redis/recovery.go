package redisstore

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// AutoClaimPending transfers pending entries from an old or crashed
// consumer to the currently running consumer.
//
// start behaves like a scan cursor:
//   - first call: "0-0"
//   - Redis returns nextStart
//   - nextStart == "0-0" means the scan is complete
func (s *StreamStore) AutoClaimPending(
	ctx context.Context,
	auctionID string,
	group string,
	consumer string,
	minIdle time.Duration,
	start string,
	count int64,
) ([]BidStreamEntry, string, error) {
	if auctionID == "" {
		return nil, "", fmt.Errorf(
			"auction ID is required for XAUTOCLAIM",
		)
	}

	if group == "" {
		return nil, "", fmt.Errorf(
			"consumer group is required for XAUTOCLAIM",
		)
	}

	if consumer == "" {
		return nil, "", fmt.Errorf(
			"consumer name is required for XAUTOCLAIM",
		)
	}

	if minIdle < 0 {
		return nil, "", fmt.Errorf(
			"minimum idle time cannot be negative",
		)
	}

	if count <= 0 {
		return nil, "", fmt.Errorf(
			"XAUTOCLAIM count must be positive",
		)
	}

	if start == "" {
		start = "0-0"
	}

	messages, nextStart, err :=
		s.client.XAutoClaim(
			ctx,
			&redis.XAutoClaimArgs{
				Stream: StreamKey(
					auctionID,
				),
				Group:    group,
				Consumer: consumer,
				MinIdle:  minIdle,
				Start:    start,
				Count:    count,
			},
		).Result()
	if err != nil {
		return nil, "", fmt.Errorf(
			"XAUTOCLAIM stream %q group %q: %w",
			StreamKey(auctionID),
			group,
			err,
		)
	}

	entries := make(
		[]BidStreamEntry,
		0,
		len(messages),
	)

	for _, message := range messages {
		entry, err := parseMessage(
			message,
		)
		if err != nil {
			return nil, "", fmt.Errorf(
				"parse claimed Redis entry %s: %w",
				message.ID,
				err,
			)
		}

		entries = append(
			entries,
			entry,
		)
	}

	return entries, nextStart, nil
}
