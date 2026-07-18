package integration_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"example.com/bidlane/internal/engine"
	redisstore "example.com/bidlane/internal/store/redis"
)

func TestDay01ConcurrentBidsHaveOneStableRedisOrder(
	t *testing.T,
) {
	redisAddress := os.Getenv("REDIS_ADDR")

	if redisAddress == "" {
		redisAddress = "localhost:6379"
	}

	client := redisstore.NewClient(redisAddress, "", 0)
	defer client.Close()

	ctx := context.Background()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf(
			"Redis is unavailable at %s: %v",
			redisAddress,
			err,
		)
	}

	streams := redisstore.NewStreamStore(client)
	service := engine.NewService(streams)

	const (
		numberOfRuns = 3
		totalBids    = 100
	)

	for run := 1; run <= numberOfRuns; run++ {
		run := run

		t.Run(fmt.Sprintf("run_%d", run), func(t *testing.T) {
			auctionID := fmt.Sprintf(
				"day01-test-%d-%s",
				run,
				uuid.NewString(),
			)

			group := "cg:engine:shard:0"
			consumerName := fmt.Sprintf(
				"day01-consumer-%d",
				run,
			)

			t.Cleanup(func() {
				cleanupContext, cancel := context.WithTimeout(
					context.Background(),
					2*time.Second,
				)
				defer cancel()

				if err := streams.DeleteStream(
					cleanupContext,
					auctionID,
				); err != nil {
					t.Logf(
						"could not clean stream %s: %v",
						auctionID,
						err,
					)
				}
			})

			if err := streams.EnsureConsumerGroup(
				ctx,
				auctionID,
				group,
			); err != nil {
				t.Fatalf(
					"create consumer group: %v",
					err,
				)
			}

			fireConcurrentBids(
				t,
				ctx,
				service,
				auctionID,
				totalBids,
			)

			readContext, cancel := context.WithTimeout(
				ctx,
				10*time.Second,
			)
			defer cancel()

			consumerEntries, err := readExactly(
				readContext,
				streams,
				auctionID,
				group,
				consumerName,
				totalBids,
			)

			if err != nil {
				t.Fatalf(
					"read consumer entries: %v",
					err,
				)
			}

			if len(consumerEntries) != totalBids {
				t.Fatalf(
					"expected %d entries, received %d",
					totalBids,
					len(consumerEntries),
				)
			}

			assertUniqueEntries(
				t,
				consumerEntries,
				totalBids,
			)

			assertStrictRedisOrder(
				t,
				consumerEntries,
			)

			// Observer 1 reads the completed stream.
			firstRange, err := streams.Range(ctx, auctionID)
			if err != nil {
				t.Fatalf(
					"first XRANGE failed: %v",
					err,
				)
			}

			// Observer 2 independently reads the same completed stream.
			secondRange, err := streams.Range(ctx, auctionID)
			if err != nil {
				t.Fatalf(
					"second XRANGE failed: %v",
					err,
				)
			}

			consumerOrder := entryIDs(consumerEntries)
			firstObserverOrder := entryIDs(firstRange)
			secondObserverOrder := entryIDs(secondRange)

			if !reflect.DeepEqual(
				consumerOrder,
				firstObserverOrder,
			) {
				t.Fatalf(
					"consumer order differs from XRANGE order\nconsumer: %v\nXRANGE: %v",
					consumerOrder,
					firstObserverOrder,
				)
			}

			if !reflect.DeepEqual(
				firstObserverOrder,
				secondObserverOrder,
			) {
				t.Fatalf(
					"two observers saw different stream orders\nfirst: %v\nsecond: %v",
					firstObserverOrder,
					secondObserverOrder,
				)
			}

			t.Logf(
				"run %d passed: %d entries, one unique and stable Redis order",
				run,
				totalBids,
			)
		})
	}
}

func fireConcurrentBids(
	t *testing.T,
	ctx context.Context,
	service *engine.Service,
	auctionID string,
	total int,
) {
	t.Helper()

	start := make(chan struct{})
	errorChannel := make(chan error, total)

	var waitGroup sync.WaitGroup
	waitGroup.Add(total)

	for index := 0; index < total; index++ {
		index := index

		go func() {
			defer waitGroup.Done()

			// All goroutines wait here so that they begin together.
			<-start

			bidderID := fmt.Sprintf(
				"bidder-%03d",
				index+1,
			)

			amount := int64(10_000 + index)

			_, err := service.ReserveBid(
				ctx,
				auctionID,
				bidderID,
				amount,
			)

			if err != nil {
				errorChannel <- fmt.Errorf(
					"%s failed: %w",
					bidderID,
					err,
				)
			}
		}()
	}

	// Release all 100 goroutines at approximately the same time.
	close(start)

	waitGroup.Wait()
	close(errorChannel)

	for err := range errorChannel {
		t.Error(err)
	}

	if t.Failed() {
		t.FailNow()
	}
}

func readExactly(
	ctx context.Context,
	streams *redisstore.StreamStore,
	auctionID string,
	group string,
	consumerName string,
	expected int,
) ([]redisstore.BidStreamEntry, error) {
	collected := make(
		[]redisstore.BidStreamEntry,
		0,
		expected,
	)

	for len(collected) < expected {
		if ctx.Err() != nil {
			return nil, fmt.Errorf(
				"timed out after receiving %d of %d entries: %w",
				len(collected),
				expected,
				ctx.Err(),
			)
		}

		remaining := expected - len(collected)

		entries, err := streams.ReadGroup(
			ctx,
			auctionID,
			group,
			consumerName,
			int64(remaining),
			500*time.Millisecond,
		)

		if err != nil {
			return nil, err
		}

		if len(entries) == 0 {
			continue
		}

		entryIDsToAck := make(
			[]string,
			0,
			len(entries),
		)

		for _, entry := range entries {
			collected = append(collected, entry)
			entryIDsToAck = append(
				entryIDsToAck,
				entry.ID,
			)
		}

		if err := streams.Ack(
			ctx,
			auctionID,
			group,
			entryIDsToAck...,
		); err != nil {
			return nil, err
		}
	}

	return collected, nil
}

func assertUniqueEntries(
	t *testing.T,
	entries []redisstore.BidStreamEntry,
	expected int,
) {
	t.Helper()

	streamIDs := make(map[string]struct{}, expected)
	bidders := make(map[string]struct{}, expected)

	for _, entry := range entries {
		if _, exists := streamIDs[entry.ID]; exists {
			t.Fatalf(
				"duplicate Redis Stream ID found: %s",
				entry.ID,
			)
		}

		streamIDs[entry.ID] = struct{}{}

		if _, exists := bidders[entry.BidderID]; exists {
			t.Fatalf(
				"bidder appeared more than once: %s",
				entry.BidderID,
			)
		}

		bidders[entry.BidderID] = struct{}{}
	}

	if len(streamIDs) != expected {
		t.Fatalf(
			"expected %d unique stream IDs, got %d",
			expected,
			len(streamIDs),
		)
	}

	if len(bidders) != expected {
		t.Fatalf(
			"expected %d unique bidders, got %d",
			expected,
			len(bidders),
		)
	}
}

func assertStrictRedisOrder(
	t *testing.T,
	entries []redisstore.BidStreamEntry,
) {
	t.Helper()

	for index := 1; index < len(entries); index++ {
		previous := entries[index-1].ID
		current := entries[index].ID

		isGreater, err := redisStreamIDGreater(
			current,
			previous,
		)

		if err != nil {
			t.Fatalf(
				"compare Redis IDs %q and %q: %v",
				previous,
				current,
				err,
			)
		}

		if !isGreater {
			t.Fatalf(
				"stream order is not strictly increasing at index %d: %s then %s",
				index,
				previous,
				current,
			)
		}
	}
}

func redisStreamIDGreater(
	current string,
	previous string,
) (bool, error) {
	currentMilliseconds, currentSequence, err :=
		parseRedisStreamID(current)

	if err != nil {
		return false, err
	}

	previousMilliseconds, previousSequence, err :=
		parseRedisStreamID(previous)

	if err != nil {
		return false, err
	}

	if currentMilliseconds > previousMilliseconds {
		return true, nil
	}

	return currentMilliseconds == previousMilliseconds &&
		currentSequence > previousSequence, nil
}

func parseRedisStreamID(
	streamID string,
) (int64, int64, error) {
	parts := strings.SplitN(streamID, "-", 2)

	if len(parts) != 2 {
		return 0, 0, fmt.Errorf(
			"invalid Redis Stream ID: %q",
			streamID,
		)
	}

	milliseconds, err := strconv.ParseInt(
		parts[0],
		10,
		64,
	)

	if err != nil {
		return 0, 0, fmt.Errorf(
			"invalid timestamp portion in %q: %w",
			streamID,
			err,
		)
	}

	sequence, err := strconv.ParseInt(
		parts[1],
		10,
		64,
	)

	if err != nil {
		return 0, 0, fmt.Errorf(
			"invalid sequence portion in %q: %w",
			streamID,
			err,
		)
	}

	return milliseconds, sequence, nil
}

func entryIDs(
	entries []redisstore.BidStreamEntry,
) []string {
	ids := make([]string, 0, len(entries))

	for _, entry := range entries {
		ids = append(ids, entry.ID)
	}

	return ids
}
