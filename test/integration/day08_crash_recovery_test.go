package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"example.com/bidlane/internal/engine"
	postgresstore "example.com/bidlane/internal/store/postgres"
	redisstore "example.com/bidlane/internal/store/redis"
)

const (
	day08TotalBids = 40
	day08CrashRuns = 20
)

// TestDay08CrashRecoveryAcrossRandomKillPoints is the parent test.
//
// It launches a separate copy of this test executable because calling
// os.Exit inside the parent would terminate the entire Go test suite.
func TestDay08CrashRecoveryAcrossRandomKillPoints(
	t *testing.T,
) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Minute,
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

	redisAddress := os.Getenv(
		"REDIS_ADDR",
	)
	if redisAddress == "" {
		redisAddress = "localhost:6379"
	}

	adminPool, err := postgresstore.ConnectPool(
		ctx,
		postgresDSN,
		"",
	)
	if err != nil {
		t.Fatalf(
			"connect administrator pool: %v",
			err,
		)
	}
	defer adminPool.Close()

	enginePool, err := postgresstore.ConnectPool(
		ctx,
		postgresDSN,
		"bidlane_engine",
	)
	if err != nil {
		t.Fatalf(
			"connect Engine pool: %v",
			err,
		)
	}
	defer enginePool.Close()

	redisClient := redisstore.NewClient(
		redisAddress,
		"",
		0,
	)
	defer redisClient.Close()

	streams := redisstore.NewStreamStore(
		redisClient,
	)

	if err := streams.Ping(ctx); err != nil {
		t.Fatalf(
			"Redis unavailable at %s: %v",
			redisAddress,
			err,
		)
	}

	// Fixed seed gives randomized but reproducible crash points.
	const randomSeed int64 = 8_008

	randomSource := rand.New(
		rand.NewSource(randomSeed),
	)

	permutation :=
		randomSource.Perm(day08TotalBids)

	killPoints :=
		permutation[:day08CrashRuns]

	t.Logf(
		"Day 8 randomized seed=%d kill points=%v",
		randomSeed,
		killPoints,
	)

	for runIndex, zeroBasedKillPoint := range killPoints {
		killAfter := zeroBasedKillPoint + 1

		t.Run(
			fmt.Sprintf(
				"run_%02d_kill_after_%02d",
				runIndex+1,
				killAfter,
			),
			func(t *testing.T) {
				if _, err := adminPool.Exec(
					ctx,
					`
						TRUNCATE TABLE
							bids,
							auction_sequences,
							auctions
						CASCADE
					`,
				); err != nil {
					t.Fatalf(
						"reset Day 8 tables: %v",
						err,
					)
				}

				auctionID := uuid.New()
				auctionIDText :=
					auctionID.String()

				effectiveCloseTime :=
					time.Now().
						UTC().
						Add(24 * time.Hour)

				if _, err := adminPool.Exec(
					ctx,
					`
						INSERT INTO auctions (
							id,
							effective_close_time
						)
						VALUES ($1, $2)
					`,
					auctionID,
					effectiveCloseTime,
				); err != nil {
					t.Fatalf(
						"create Day 8 auction: %v",
						err,
					)
				}

				if err := streams.DeleteStream(
					ctx,
					auctionIDText,
				); err != nil {
					t.Fatalf(
						"delete old Day 8 stream: %v",
						err,
					)
				}

				t.Cleanup(func() {
					cleanupCtx,
						cleanupCancel :=
						context.WithTimeout(
							context.Background(),
							10*time.Second,
						)
					defer cleanupCancel()

					_ = streams.DeleteStream(
						cleanupCtx,
						auctionIDText,
					)
				})

				service :=
					engine.NewService(streams)

				// Submit distinct logical bids.
				for bidIndex := 0; bidIndex < day08TotalBids; bidIndex++ {
					_, err :=
						service.
							ReserveBidWithIdempotencyKey(
								ctx,
								auctionIDText,
								uuid.NewString(),
								int64(
									50_000+
										bidIndex,
								),
								uuid.NewString(),
							)
					if err != nil {
						t.Fatalf(
							"submit Day 8 bid %d: %v",
							bidIndex,
							err,
						)
					}
				}

				group :=
					"cg:day08:" +
						uuid.NewString()

				child := exec.Command(
					os.Args[0],
					"-test.run=^TestDay08CrashWorker$",
					"-test.v",
				)

				child.Env = append(
					os.Environ(),
					"BIDLANE_DAY08_CHILD=1",
					"DAY08_AUCTION_ID="+
						auctionIDText,
					"DAY08_GROUP="+group,
					"DAY08_TOTAL="+
						strconv.Itoa(
							day08TotalBids,
						),
					"DAY08_KILL_AFTER="+
						strconv.Itoa(
							killAfter,
						),
					"POSTGRES_ADMIN_DSN="+
						postgresDSN,
					"REDIS_ADDR="+
						redisAddress,
				)

				output, childErr :=
					child.CombinedOutput()

				if childErr == nil {
					t.Fatalf(
						"crash worker unexpectedly exited successfully:\n%s",
						output,
					)
				}

				var exitError *exec.ExitError

				if !errors.As(
					childErr,
					&exitError,
				) {
					t.Fatalf(
						"crash worker returned non-exit error: %v\n%s",
						childErr,
						output,
					)
				}

				if exitError.ExitCode() != 1 {
					t.Fatalf(
						"crash worker exit code=%d, expected 1:\n%s",
						exitError.ExitCode(),
						output,
					)
				}

				expectedMarker :=
					fmt.Sprintf(
						"DAY08_CRASH_AFTER_PERSIST=%d",
						killAfter,
					)

				if !strings.Contains(
					string(output),
					expectedMarker,
				) {
					t.Fatalf(
						"worker did not reach intended crash point %d:\n%s",
						killAfter,
						output,
					)
				}

				// The kill-point bid committed before the crash.
				var rowsAfterCrash int64

				if err := adminPool.QueryRow(
					ctx,
					`
						SELECT COUNT(*)
						FROM bids
						WHERE auction_id = $1
					`,
					auctionID,
				).Scan(&rowsAfterCrash); err != nil {
					t.Fatalf(
						"count bids after crash: %v",
						err,
					)
				}

				if rowsAfterCrash !=
					int64(killAfter) {
					t.Fatalf(
						"expected %d committed rows at crash point, got %d",
						killAfter,
						rowsAfterCrash,
					)
				}

				pendingBeforeRecovery,
					err :=
					redisClient.XPending(
						ctx,
						redisstore.StreamKey(
							auctionIDText,
						),
						group,
					).Result()
				if err != nil {
					t.Fatalf(
						"read pending entries after crash: %v",
						err,
					)
				}

				if pendingBeforeRecovery.Count == 0 {
					t.Fatal(
						"expected at least the committed-but-unacknowledged entry to remain pending",
					)
				}

				// Allow the pending entries to exceed the test's
				// one-millisecond idle threshold.
				time.Sleep(
					10 * time.Millisecond,
				)

				restartConsumer :=
					engine.NewLedgerConsumer(
						streams,
						postgresstore.NewLedgerStore(
							enginePool,
						),
						slog.New(
							slog.NewTextHandler(
								io.Discard,
								nil,
							),
						),
						engine.LedgerConsumerConfig{
							AuctionID: auctionIDText,
							Group:     group,
							Consumer: "day08-restart-" +
								uuid.NewString(),
							BatchSize: day08TotalBids,
							BlockPeriod: 250 *
								time.Millisecond,
							RecoveryMinIdle:   time.Millisecond,
							RecoveryBatchSize: 7,
						},
					)

				recovered, err :=
					restartConsumer.
						RecoverPending(ctx)
				if err != nil {
					t.Fatalf(
						"recover pending entries: %v",
						err,
					)
				}

				if recovered == 0 {
					t.Fatal(
						"restart consumer claimed no pending entries",
					)
				}

				// Some entries may not have been delivered to the
				// crashed process if XREADGROUP returned a partial
				// batch. Process any still-new entries normally.
				var rowsAfterRecovery int64

				if err := adminPool.QueryRow(
					ctx,
					`
						SELECT COUNT(*)
						FROM bids
						WHERE auction_id = $1
					`,
					auctionID,
				).Scan(
					&rowsAfterRecovery,
				); err != nil {
					t.Fatalf(
						"count rows after XAUTOCLAIM: %v",
						err,
					)
				}

				if rowsAfterRecovery <
					day08TotalBids {
					remaining :=
						day08TotalBids -
							int(
								rowsAfterRecovery,
							)

					if err :=
						restartConsumer.
							ProcessExactly(
								ctx,
								remaining,
							); err != nil {
						t.Fatalf(
							"process %d never-delivered entries: %v",
							remaining,
							err,
						)
					}
				}

				assertDay08FinalState(
					t,
					ctx,
					adminPool,
					redisClient,
					auctionID,
					auctionIDText,
					group,
				)

				t.Logf(
					"killAfter=%d rowsAtCrash=%d pending=%d recovered=%d finalRows=%d",
					killAfter,
					rowsAfterCrash,
					pendingBeforeRecovery.Count,
					recovered,
					day08TotalBids,
				)
			},
		)
	}

	t.Logf(
		"Day 8 passed: %d randomized kill-restart runs, zero lost bids, zero duplicate rows",
		day08CrashRuns,
	)
}

// TestDay08CrashWorker runs only in the child process.
//
// It deliberately calls os.Exit(1) after PostgreSQL commit but
// before Redis XACK.
func TestDay08CrashWorker(
	t *testing.T,
) {
	if os.Getenv(
		"BIDLANE_DAY08_CHILD",
	) != "1" {
		t.Skip(
			"Day 8 crash worker runs only as a subprocess",
		)
	}

	auctionID :=
		os.Getenv("DAY08_AUCTION_ID")
	group :=
		os.Getenv("DAY08_GROUP")

	total :=
		mustDay08EnvironmentInteger(
			t,
			"DAY08_TOTAL",
		)

	killAfter :=
		mustDay08EnvironmentInteger(
			t,
			"DAY08_KILL_AFTER",
		)

	postgresDSN :=
		os.Getenv("POSTGRES_ADMIN_DSN")
	redisAddress :=
		os.Getenv("REDIS_ADDR")

	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()

	enginePool, err :=
		postgresstore.ConnectPool(
			ctx,
			postgresDSN,
			"bidlane_engine",
		)
	if err != nil {
		t.Fatalf(
			"child connect Engine pool: %v",
			err,
		)
	}
	defer enginePool.Close()

	redisClient :=
		redisstore.NewClient(
			redisAddress,
			"",
			0,
		)
	defer redisClient.Close()

	streams :=
		redisstore.NewStreamStore(
			redisClient,
		)

	persistedCount := 0

	consumer :=
		engine.NewLedgerConsumer(
			streams,
			postgresstore.NewLedgerStore(
				enginePool,
			),
			slog.New(
				slog.NewTextHandler(
					io.Discard,
					nil,
				),
			),
			engine.LedgerConsumerConfig{
				AuctionID:   auctionID,
				Group:       group,
				Consumer:    "day08-crashed-consumer",
				BatchSize:   int64(total),
				BlockPeriod: 250 * time.Millisecond,

				AfterPersistBeforeAck: func(
					_ redisstore.BidStreamEntry,
					_ postgresstore.AppendBidResult,
				) {
					persistedCount++

					if persistedCount ==
						killAfter {
						// Write directly to stderr so the
						// parent can prove this exact crash
						// point was reached.
						_, _ = fmt.Fprintf(
							os.Stderr,
							"DAY08_CRASH_AFTER_PERSIST=%d\n",
							persistedCount,
						)

						// Deliberately bypass defers and
						// graceful shutdown.
						os.Exit(1)
					}
				},
			},
		)

	if err := consumer.ProcessExactly(
		ctx,
		total,
	); err != nil {
		t.Fatalf(
			"child process bids: %v",
			err,
		)
	}

	t.Fatalf(
		"child processed all entries without reaching crash point %d",
		killAfter,
	)
}

func mustDay08EnvironmentInteger(
	t *testing.T,
	name string,
) int {
	t.Helper()

	value := os.Getenv(name)

	number, err := strconv.Atoi(value)
	if err != nil {
		t.Fatalf(
			"environment variable %s=%q is not an integer: %v",
			name,
			value,
			err,
		)
	}

	if number <= 0 {
		t.Fatalf(
			"environment variable %s must be positive, got %d",
			name,
			number,
		)
	}

	return number
}

func assertDay08FinalState(
	t *testing.T,
	ctx context.Context,
	adminPool *pgxpool.Pool,
	redisClient *redis.Client,
	auctionID uuid.UUID,
	auctionIDText string,
	group string,
) {
	t.Helper()

	var (
		rowCount              int64
		distinctSequences     int64
		distinctIdempotency   int64
		distinctStreamEntries int64
		minimumSequence       int64
		maximumSequence       int64
		sequenceSum           int64
	)

	if err := adminPool.QueryRow(
		ctx,
		`
			SELECT
				COUNT(*),
				COUNT(DISTINCT sequence_no),
				COUNT(DISTINCT idempotency_key),
				COUNT(DISTINCT stream_entry_id),
				MIN(sequence_no),
				MAX(sequence_no),
				SUM(sequence_no)
			FROM bids
			WHERE auction_id = $1
		`,
		auctionID,
	).Scan(
		&rowCount,
		&distinctSequences,
		&distinctIdempotency,
		&distinctStreamEntries,
		&minimumSequence,
		&maximumSequence,
		&sequenceSum,
	); err != nil {
		t.Fatalf(
			"query Day 8 final ledger: %v",
			err,
		)
	}

	expectedSum := int64(
		day08TotalBids *
			(day08TotalBids + 1) /
			2,
	)

	if rowCount != day08TotalBids {
		t.Fatalf(
			"lost bids: expected %d rows, got %d",
			day08TotalBids,
			rowCount,
		)
	}

	if distinctSequences != day08TotalBids {
		t.Fatalf(
			"expected %d unique sequences, got %d",
			day08TotalBids,
			distinctSequences,
		)
	}

	if distinctIdempotency != day08TotalBids {
		t.Fatalf(
			"expected %d unique idempotency keys, got %d",
			day08TotalBids,
			distinctIdempotency,
		)
	}

	if distinctStreamEntries != day08TotalBids {
		t.Fatalf(
			"expected %d unique stream IDs, got %d",
			day08TotalBids,
			distinctStreamEntries,
		)
	}

	if minimumSequence != 1 ||
		maximumSequence != day08TotalBids {
		t.Fatalf(
			"expected sequences 1..%d, got %d..%d",
			day08TotalBids,
			minimumSequence,
			maximumSequence,
		)
	}

	if sequenceSum != expectedSum {
		t.Fatalf(
			"expected sequence sum %d, got %d",
			expectedSum,
			sequenceSum,
		)
	}

	var gapCount int64

	if err := adminPool.QueryRow(
		ctx,
		`
			SELECT COUNT(*)
			FROM generate_series(
				1::BIGINT,
				$2::BIGINT
			) AS expected(sequence_no)
			LEFT JOIN bids
			  ON bids.auction_id = $1
			 AND bids.sequence_no =
			     expected.sequence_no
			WHERE bids.sequence_no IS NULL
		`,
		auctionID,
		day08TotalBids,
	).Scan(&gapCount); err != nil {
		t.Fatalf(
			"query Day 8 sequence gaps: %v",
			err,
		)
	}

	if gapCount != 0 {
		t.Fatalf(
			"recovery produced %d sequence gaps",
			gapCount,
		)
	}

	var counterValue int64

	if err := adminPool.QueryRow(
		ctx,
		`
			SELECT last_seq
			FROM auction_sequences
			WHERE auction_id = $1
		`,
		auctionID,
	).Scan(&counterValue); err != nil {
		t.Fatalf(
			"read Day 8 sequence counter: %v",
			err,
		)
	}

	if counterValue != day08TotalBids {
		t.Fatalf(
			"expected sequence counter %d, got %d",
			day08TotalBids,
			counterValue,
		)
	}

	pending, err := redisClient.XPending(
		ctx,
		redisstore.StreamKey(
			auctionIDText,
		),
		group,
	).Result()
	if err != nil {
		t.Fatalf(
			"read final pending state: %v",
			err,
		)
	}

	if pending.Count != 0 {
		t.Fatalf(
			"%d entries remain pending after recovery",
			pending.Count,
		)
	}
}
