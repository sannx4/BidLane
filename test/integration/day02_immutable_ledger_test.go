package integration_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	postgresstore "example.com/bidlane/internal/store/postgres"
)

func TestDay02BidLedgerIsImmutableAtDatabaseLayer(
	t *testing.T,
) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	dsn := os.Getenv("POSTGRES_ADMIN_DSN")

	if dsn == "" {
		dsn = "postgres://bidlane:bidlane@localhost:5432/bidlane?sslmode=disable"
	}

	// This connection acts as the table owner/database administrator.
	adminConn, err := postgresstore.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect as database administrator: %v", err)
	}
	defer adminConn.Close(context.Background())

	// A separate connection will temporarily assume the restricted
	// bidlane_engine role.
	appConn, err := postgresstore.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for application-role test: %v", err)
	}
	defer appConn.Close(context.Background())

	// Keep repeated local test runs independent.
	if _, err := adminConn.Exec(
		ctx,
		"TRUNCATE TABLE bids, auctions CASCADE",
	); err != nil {
		t.Fatalf(
			"clean existing test rows: %v\n"+
				"Did you apply the Day 2 migration?",
			err,
		)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		_, _ = adminConn.Exec(
			cleanupCtx,
			"TRUNCATE TABLE bids, auctions CASCADE",
		)
	})

	auctionID := uuid.New()
	bidderID := uuid.New()
	idempotencyKey := uuid.New()

	// Restrict this connection to the application's database privileges.
	if _, err := appConn.Exec(
		ctx,
		"SET ROLE bidlane_engine",
	); err != nil {
		t.Fatalf("assume bidlane_engine role: %v", err)
	}

	defer func() {
		_, _ = appConn.Exec(
			context.Background(),
			"RESET ROLE",
		)
	}()

	// The application role is allowed to append auction/bid data.
	if _, err := appConn.Exec(
		ctx,
		`
	INSERT INTO auctions (
		id,
		effective_close_time
	)
	VALUES ($1, $2)
	`,
		auctionID,
		time.Now().UTC().Add(24*time.Hour),
	); err != nil {
		t.Fatalf(
			"application role should be allowed to insert auction: %v",
			err,
		)
	}

	if _, err := appConn.Exec(
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
		auctionID,
		int64(1),
		int64(50_000),
		bidderID,
		idempotencyKey,
		"1752821000000-0",
	); err != nil {
		t.Fatalf(
			"application role should be allowed to append a bid: %v",
			err,
		)
	}

	// Prove that (auction_id, sequence_no) is unique.
	_, err = appConn.Exec(
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
		auctionID,
		int64(1), // Same auction and sequence number.
		int64(60_000),
		uuid.New(),
		uuid.New(),
		"1752821000000-1",
	)

	requirePostgresErrorCode(
		t,
		err,
		"23505",
		"duplicate (auction_id, sequence_no)",
	)

	// The application role must not possess UPDATE permission.
	_, err = appConn.Exec(
		ctx,
		"UPDATE bids SET amount = 1",
	)

	requirePostgresErrorCode(
		t,
		err,
		"42501",
		"application-role UPDATE",
	)

	// The application role must not possess DELETE permission.
	_, err = appConn.Exec(
		ctx,
		"DELETE FROM bids",
	)

	requirePostgresErrorCode(
		t,
		err,
		"42501",
		"application-role DELETE",
	)

	// Return this connection to its original administrator role.
	if _, err := appConn.Exec(
		ctx,
		"RESET ROLE",
	); err != nil {
		t.Fatalf("reset application role: %v", err)
	}

	// The owner has table privileges, so this reaches the trigger.
	// The trigger itself must reject the UPDATE.
	_, err = adminConn.Exec(
		ctx,
		"UPDATE bids SET amount = 1",
	)

	requirePostgresErrorCode(
		t,
		err,
		"55000",
		"owner UPDATE blocked by immutability trigger",
	)

	// The trigger must reject DELETE as well.
	_, err = adminConn.Exec(
		ctx,
		"DELETE FROM bids",
	)

	requirePostgresErrorCode(
		t,
		err,
		"55000",
		"owner DELETE blocked by immutability trigger",
	)

	// Prove that the failed UPDATE did not change the amount.
	var storedAmount int64

	if err := adminConn.QueryRow(
		ctx,
		`
			SELECT amount
			FROM bids
			WHERE auction_id = $1
			  AND sequence_no = $2
		`,
		auctionID,
		int64(1),
	).Scan(&storedAmount); err != nil {
		t.Fatalf("read original bid amount: %v", err)
	}

	if storedAmount != 50_000 {
		t.Fatalf(
			"immutable bid changed: expected amount 50000, got %d",
			storedAmount,
		)
	}

	// Prove that the failed DELETE did not remove the row.
	var bidCount int

	if err := adminConn.QueryRow(
		ctx,
		`
			SELECT COUNT(*)
			FROM bids
			WHERE auction_id = $1
			  AND sequence_no = $2
		`,
		auctionID,
		int64(1),
	).Scan(&bidCount); err != nil {
		t.Fatalf("count immutable bid: %v", err)
	}

	if bidCount != 1 {
		t.Fatalf(
			"expected the original bid to remain, found %d rows",
			bidCount,
		)
	}

	t.Log(
		"Day 2 passed: bid inserted once, composite order unique, " +
			"application mutation revoked, owner mutation rejected by trigger",
	)
}

func requirePostgresErrorCode(
	t *testing.T,
	err error,
	expectedCode string,
	operation string,
) {
	t.Helper()

	if err == nil {
		t.Fatalf(
			"%s unexpectedly succeeded; expected PostgreSQL error %s",
			operation,
			expectedCode,
		)
	}

	var postgresError *pgconn.PgError

	if !errors.As(err, &postgresError) {
		t.Fatalf(
			"%s returned a non-PostgreSQL error: %v",
			operation,
			err,
		)
	}

	if postgresError.Code != expectedCode {
		t.Fatalf(
			"%s returned SQLSTATE %s, expected %s; message: %s",
			operation,
			postgresError.Code,
			expectedCode,
			postgresError.Message,
		)
	}

	t.Logf(
		"%s correctly failed with SQLSTATE %s: %s",
		operation,
		postgresError.Code,
		postgresError.Message,
	)
}
