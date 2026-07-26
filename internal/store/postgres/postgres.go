package postgresstore

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect creates one PostgreSQL connection.
//
// Day 2 uses this for operations that need one specific session,
// such as SET ROLE and RESET ROLE.
func Connect(
	ctx context.Context,
	dsn string,
) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf(
			"connect to PostgreSQL: %w",
			err,
		)
	}

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close(ctx)

		return nil, fmt.Errorf(
			"ping PostgreSQL: %w",
			err,
		)
	}

	return conn, nil
}

// ConnectPool creates a PostgreSQL connection pool.
//
// role may be:
//   - ""                : retain the original database user
//   - "bidlane_engine" : every pooled connection assumes the
//     restricted Engine role
func ConnectPool(
	ctx context.Context,
	dsn string,
	role string,
) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf(
			"parse PostgreSQL pool configuration: %w",
			err,
		)
	}

	switch role {
	case "":
		// Keep the original database user.

	case "bidlane_engine":
		config.AfterConnect = func(
			ctx context.Context,
			conn *pgx.Conn,
		) error {
			if _, err := conn.Exec(
				ctx,
				"SET ROLE bidlane_engine",
			); err != nil {
				return fmt.Errorf(
					"assume bidlane_engine role: %w",
					err,
				)
			}

			return nil
		}

	case "bidlane_outbox_relay":
		config.AfterConnect = func(
			ctx context.Context,
			conn *pgx.Conn,
		) error {
			if _, err := conn.Exec(
				ctx,
				"SET ROLE bidlane_outbox_relay",
			); err != nil {
				return fmt.Errorf(
					"assume bidlane_outbox_relay role: %w",
					err,
				)
			}

			return nil
		}

	default:
		return nil, fmt.Errorf(
			"unsupported PostgreSQL role %q",
			role,
		)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf(
			"create PostgreSQL pool: %w",
			err,
		)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()

		return nil, fmt.Errorf(
			"ping PostgreSQL pool: %w",
			err,
		)
	}

	return pool, nil
}
