package redisstore

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// StreamEntryTime returns the server-assigned ingress timestamp
// encoded in a Redis Stream ID.
//
// Example:
//
//	1752821000000-4
//	└─────────────┘
//	    milliseconds
//
// The second component orders entries created during the same
// millisecond but does not change their timestamp.
func StreamEntryTime(
	streamID string,
) (time.Time, error) {
	parts := strings.SplitN(
		streamID,
		"-",
		2,
	)

	if len(parts) != 2 ||
		parts[0] == "" ||
		parts[1] == "" {
		return time.Time{}, fmt.Errorf(
			"invalid Redis Stream ID %q",
			streamID,
		)
	}

	milliseconds, err := strconv.ParseInt(
		parts[0],
		10,
		64,
	)
	if err != nil {
		return time.Time{}, fmt.Errorf(
			"parse Redis Stream timestamp %q: %w",
			parts[0],
			err,
		)
	}

	if milliseconds < 0 {
		return time.Time{}, fmt.Errorf(
			"Redis Stream timestamp cannot be negative: %d",
			milliseconds,
		)
	}

	// Validate the sequence component as well.
	if _, err := strconv.ParseUint(
		parts[1],
		10,
		64,
	); err != nil {
		return time.Time{}, fmt.Errorf(
			"parse Redis Stream sequence %q: %w",
			parts[1],
			err,
		)
	}

	return time.UnixMilli(
		milliseconds,
	).UTC(), nil
}
