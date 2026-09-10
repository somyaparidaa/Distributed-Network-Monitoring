package polling

import (
	"context"
	"errors"
	"time"
)

// RetryConfig configures bounded retry attempts and backoff timing.
type RetryConfig struct {
	MaxRetries     int
	InitialBackoff time.Duration
}

// IsRetryable determines whether an error is transient and safe to retry.
// Only 503 (ErrDeviceUnavailable) and transport/timeout errors (ErrDeviceUnreachable) are retried.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrDeviceUnavailable) || errors.Is(err, ErrDeviceUnreachable)
}

// ExecuteWithRetry executes a poll operation, retrying transient errors with exponential backoff.
func ExecuteWithRetry(ctx context.Context, cfg RetryConfig, fn func() (Telemetry, error)) (Telemetry, error) {
	var lastErr error
	backoff := cfg.InitialBackoff
	if backoff <= 0 {
		backoff = 25 * time.Millisecond
	}

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		// Check context before executing attempt
		if err := ctx.Err(); err != nil {
			return Telemetry{}, err
		}

		telemetry, err := fn()
		if err == nil {
			return telemetry, nil
		}

		lastErr = err

		// Do not retry non-retryable errors (e.g. 404, malformed json)
		if !IsRetryable(err) {
			return Telemetry{}, lastErr
		}

		// If this was the last attempt, don't sleep
		if attempt == cfg.MaxRetries {
			break
		}

		// Wait for backoff duration or context cancellation
		select {
		case <-ctx.Done():
			return Telemetry{}, ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
		}
	}

	return Telemetry{}, lastErr
}
