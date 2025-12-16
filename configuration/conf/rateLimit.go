package conf

import (
	"golang.org/x/time/rate"
	"time"
)

type RateLimitConfiguration struct {
	// RateLimitElementsPerSecond defines the number of elements added to the queue per second.
	RateLimitElementsPerSecond rate.Limit

	// RateLimitElementsBurst defines the maximum burst size for the rate limiter.
	RateLimitElementsBurst int

	// FailureRateBaseDelay defines the base delay for exponential backoff on failures.
	FailureRateBaseDelay time.Duration

	// FailureRateMaxDelay defines the maximum delay for exponential backoff on failures.
	FailureRateMaxDelay time.Duration
}
