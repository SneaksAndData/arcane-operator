package conf

// StreamOperatorConfiguration holds configuration for StreamClassOperatorService.
type StreamOperatorConfiguration struct {

	// MaxBufferCapacity is the max buffer capacity for StreamClasses events stream.
	MaxBufferCapacity int

	// RateLimitConfiguration holds the rate limiting configuration
	RateLimiting RateLimitConfiguration
}
