package conf

// StreamingJobOperatorConfiguration holds configuration for StreamOperatorService.
type StreamingJobOperatorConfiguration struct {

	// MaxBufferCapacity is the max buffer capacity for StreamClasses events stream.
	MaxBufferCapacity int
}
