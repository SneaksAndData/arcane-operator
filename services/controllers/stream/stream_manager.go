package stream

// StreamManager defines the interface for managing stream definition objects
type StreamManager interface {

	// SetFailed marks the given stream event as failed
	SetFailed(element StreamEvent) error

	// SetOperatorError marks the given stream event as having an operator error
	SetOperatorError(element StreamEvent) error
}
