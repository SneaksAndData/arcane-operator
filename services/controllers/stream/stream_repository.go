package stream

// StreamRepository defines the interface for accessing stream definitions
type StreamRepository interface {

	// GetStreamById retrieves the stream definition by its identity
	GetStreamById(id StreamIdentity) (StreamDefinition, error)
}
