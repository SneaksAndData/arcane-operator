package stream

// JobManager defines the interface for managing streaming jobs
type JobManager interface {

	// EnsureStopped ensures that the streaming job with the given identity is stopped
	// If the job is already stopped, this is a no-op
	EnsureStopped(id StreamIdentity) error

	// StartJob starts a new streaming job with the given definition
	StartJob(definition StreamDefinition)

	// StartBackfill starts a backfill job for the given stream definition
	StartBackfill(definition StreamDefinition)
}
