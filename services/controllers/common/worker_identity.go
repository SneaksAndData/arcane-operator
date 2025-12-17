package common

// WorkerId defines a unique identifier for a stream operator worker
// Typically this would be derived from the name/namespace of the object being watched
type WorkerId string

// StreamClassWorkerIdentity defines an interface for objects that can provide a WorkerId
type StreamClassWorkerIdentity interface {
	WorkerId() WorkerId
}
