package stream

type StreamIdentity struct {
	StreamId  string
	Namespace string
}

// String returns a string representation of the StreamIdentity
func (s StreamIdentity) String() string {
	return s.Namespace + "/" + s.StreamId
}
