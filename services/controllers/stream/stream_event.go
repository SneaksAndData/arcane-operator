package stream

import "fmt"

type EventType string

// We handle only addition and modification events for streams
// Stream class deletion leads to the deletion of an associated stream.
// And we rely on the Kubernetes garbage collector to clean up associated resources.
const (
	Added    EventType = "Added"
	Modified EventType = "Modified"
)

type StreamEvent interface {
	Type() EventType

	ConfigurationChecksum() string

	CrashLoopDetected() bool

	Suspended() bool

	BackfillRequested() bool

	ClassName() string

	StreamId() StreamIdentity

	StreamNameSpace() string
}

func tryFromKubernetesObject(_ any) (StreamEvent, error) {
	return nil, fmt.Errorf("Not implemented")
}
