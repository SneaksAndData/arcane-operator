package stream_class

import v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"

// Define event types for StreamClass events
type eventType string

const (
	StreamClassAdded   eventType = "StreamClassAdded"
	StreamClassUpdated eventType = "StreamClassUpdated"
	StreamClassDeleted eventType = "StreamClassDeleted"
)

// StreamClassEvent represents an event for a StreamClass object
type StreamClassEvent struct {
	Type        eventType
	StreamClass *v1.StreamClass
}
