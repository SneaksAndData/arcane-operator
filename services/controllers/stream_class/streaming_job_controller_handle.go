package stream_class

import (
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/common"
)

// StreamControllerHandle defines the interface for object that can be used to control a streaming job worker
type StreamControllerHandle interface {

	// IsUpdateNeeded Returns true if the streaming job needs to be updated
	IsUpdateNeeded(obj *v1.StreamClass) bool

	// Id returns the identity of the streaming job worker
	Id() common.StreamClassWorkerIdentity

	// Stop stops the streaming job worker and returns a context that is done when the worker has stopped
	Stop() error
}
