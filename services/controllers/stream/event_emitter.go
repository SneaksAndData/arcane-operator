package stream

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// EventEmitter interface is used to emit events related to stream processing.
type EventEmitter interface {

	// MigrationJobNameFallback is called when a migration job is not found and a fallback job is used.
	MigrationJobNameFallback(object runtime.Object, usedJob *corev1.ObjectReference)
}
