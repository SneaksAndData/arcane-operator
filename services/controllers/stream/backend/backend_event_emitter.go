package backend

import (
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
)

var _ stream.EventEmitter = (*EventEmitter)(nil)

type EventEmitter struct {
	eventRecorder record.EventRecorder
}

func NewBackendEventEmitter(eventRecorder record.EventRecorder) *EventEmitter {
	return &EventEmitter{
		eventRecorder: eventRecorder,
	}
}

func (b EventEmitter) MigrationJobNameFallback(object runtime.Object, usedJob *corev1.ObjectReference) {
	b.eventRecorder.Eventf(object, "Warning",
		"MigrationFallback",
		"Migration job not found, falling back to %s/%s", usedJob.Namespace, usedJob.Name)

}
