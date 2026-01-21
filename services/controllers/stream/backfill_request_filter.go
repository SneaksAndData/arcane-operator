package stream

import (
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

var (
	_ predicate.TypedPredicate[*v1.BackfillRequest] = (*BackfillRequestFilter)(nil)
)

type BackfillRequestFilter struct {
	streamClass string
}

// Create filters BackfillRequests that are not completed and match the specified stream class.
func (j *BackfillRequestFilter) Create(e event.TypedCreateEvent[*v1.BackfillRequest]) bool { // coverage-ignore (trivial)
	return !e.Object.Spec.Completed && e.Object.Spec.StreamClass == j.streamClass
}

// Delete filters BackfillRequests that are not completed and match the specified stream class.
func (j *BackfillRequestFilter) Delete(e event.TypedDeleteEvent[*v1.BackfillRequest]) bool { // coverage-ignore (trivial)
	return !e.Object.Spec.Completed && e.Object.Spec.StreamClass == j.streamClass
}

// Update always returns false to ignore update events.
func (j *BackfillRequestFilter) Update(_ event.TypedUpdateEvent[*v1.BackfillRequest]) bool { // coverage-ignore (trivial)
	return false
}

// Generic always returns false to ignore generic events.
func (j *BackfillRequestFilter) Generic(_ event.TypedGenericEvent[*v1.BackfillRequest]) bool { // coverage-ignore (trivial)
	return false
}

func NewBackfillRequestFilter(streamClass string) predicate.TypedPredicate[*v1.BackfillRequest] { // coverage-ignore (trivial)
	return &BackfillRequestFilter{
		streamClass: streamClass,
	}
}
