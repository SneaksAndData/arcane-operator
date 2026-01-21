package stream

import (
	v1 "k8s.io/api/batch/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

var (
	_ predicate.TypedPredicate[*v1.Job] = (*JobFilter)(nil)
)

// JobFilter is a predicate that allows job events to pass through to the Stream controller.
type JobFilter struct {
}

// Create is called when a new object is created.
// It returns true to allow the event to be processed.
func (j *JobFilter) Create(_ event.TypedCreateEvent[*v1.Job]) bool { // coverage-ignore (trivial)
	return true
}

// Delete is called when an object is deleted.
// It returns true to allow the event to be processed.
func (j *JobFilter) Delete(_ event.TypedDeleteEvent[*v1.Job]) bool { // coverage-ignore (trivial)
	return true
}

// Update is called when an object is updated.
func (j *JobFilter) Update(e event.TypedUpdateEvent[*v1.Job]) bool { // coverage-ignore (trivial)
	return NewStreamingJobFromV1Job(e.ObjectNew).IsCompleted()
}

// Generic is called for generic events.
// We're filtering out generic events by returning false.
func (j *JobFilter) Generic(_ event.TypedGenericEvent[*v1.Job]) bool { // coverage-ignore (trivial)
	return false
}

func NewJobFilter() predicate.TypedPredicate[*v1.Job] { // coverage-ignore (trivial)
	return &JobFilter{}
}
