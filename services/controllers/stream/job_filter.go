package stream

import (
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

var (
	_ predicate.TypedPredicate[client.Object] = (*JobFilter)(nil)
)

type JobFilter struct {
}

func (j *JobFilter) Create(e event.TypedCreateEvent[client.Object]) bool {
	return true
}

func (j *JobFilter) Delete(e event.TypedDeleteEvent[client.Object]) bool {
	return true
}

func (j *JobFilter) Update(e event.TypedUpdateEvent[client.Object]) bool {
	return true
}

func (j *JobFilter) Generic(e event.TypedGenericEvent[client.Object]) bool {
	return true
}

func NewJobFilter() predicate.TypedPredicate[client.Object] {
	return &JobFilter{}
}
