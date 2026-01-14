package stream

import (
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

var (
	_ predicate.TypedPredicate[*v1.BackfillRequest] = (*BfrFilter)(nil)
)

type BfrFilter struct {
}

func (j *BfrFilter) Create(e event.TypedCreateEvent[*v1.BackfillRequest]) bool {
	return true
}

func (j *BfrFilter) Delete(e event.TypedDeleteEvent[*v1.BackfillRequest]) bool {
	return true
}

func (j *BfrFilter) Update(e event.TypedUpdateEvent[*v1.BackfillRequest]) bool {
	return true
}

func (j *BfrFilter) Generic(e event.TypedGenericEvent[*v1.BackfillRequest]) bool {
	return true
}

func NewBfrFilter() predicate.TypedPredicate[*v1.BackfillRequest] {
	return &BfrFilter{}
}
