package job

import (
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend"
	v1 "k8s.io/api/batch/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

var (
	_ predicate.TypedPredicate[*v1.Job] = (*Predicate)(nil)
)

// Predicate is a predicate that allows job events to pass through to the Stream controller.
type Predicate struct {
	backend.SecondaryResourcePredicate[*v1.Job]
}

// Update is called when an object is updated.
func (j *Predicate) Update(e event.TypedUpdateEvent[*v1.Job]) bool { // coverage-ignore (trivial)
	return FromResource(e.ObjectNew).IsCompleted() || FromResource(e.ObjectNew).IsFailed()
}

func NewPredicate() predicate.TypedPredicate[*v1.Job] { // coverage-ignore (trivial)
	return &Predicate{}
}
