package cron_job

import (
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend"
	v1 "k8s.io/api/batch/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

var (
	_ predicate.TypedPredicate[*v1.CronJob] = (*Predicate)(nil)
)

type Predicate struct {
	backend.SecondaryResourcePredicate[*v1.CronJob]
}

func (j *Predicate) Update(e event.TypedUpdateEvent[*v1.CronJob]) bool { // coverage-ignore (trivial)
	return true
}

func NewPredicate() predicate.TypedPredicate[*v1.CronJob] { // coverage-ignore (trivial)
	return &Predicate{}
}
