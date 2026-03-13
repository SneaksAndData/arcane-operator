package job

import (
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend"
	v1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
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
	res, err := FromResource(e.ObjectNew)

	if err != nil {
		// If we can't parse the resource, we don't want to trigger a reconcile.
		j.getLogger(types.NamespacedName{Name: e.ObjectNew.Namespace, Namespace: e.ObjectNew.Namespace}).
			V(0).
			Error(err, "unable to parse job resource in predicate")
		return false
	}

	return res.IsCompleted() || res.IsFailed()
}

func NewPredicate() predicate.TypedPredicate[*v1.Job] { // coverage-ignore (trivial)
	return &Predicate{}
}

func (j *Predicate) getLogger(request types.NamespacedName) klog.Logger { // coverage-ignore (trivial)
	return klog.Background().
		WithName("job.Backend").
		WithValues("namespace", request.Namespace, "streamId", request.Name)
}
