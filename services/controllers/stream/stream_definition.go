package stream

import (
	"github.com/SneaksAndData/arcane-operator/services"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

type Phase string

const (
	New         Phase = ""
	Pending     Phase = "Pending"
	Running     Phase = "Running"
	Backfilling Phase = "Backfilling"
	Suspended   Phase = "Completed"
	Failed      Phase = "Failed"
)

type StreamDefinition interface {
	GetPhase() Phase
	Suspended() bool
	CurrentConfiguration() string
	LastObservedConfiguration() string
	NamespacedName() types.NamespacedName
	ToUnstructured() *unstructured.Unstructured
	SetStatus(status Phase)
	StateString() string

	GetStreamingJobName() (types.NamespacedName, error)
	GetBackfillJobName() (types.NamespacedName, error)

	ToOwnerReference() (v1.OwnerReference, error)
	JobConfigurator() services.JobConfigurator
}

func FromUnstructured(obj *unstructured.Unstructured) StreamDefinition {
	panic("implement me")
}
