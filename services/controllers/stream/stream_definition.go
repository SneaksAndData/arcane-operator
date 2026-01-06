package stream

import (
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
	Suspended   Phase = "Suspended"
	Failed      Phase = "Failed"
)

type Definition interface {
	GetPhase() Phase
	Suspended() bool
	CurrentConfiguration() string
	LastObservedConfiguration() string
	NamespacedName() types.NamespacedName
	ToUnstructured() *unstructured.Unstructured
	SetStatus(status Phase) error
	StateString() string

	GetStreamingJobName() (types.NamespacedName, error)
	GetBackfillJobName() (types.NamespacedName, error)

	ToOwnerReference() (v1.OwnerReference, error)
	JobConfigurator() JobConfigurator
}

func fromUnstructured(obj *unstructured.Unstructured) (Definition, error) {
	v := unstructuredWrapper{
		underlying: obj,
	}

	err := v.Validate()
	if err != nil {
		return nil, err
	}
	return &v, nil
}
