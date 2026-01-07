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
	Suspended   Phase = "Suspended"
	Failed      Phase = "Failed"
)

type Definition interface {
	// GetPhase returns the current phase of the stream definition.
	GetPhase() Phase

	// Suspended returns true if the stream definition is suspended.
	Suspended() bool

	// CurrentConfiguration returns the hash sum of the current configuration (spec) of the stream definition.
	CurrentConfiguration() (string, error)

	// LastAppliedConfiguration returns the hash sum of the last observed configuration (spec) of the stream definition.
	LastAppliedConfiguration() string

	// RecomputeConfiguration recomputes and updates the last observed configuration hash.
	// This should be called after any changes to the spec have been applied and the object saved to the API server.
	RecomputeConfiguration() error

	// NamespacedName returns the namespaced name of the stream definition.
	NamespacedName() types.NamespacedName

	// ToUnstructured converts the stream definition to an unstructured object.
	ToUnstructured() *unstructured.Unstructured

	// SetPhase sets the status of the stream definition.
	SetPhase(status Phase) error

	// StateString returns a string representation of the current state.
	// This is primarily used for logging and debugging purposes.
	StateString() string

	// GetStreamingJobName returns the namespaced name of the streaming job associated with the stream definition.
	GetStreamingJobName() types.NamespacedName

	// GetBackfillJobName returns the namespaced name of the backfill job associated with the stream definition.
	GetBackfillJobName() types.NamespacedName

	// ToOwnerReference converts the stream definition to an owner reference.
	ToOwnerReference() v1.OwnerReference

	// JobConfigurator returns a JobConfigurator for the stream definition.
	JobConfigurator() services.JobConfigurator
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
