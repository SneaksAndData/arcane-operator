package stream

import (
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/job"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	CurrentConfiguration(request *v1.BackfillRequest) (string, error)

	// LastAppliedConfiguration returns the hash sum of the last observed configuration (spec) of the stream definition.
	LastAppliedConfiguration() string

	// RecomputeConfiguration recomputes and updates the last observed configuration hash.
	// This should be called after any changes to the spec have been applied and the object saved to the API server.
	RecomputeConfiguration(request *v1.BackfillRequest) error

	// NamespacedName returns the namespaced name of the stream definition.
	NamespacedName() types.NamespacedName

	// ToUnstructured converts the stream definition to an unstructured object.
	ToUnstructured() *unstructured.Unstructured

	// SetPhase sets the status of the stream definition.
	SetPhase(status Phase) error

	// SetSuspended sets the suspended state of the stream definition.
	SetSuspended(suspended bool) error

	// StateString returns a string representation of the current state.
	// This is primarily used for logging and debugging purposes.
	StateString() string

	// ToOwnerReference converts the stream definition to an owner reference.
	ToOwnerReference() metav1.OwnerReference

	// ToConfiguratorProvider converts the stream definition to a JobConfiguratorProvider.
	ToConfiguratorProvider() job.ConfiguratorProvider

	// GetJobTemplate returns the job template reference based on the stream definition and backfill request.
	GetJobTemplate(request *v1.BackfillRequest) types.NamespacedName

	// SetConditions returns the current conditions of the stream definition.
	SetConditions(conditions []metav1.Condition) error
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
