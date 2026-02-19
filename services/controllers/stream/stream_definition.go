package stream

import (
	"context"
	"fmt"
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/job"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	// This method can return a copy of the underlying object, so it should not be used for modifications.
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

	// ComputeConditions computes the conditions for the stream definition based on the backfill request.
	ComputeConditions(bfr *v1.BackfillRequest) []metav1.Condition

	// GetReferenceForSecret returns a LocalObjectReference for the given secret name.
	GetReferenceForSecret(name string) (*corev1.LocalObjectReference, error)

	// UpdateUnderlyingObject allows for direct updates to the underlying unstructured object, which can be useful
	// for operations that require modifying fields not exposed by the Definition interface.
	// The definition implementation is responsible for ensuring that any changes made to the underlying
	// object are consistent with the Definition's state and behavior.
	UpdateUnderlyingObject(processor func(*unstructured.Unstructured) error) error
}

func FromUnstructured(obj *unstructured.Unstructured) (Definition, error) { // coverage-ignore
	v := unstructuredWrapper{
		underlying: obj,
	}

	err := v.Validate()
	if err != nil { // coverage-ignore
		return nil, fmt.Errorf("failed to parse Stream definition: %w", err)
	}
	return &v, nil
}

// GetStreamForClass retrieves the stream definition for a given stream class and namespaced name.
func GetStreamForClass(ctx context.Context, client client.Client, sc *v1.StreamClass, name types.NamespacedName) (Definition, error) {
	gvk := sc.TargetResourceGvk()
	maybeSd := unstructured.Unstructured{}
	maybeSd.SetGroupVersionKind(gvk)
	err := client.Get(ctx, name, &maybeSd)
	if err != nil {
		return nil, err
	}
	return FromUnstructured(&maybeSd)
}
