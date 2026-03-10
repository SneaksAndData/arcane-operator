package stream

import (
	"context"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/job"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Backend string

const (
	BatchJob Backend = "BatchJobBackend"
	CronJob  Backend = "CronJob"
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
	job.ConfiguratorProvider

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

	// GetJobTemplate returns the job template reference based on the stream definition and backfill request.
	GetJobTemplate(request *v1.BackfillRequest) types.NamespacedName

	// SetConditions returns the current conditions of the stream definition.
	SetConditions(conditions []metav1.Condition) error

	// ComputeConditions computes the conditions for the stream definition based on the backfill request.
	ComputeConditions(bfr *v1.BackfillRequest) []metav1.Condition

	// GetReferenceForSecret returns a LocalObjectReference for the given secret name.
	GetReferenceForSecret(name string) (*corev1.LocalObjectReference, error)

	// Validate validates the stream definition and returns an error if any required fields are missing or invalid.
	Validate() error

	// GetBackend returns the streaming backend type (e.g., BatchJob, CronJob) associated with this stream definition.
	GetBackend() Backend
}

// DefinitionParser is a function type that takes an unstructured object and returns a validated Definition or an
// error if the parsing fails. This allows for flexible parsing logic that can be customized based on the specific
// structure of the unstructured object.
type DefinitionParser func(*unstructured.Unstructured) (Definition, error)

// GetStreamForClass retrieves the stream definition for a given stream class and namespaced name.
func GetStreamForClass(ctx context.Context, client client.Client, sc *v1.StreamClass, name types.NamespacedName, definitionParser DefinitionParser) (Definition, error) {
	gvk := sc.TargetResourceGvk()
	maybeSd := unstructured.Unstructured{}
	maybeSd.SetGroupVersionKind(gvk)
	err := client.Get(ctx, name, &maybeSd)
	if err != nil { // coverage-ignore
		return nil, err
	}
	return definitionParser(&maybeSd)
}
