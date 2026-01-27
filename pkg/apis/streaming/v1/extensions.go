package v1

import (
	"github.com/SneaksAndData/arcane-operator/services/job"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// StateString returns a string representation of the current state
func (in *StreamClass) StateString() string {
	if in == nil {
		return "(nil)"
	}

	return string(in.Status.Phase)
}

// TargetResourceGvk returns the GroupVersionKind of the target resource
func (in *StreamClass) TargetResourceGvk() schema.GroupVersionKind {
	return schema.GroupVersionKind{
		Group:   in.Spec.APIGroupRef,
		Version: in.Spec.APIVersion,
		Kind:    in.Spec.KindRef,
	}
}

var _ job.ConfiguratorProvider = (*BackfillRequest)(nil)

// JobConfigurator returns a JobConfigurator for the BackfillRequest
func (in *BackfillRequest) JobConfigurator() (job.Configurator, error) {
	if in == nil {
		return nil, nil
	}
	configurator := job.NewConfiguratorChainBuilder().
		WithConfigurator(job.NewEnvironmentConfigurator(in.Spec, "OVERRIDE")).
		WithConfigurator(job.NewBackfillConfigurator(true)).
		Build()
	return configurator, nil
}
