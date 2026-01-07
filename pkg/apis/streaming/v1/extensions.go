package v1

import (
	"github.com/SneaksAndData/arcane-operator/services"
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

var _ services.JobConfiguratorProvider = (*BackfillRequest)(nil)

// JobConfigurator returns a JobConfigurator for the BackfillRequest
func (in *BackfillRequest) JobConfigurator() services.JobConfigurator {
	if in == nil {
		return nil
	}
	return services.NewEnvironmentConfigurator(in, "OVERRIDE")
}
