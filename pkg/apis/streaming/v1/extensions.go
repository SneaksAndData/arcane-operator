package v1

import (
	"fmt"
	"github.com/SneaksAndData/arcane-operator/services/job"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"strings"
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

const tagPrefix = "arcane.sneaksanddata.com"

// MetricsTags returns the metrics tags for the StreamClass
func (in *StreamClass) MetricsTags() map[string]string {
	namespace := "unknown"
	if in.Namespace != "" {
		namespace = strings.ToLower(in.Namespace)
	}

	kindRef := "unknown"
	if in.Spec.KindRef != "" {
		kindRef = camelCaseToSnakeCase(in.Spec.KindRef)
	}

	kind := "unknown"
	if in.Kind != "" {
		kind = camelCaseToSnakeCase(in.Kind)
	}

	return map[string]string{
		fmt.Sprintf("%s/namespace", tagPrefix): namespace,
		fmt.Sprintf("%s/kind_ref", tagPrefix):  kindRef,
		fmt.Sprintf("%s/kind", tagPrefix):      kind,
		fmt.Sprintf("%s/phase", tagPrefix):     strings.ToLower(string(in.Status.Phase)),
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

func camelCaseToSnakeCase(input string) string {
	var output strings.Builder
	for i, ch := range input {
		if ch >= 'A' && ch <= 'Z' {
			if i > 0 && input[i-1] >= 'a' && input[i-1] <= 'z' {
				output.WriteRune('_')
			}
			output.WriteRune(ch + 32) // Convert to lowercase
		} else {
			output.WriteRune(ch)
		}
	}
	return output.String()
}
