package v1

import "k8s.io/apimachinery/pkg/runtime/schema"

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
