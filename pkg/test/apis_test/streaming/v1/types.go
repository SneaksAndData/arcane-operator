//go: build test

package v1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MockStreamDefinitionSpec is a mock implementation of the StreamDefinitionSpec for testing purposes.
type MockStreamDefinitionSpec struct {
	// Source represents the source of the stream.
	Source string `json:"source"`

	// Destination represents the destination of the stream.
	Destination string `json:"destination"`

	// Suspended indicates whether the stream is suspended.
	Suspended bool `json:"suspended"`

	// JobTemplateRef represents a reference to the job template.
	JobTemplateRef v1.ObjectReference `json:"jobTemplateRef"`

	// BackfillJobTemplateRef represents a reference to the job template.
	BackfillJobTemplateRef v1.ObjectReference `json:"backfillJobTemplateRef"`

	// SecretRef
	SecretRef v1.LocalObjectReference `json:"secretRef,omitempty"`
}

type MockStreamDefinitionStatus struct {
	// Phase represents the current phase of the stream.
	Phase string `json:"phase"`

	// ConfigurationHash represents the hash of the current configuration.
	ConfigurationHash string `json:"configurationHash"`
}

// MockStreamDefinition is a mock implementation of the StreamDefinition for testing purposes.
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:subresource:status
// +kubebuilder:object:root=true
type MockStreamDefinition struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MockStreamDefinitionSpec   `json:"spec,omitempty"`
	Status MockStreamDefinitionStatus `json:"status,omitempty"`
}

// MockStreamDefinitionList contains a list of MockStreamDefinition resources
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type MockStreamDefinitionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MockStreamDefinition `json:"items"`
}
