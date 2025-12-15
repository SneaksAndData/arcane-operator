package v1

import (
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Phase represents the current phase of the stream class
// +kubebuilder:validation:Enum=INITIALIZING;READY;FAILED;STOPPED
type Phase string

const (
	PhaseInitializing Phase = "Initializing"
	PhaseReady        Phase = "Ready"
	PhaseFailed       Phase = "Failed"
	PhaseStopped      Phase = "Stopped"
)

// StreamClassSpec defines the desired state of a stream class to watch
type StreamClassSpec struct {

	// APIGroupRef is the api group of the stream class to watch for
	APIGroupRef string `json:"apiGroupRef,omitempty"`

	// APIVersion is the API version of the stream class to watch for
	APIVersion string `json:"apiVersion"`

	// KindRef is the kind of the stream class to watch for
	KindRef string `json:"kindRef"`

	// PluralName is the plural name of the stream class to watch for
	PluralName string `json:"pluralName"`

	// SecretRefs is a list of fields to be extracted from the secret
	SecretRefs []string `json:"secretRefs,omitempty"`
}

// StreamClassStatus defines the observed state of a stream class
type StreamClassStatus struct {
	// Phase represents the current phase of the stream class
	Phase Phase `json:"phase,omitempty"`

	// Conditions represent the latest available observations
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:object:root=true

// StreamClass is the Schema for the stream class API
type StreamClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StreamClassSpec   `json:"spec,omitempty"`
	Status StreamClassStatus `json:"status,omitempty"`
}

// StreamClassList contains a list of StreamClass resources
// +kubebuilder:object:root=true
type StreamClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StreamClass `json:"items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:subresource:status
// +kubebuilder:object:root=true

// StreamingJobTemplate is a schema for streaming job templates
type StreamingJobTemplate struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec batchv1.JobSpec `json:"spec,omitempty"`
}

// StreamingJobTemplateList contains a list of Job resources
// +kubebuilder:object:root=true
type StreamingJobTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StreamingJobTemplate `json:"items"`
}
