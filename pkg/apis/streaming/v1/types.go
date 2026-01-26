package v1

import (
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Phase represents the current phase of the stream class
// +kubebuilder:validation:Enum=Pending;Ready;Failed;Stopped
type Phase string

const (
	PhaseNew     Phase = ""
	PhasePending Phase = "Pending"
	PhaseReady   Phase = "Ready"
	PhaseFailed  Phase = "Failed"
	PhaseStopped Phase = "Stopped"
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

	// TargetNamespace is the namespace where streaming jobs will be created
	TargetNamespace string `json:"namespace,omitempty"`
}

// StreamClassStatus defines the observed state of a stream class
type StreamClassStatus struct {
	// Phase represents the current phase of the stream class
	Phase Phase `json:"phase,omitempty"`

	// Conditions represent the latest available observations
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// StreamClass is the Schema for the stream class API
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=sc
// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:name="ApiGroupRef",type=string,JSONPath=`.spec.apiGroupRef`
// +kubebuilder:printcolumn:name="ApiVersion",type=string,JSONPath=`.spec.apiVersion`
// +kubebuilder:printcolumn:name="KindRef",type=string,JSONPath=`.spec.kindRef`
// +kubebuilder:printcolumn:name="PluralName",type=string,JSONPath=`.spec.pluralName`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
type StreamClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StreamClassSpec   `json:"spec,omitempty"`
	Status StreamClassStatus `json:"status,omitempty"`
}

// StreamClassList contains a list of StreamClass resources
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type StreamClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StreamClass `json:"items"`
}

// StreamingJobTemplate is a schema for streaming job templates
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:scope=Namespaced,shortName=sjt
// +kubebuilder:subresource:status
// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:name="MemoryLimit",type=string,JSONPath=`.spec.spec.template.spec.containers[0].resources.limits.memory`
// +kubebuilder:printcolumn:name="CpuLimit",type=string,JSONPath=`.spec.spec.template.spec.containers[0].resources.limits.cpu`
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.spec.spec.template.spec.containers[0].image`
type StreamingJobTemplate struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec batchv1.Job `json:"spec,omitempty"`
}

// StreamingJobTemplateList contains a list of Job resources
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type StreamingJobTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StreamingJobTemplate `json:"items"`
}

// BackfillRequestSpec defines the desired state of a backfill request
type BackfillRequestSpec struct {
	// StreamClass is the name of the stream class to backfill
	StreamClass string `json:"streamClass"`

	// StreamId is the ID of the stream to backfill
	StreamId string `json:"streamId"`

	// Completed indicates whether the backfill request has been completed
	Completed bool `json:"completed,omitempty"`
}

// BackfillRequestStatus defines the observed state of a backfill request
type BackfillRequestStatus struct {
	// Phase represents the current phase of the backfill request
	Phase Phase `json:"phase,omitempty"`

	// Conditions represent the latest available observations
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// BackfillRequest is the Schema for the backfill request API
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=bfr
// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:name="StreamClass",type=string,JSONPath=`.spec.streamClass`
// +kubebuilder:printcolumn:name="StreamId",type=string,JSONPath=`.spec.streamId`
type BackfillRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BackfillRequestSpec   `json:"spec,omitempty"`
	Status BackfillRequestStatus `json:"status,omitempty"`
}

// BackfillRequestList contains a list of BackfillRequest resources
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type BackfillRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BackfillRequest `json:"items"`
}
