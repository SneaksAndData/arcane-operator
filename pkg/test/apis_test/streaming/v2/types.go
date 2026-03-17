//go: build test

package v2

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CronJobBackend represents the backend configuration for batch processing, including the cron schedule and a reference
// to the job template.
type CronJobBackend struct {

	// Schedule represents the cron schedule for batch processing.
	Schedule string `json:"schedule"`

	// JobTemplateRef represents a reference to the job template.
	JobTemplateRef v1.ObjectReference `json:"jobTemplateRef"`
}

// BatchJobBackend represents the backend configuration for real-time streaming, including the change capture interval
// and a reference to the job template.
type BatchJobBackend struct {
	// ChangeCaptureInterval represents the interval at which changes are captured for real-time processing.
	ChangeCaptureInterval string `json:"changeCaptureInterval"`

	// JobTemplateRef represents a reference to the job template.
	JobTemplateRef v1.ObjectReference `json:"jobTemplateRef"`
}

// StreamingBackend represents the backend configuration for streaming, including both real-time and batch processing options.
type StreamingBackend struct {
	// BatchJobBackend represents the backend configuration for real-time streaming.
	BatchJobBackend *BatchJobBackend `json:"changeCapture,omitempty"`

	// CronJobBackend represents the backend configuration for batch processing.
	CronJobBackend *CronJobBackend `json:"batch,omitempty"`
}

// ExecutionSettings represents the execution settings for a stream, including suspension status and backend configuration.
type ExecutionSettings struct {
	// APIVersion represents the API version of the execution settings.
	APIVersion string `json:"apiVersion"`

	// Suspended indicates whether the stream is suspended.
	Suspended bool `json:"suspended"`

	// BackfillJobTemplateRef represents a reference to the job template.
	BackfillJobTemplateRef v1.ObjectReference `json:"backfillJobTemplateRef"`

	// StreamingBackend represents the backend configuration for streaming.
	StreamingBackend StreamingBackend `json:"streamingBackend"`
}

// MockStreamDefinitionSpec is a mock implementation of the StreamDefinitionSpec for testing purposes.
type MockStreamDefinitionSpec struct {

	// Execution represents the execution settings of the stream.
	ExecutionSettings ExecutionSettings `json:"execution"`

	// Source represents the source of the stream.
	Source string `json:"source"`

	// Destination represents the destination of the stream.
	Destination string `json:"destination"`

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
