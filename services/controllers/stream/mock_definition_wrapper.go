package stream

import (
	"context"
	"fmt"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	testv2 "github.com/SneaksAndData/arcane-operator/pkg/test/apis_test/streaming/v2"
	"github.com/SneaksAndData/arcane-operator/services/job"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ Definition = (*MockDefinitionWrapper)(nil)

type MockDefinitionWrapper struct {
	underlying      *testv2.MockStreamDefinition
	configuration   string
	streamingJobRef corev1.ObjectReference
	backfillJobRef  corev1.ObjectReference
}

// NewMockDefinitionWrapper creates a new MockDefinitionWrapper and validates the underlying mock definition.
func NewMockDefinitionWrapper(mock *testv2.MockStreamDefinition) (*MockDefinitionWrapper, error) {
	wrapper := &MockDefinitionWrapper{
		underlying: mock,
	}
	if err := wrapper.Validate(); err != nil {
		return nil, err
	}
	return wrapper, nil
}

func (m *MockDefinitionWrapper) GetPhase() Phase {
	return Phase(m.underlying.Status.Phase)
}

func (m *MockDefinitionWrapper) Suspended() bool {
	return m.underlying.Spec.ExecutionSettings.Suspended
}

func (m *MockDefinitionWrapper) CurrentConfiguration(request *v1.BackfillRequest) (string, error) {
	selfConfiguration := "0xdeadbeef"
	if request == nil {
		return selfConfiguration, nil
	}
	requestConfiguration := "0xdeadface"
	return fmt.Sprintf("%s:%s", selfConfiguration, requestConfiguration), nil
}

func (m *MockDefinitionWrapper) LastAppliedConfiguration() string {
	return m.configuration
}

func (m *MockDefinitionWrapper) RecomputeConfiguration(request *v1.BackfillRequest) error {
	currentConfig, err := m.CurrentConfiguration(request)
	if err != nil {
		return err
	}
	m.underlying.Status.ConfigurationHash = currentConfig
	m.configuration = currentConfig
	return nil
}

func (m *MockDefinitionWrapper) NamespacedName() types.NamespacedName {
	return types.NamespacedName{
		Name:      m.underlying.GetName(),
		Namespace: m.underlying.GetNamespace(),
	}
}

func (m *MockDefinitionWrapper) ToUnstructured() *unstructured.Unstructured {
	unstructuredObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(m.underlying)
	if err != nil {
		// Should not happen in practice, but return a minimal unstructured object
		return &unstructured.Unstructured{
			Object: map[string]interface{}{},
		}
	}
	return &unstructured.Unstructured{Object: unstructuredObj}
}

func (m *MockDefinitionWrapper) SetPhase(phase Phase) error {
	m.underlying.Status.Phase = string(phase)
	return nil
}

func (m *MockDefinitionWrapper) SetSuspended(suspended bool) error {
	m.underlying.Spec.ExecutionSettings.Suspended = suspended
	return nil
}

func (m *MockDefinitionWrapper) StateString() string {
	phase := m.GetPhase()
	return fmt.Sprintf("phase=%s", phase)
}

func (m *MockDefinitionWrapper) ToOwnerReference() metav1.OwnerReference {
	ctrl := true
	return metav1.OwnerReference{
		APIVersion: m.underlying.APIVersion,
		Kind:       m.underlying.Kind,
		Name:       m.underlying.GetName(),
		UID:        m.underlying.GetUID(),
		Controller: &ctrl,
	}
}

func (m *MockDefinitionWrapper) ToConfiguratorProvider() job.ConfiguratorProvider {
	return m
}

func (m *MockDefinitionWrapper) GetJobTemplate(request *v1.BackfillRequest) types.NamespacedName {
	if request == nil {
		namespace := m.streamingJobRef.Namespace
		if namespace == "" {
			namespace = m.underlying.GetNamespace()
		}
		return types.NamespacedName{
			Name:      m.streamingJobRef.Name,
			Namespace: namespace,
		}
	} else {
		namespace := m.backfillJobRef.Namespace
		if namespace == "" {
			namespace = m.underlying.GetNamespace()
		}
		return types.NamespacedName{
			Name:      m.backfillJobRef.Name,
			Namespace: namespace,
		}
	}
}

func (m *MockDefinitionWrapper) SetConditions(_ []metav1.Condition) error {
	// For mock objects, we could store conditions in a separate field or just accept them
	// For now, we'll accept them without storing (since MockStreamDefinition doesn't have a conditions field)
	return nil
}

func (m *MockDefinitionWrapper) ComputeConditions(bfr *v1.BackfillRequest) []metav1.Condition {
	switch m.GetPhase() {
	case Pending:
		return []metav1.Condition{
			{
				Type:    "Warning",
				Status:  metav1.ConditionTrue,
				Reason:  "StreamPending",
				Message: "The stream is pending and will start soon",
				LastTransitionTime: metav1.Time{
					Time: metav1.Now().Time,
				},
			},
		}
	case Backfilling:
		return []metav1.Condition{
			{
				Type:    "Ready",
				Status:  metav1.ConditionTrue,
				Reason:  "StreamBackfilling",
				Message: "The stream is currently backfilling data, request ID: " + bfr.Name,
				LastTransitionTime: metav1.Time{
					Time: metav1.Now().Time,
				},
			},
		}
	case Running:
		return []metav1.Condition{
			{
				Type:    "Ready",
				Status:  metav1.ConditionTrue,
				Reason:  "StreamRunning",
				Message: "The stream is currently running.",
				LastTransitionTime: metav1.Time{
					Time: metav1.Now().Time,
				},
			},
		}
	case Suspended:
		return []metav1.Condition{
			{
				Type:    "Warning",
				Status:  metav1.ConditionTrue,
				Reason:  "StreamSuspended",
				Message: "The stream is suspended.",
				LastTransitionTime: metav1.Time{
					Time: metav1.Now().Time,
				},
			},
		}
	case Failed:
		return []metav1.Condition{
			{
				Type:    "Error",
				Status:  metav1.ConditionTrue,
				Reason:  "StreamFailed",
				Message: "The stream has failed.",
				LastTransitionTime: metav1.Time{
					Time: metav1.Now().Time,
				},
			},
		}
	default:
		return []metav1.Condition{}
	}
}

func (m *MockDefinitionWrapper) GetReferenceForSecret(fieldName string) (*corev1.LocalObjectReference, error) {
	if fieldName == "secretRef" {
		return &m.underlying.Spec.SecretRef, nil
	}
	return nil, fmt.Errorf("secret field %s not found in mock definition", fieldName)
}

// ConfiguratorProvider implementation
func (m *MockDefinitionWrapper) JobConfigurator() (job.Configurator, error) {
	configurator := job.NewConfiguratorChainBuilder().
		WithConfigurator(job.NewNameConfigurator(m.underlying.GetName())).
		WithConfigurator(job.NewNamespaceConfigurator(m.underlying.GetNamespace())).
		WithConfigurator(job.NewMetadataConfigurator(m.underlying.GetName(), m.underlying.Kind)).
		WithConfigurator(job.NewBackfillConfigurator(false)).
		WithConfigurator(job.NewOwnerConfigurator(m.ToOwnerReference())).
		Build()
	return configurator, nil
}

// Validate validates the underlying mock definition and extracts job references.
func (m *MockDefinitionWrapper) Validate() error {
	if m.underlying == nil {
		return fmt.Errorf("underlying MockStreamDefinition is nil")
	}

	// Initialize configuration hash
	m.configuration = m.underlying.Status.ConfigurationHash

	return nil
}

func (m *MockDefinitionWrapper) GetBackend() Backend {
	return BatchJob
}

func (m *MockDefinitionWrapper) GetPreviousBackend(ctx context.Context, c client.Client) (*Backend, error) {
	var backend Backend

	var cronJob batchv1.CronJob
	err := c.Get(ctx, m.NamespacedName(), &cronJob)
	if err == nil {
		backend = CronJob
		return &backend, nil
	}
	if client.IgnoreNotFound(err) != nil {
		return nil, err
	}

	var j batchv1.Job
	err = c.Get(ctx, m.NamespacedName(), &j)
	if err == nil {
		backend = BatchJob
		return &backend, nil
	}
	if client.IgnoreNotFound(err) != nil {
		return nil, err
	}

	// If neither a CronJob nor a Job exists with the same name, we can assume that there was no previous backend.
	return nil, nil
}
