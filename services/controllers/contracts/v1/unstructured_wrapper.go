package v1

import (
	"errors"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/job"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

var (
	_ stream.Definition = (*ExecutionSettings)(nil)
	// _ job.ConfiguratorProvider    = (*ExecutionSettings)(nil)
	// _ job.SecretReferenceProvider = (*ExecutionSettings)(nil)
)

type executionSettingsSpec struct {
	ExecutionSettings struct {
		Suspended  bool   `json:"suspended"`
		APIVersion string `json:"apiVersion"`
	} `json:"execution"`
}

type ExecutionSettings struct {
	*StatusWrapper
	spec executionSettingsSpec
}

func NewExecutionSettings(u *unstructured.Unstructured) *ExecutionSettings {
	return &ExecutionSettings{
		StatusWrapper: &StatusWrapper{
			underlying: u,
		},
	}
}

func (e *ExecutionSettings) Suspended() bool {
	return e.spec.ExecutionSettings.Suspended
}

func (e *ExecutionSettings) SetSuspended(suspended bool) error {
	e.spec.ExecutionSettings.Suspended = suspended
	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&e.spec.ExecutionSettings)
	if err != nil {
		return err
	}
	spec, ok := e.underlying.Object["spec"].(map[string]interface{})
	if !ok {
		return errors.New("failed to convert spec to map[string]interface{}")
	}

	spec["execution"] = u
	return nil
}

func (e *ExecutionSettings) LastAppliedConfiguration() string {
	//TODO implement me
	panic("implement me")
}

func (e *ExecutionSettings) NamespacedName() types.NamespacedName {
	//TODO implement me
	panic("implement me")
}

func (e *ExecutionSettings) ToUnstructured() *unstructured.Unstructured {
	//TODO implement me
	panic("implement me")
}

func (e *ExecutionSettings) StateString() string {
	//TODO implement me
	panic("implement me")
}

func (e *ExecutionSettings) ToOwnerReference() metav1.OwnerReference {
	//TODO implement me
	panic("implement me")
}

func (e *ExecutionSettings) ToConfiguratorProvider() job.ConfiguratorProvider {
	//TODO implement me
	panic("implement me")
}

func (e *ExecutionSettings) GetJobTemplate(request *v1.BackfillRequest) types.NamespacedName {
	//TODO implement me
	panic("implement me")
}

func (e *ExecutionSettings) GetReferenceForSecret(name string) (*corev1.LocalObjectReference, error) {
	//TODO implement me
	panic("implement me")
}

func (e *ExecutionSettings) Validate() error {
	err := e.StatusWrapper.validate()
	if err != nil {
		return err
	}

	spec, ok := e.underlying.Object["spec"].(map[string]interface{})
	if !ok {
		return errors.New("failed to convert spec to map[string]interface{}")
	}

	execution, ok := spec["execution"].(map[string]interface{})
	if !ok {
		return errors.New("failed to convert execution to map[string]interface{}")
	}

	err = runtime.DefaultUnstructuredConverter.FromUnstructured(map[string]interface{}{"execution": execution}, &e.spec)
	if err != nil {
		return err
	}

	return nil
}
