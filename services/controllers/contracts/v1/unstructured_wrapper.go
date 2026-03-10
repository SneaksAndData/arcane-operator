package v1

import (
	"errors"
	"fmt"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/contracts/common"
	"github.com/SneaksAndData/arcane-operator/services/controllers/contracts/status_v0"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/job"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

var (
	_ stream.Definition           = (*ExecutionSettings)(nil)
	_ job.ConfiguratorProvider    = (*ExecutionSettings)(nil)
	_ job.SecretReferenceProvider = (*ExecutionSettings)(nil)
)

type ExecutionSettings struct {
	common.SecretReferenceReader
	*status_v0.StatusWrapper
	*common.ConfiguratorProvider
	*common.OwnerReferenceProvider
	Underlying *unstructured.Unstructured

	spec struct {
		ExecutionSettings struct {
			Suspended              bool                    `json:"suspended"`
			APIVersion             string                  `json:"apiVersion"`
			BackfillJobTemplateRef *corev1.ObjectReference `json:"backfillJobTemplateRef,omitempty"`
			StreamingBackend       struct {
				BatchJobBackend *struct {
					ChangeCaptureInterval string                 `json:"changeCaptureInterval"`
					JobTemplateRef        corev1.ObjectReference `json:"jobTemplateRef"`
				} `json:"realtime,omitempty"`
				CronJobBackend *struct {
					Schedule       string                 `json:"schedule"`
					JobTemplateRef corev1.ObjectReference `json:"jobTemplateRef"`
				} `json:"batch,omitempty"`
			} `json:"streamingBackend"`
		} `json:"execution"`
	}
}

func NewExecutionSettings(u *unstructured.Unstructured) *ExecutionSettings {
	w := &ExecutionSettings{
		Underlying: u,
	}
	w.SecretReferenceReader = common.NewSecretReferenceReader(u)
	w.ConfiguratorProvider = common.NewConfiguratorProvider(u, w)
	w.OwnerReferenceProvider = common.NewOwnerReferenceProvider(u)
	w.StatusWrapper = status_v0.NewStatusWrapper(u)
	return w
}

func (e *ExecutionSettings) Suspended() bool {
	return e.spec.ExecutionSettings.Suspended
}

func (e *ExecutionSettings) SetSuspended(suspended bool) error {
	e.spec.ExecutionSettings.Suspended = suspended
	err := e.deserializeTo(e.Underlying)
	if err != nil {
		return err
	}
	return nil
}

func (e *ExecutionSettings) NamespacedName() types.NamespacedName {
	return types.NamespacedName{
		Namespace: e.Underlying.GetNamespace(),
		Name:      e.Underlying.GetName(),
	}
}

func (e *ExecutionSettings) ToUnstructured() *unstructured.Unstructured {
	return e.Underlying
}

func (e *ExecutionSettings) StateString() string {
	phase := e.GetPhase()
	return fmt.Sprintf("phase=%s", phase)
}

func (e *ExecutionSettings) GetJobTemplate(request *v1.BackfillRequest) types.NamespacedName {
	if request != nil {
		return types.NamespacedName{
			Name:      e.spec.ExecutionSettings.BackfillJobTemplateRef.Name,
			Namespace: e.spec.ExecutionSettings.BackfillJobTemplateRef.Namespace,
		}
	}

	if e.spec.ExecutionSettings.StreamingBackend.BatchJobBackend != nil {
		return types.NamespacedName{
			Name:      e.spec.ExecutionSettings.StreamingBackend.BatchJobBackend.JobTemplateRef.Name,
			Namespace: e.spec.ExecutionSettings.StreamingBackend.BatchJobBackend.JobTemplateRef.Namespace,
		}
	}

	return types.NamespacedName{
		Name:      e.spec.ExecutionSettings.StreamingBackend.CronJobBackend.JobTemplateRef.Name,
		Namespace: e.spec.ExecutionSettings.StreamingBackend.CronJobBackend.JobTemplateRef.Namespace,
	}
}

func (e *ExecutionSettings) Validate() error {
	spec, ok := e.Underlying.Object["spec"].(map[string]interface{})
	if !ok {
		return errors.New("failed to convert spec to map[string]interface{}")
	}

	execution, ok := spec["execution"].(map[string]interface{})
	if !ok {
		return errors.New("failed to convert execution to map[string]interface{}")
	}

	err := runtime.DefaultUnstructuredConverter.FromUnstructured(map[string]interface{}{"execution": execution}, &e.spec)
	if err != nil {
		return err
	}

	err = e.ExtractPhase()
	if err != nil { // coverage-ignore
		return err
	}

	err = e.ExtractConfigurationHash()
	if err != nil { // coverage-ignore
		return err
	}

	return nil
}

func (e *ExecutionSettings) GetBackend() stream.Backend {
	if e.spec.ExecutionSettings.StreamingBackend.CronJobBackend != nil {
		return stream.CronJob
	}
	return stream.BatchJob
}

func (e *ExecutionSettings) deserializeTo(unstructured *unstructured.Unstructured) error {
	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&e.spec.ExecutionSettings)
	if err != nil {
		return err
	}

	spec, ok := unstructured.Object["spec"].(map[string]interface{})
	if !ok {
		return errors.New("failed to convert spec to map[string]interface{}")
	}
	spec["execution"] = u
	return nil
}
