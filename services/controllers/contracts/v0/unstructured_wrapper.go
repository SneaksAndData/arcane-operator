package v0

import (
	"context"
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
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	_ stream.Definition           = (*UnstructuredWrapper)(nil)
	_ job.ConfiguratorProvider    = (*UnstructuredWrapper)(nil)
	_ job.SecretReferenceProvider = (*UnstructuredWrapper)(nil)
)

type UnstructuredWrapper struct {
	common.SecretReferenceReader
	*status_v0.StatusWrapper
	*common.ConfiguratorProvider
	*common.OwnerReferenceProvider

	Underlying      *unstructured.Unstructured
	suspended       bool
	streamingJobRef corev1.ObjectReference
	backfillJobRef  corev1.ObjectReference
}

// NewUnstructuredWrapper creates a new UnstructuredWrapper from the given unstructured object.
func NewUnstructuredWrapper(obj *unstructured.Unstructured) stream.Definition {
	w := &UnstructuredWrapper{
		Underlying: obj,
	}

	w.StatusWrapper = status_v0.NewStatusWrapper(obj)
	w.SecretReferenceReader = common.NewSecretReferenceReader(obj)
	w.ConfiguratorProvider = common.NewConfiguratorProvider(obj, w)
	w.OwnerReferenceProvider = common.NewOwnerReferenceProvider(obj)
	return w
}

func (u *UnstructuredWrapper) Suspended() bool {
	return u.suspended
}

func (u *UnstructuredWrapper) NamespacedName() types.NamespacedName {
	return types.NamespacedName{
		Name:      u.Underlying.GetName(),
		Namespace: u.Underlying.GetNamespace(),
	}
}

func (u *UnstructuredWrapper) ToUnstructured() *unstructured.Unstructured {
	return u.Underlying
}

func (u *UnstructuredWrapper) SetSuspended(suspended bool) error {
	u.suspended = suspended
	return unstructured.SetNestedField(u.Underlying.Object, suspended, "spec", "suspended")
}

func (u *UnstructuredWrapper) StateString() string {
	phase := u.GetPhase()
	return fmt.Sprintf("phase=%s", phase)
}

func (u *UnstructuredWrapper) GetJobTemplate(request *v1.BackfillRequest) types.NamespacedName {
	if request == nil {
		namespace := u.streamingJobRef.Namespace
		if namespace == "" { // coverage-ignore
			namespace = u.Underlying.GetNamespace()
		}
		return types.NamespacedName{
			Name:      u.streamingJobRef.Name,
			Namespace: namespace,
		}
	}
	namespace := u.streamingJobRef.Namespace
	if namespace == "" { // coverage-ignore
		namespace = u.Underlying.GetNamespace()
	}
	return types.NamespacedName{
		Name:      u.backfillJobRef.Name,
		Namespace: namespace,
	}
}

func (u *UnstructuredWrapper) Validate() error {
	err := u.ExtractPhase()
	if err != nil { // coverage-ignore
		return err
	}

	err = u.extractSuspended()
	if err != nil { // coverage-ignore
		return err
	}

	err = u.ExtractConfigurationHash()
	if err != nil { // coverage-ignore
		return err
	}

	err = u.extractStreamingJobRef("jobTemplateRef", &u.streamingJobRef)
	if err != nil { // coverage-ignore
		return err
	}

	err = u.extractStreamingJobRef("backfillJobTemplateRef", &u.backfillJobRef)
	if err != nil { // coverage-ignore
		return err
	}

	return nil
}

func (u *UnstructuredWrapper) GetBackend() stream.Backend {
	return stream.BatchJob
}

func (u *UnstructuredWrapper) GetPreviousBackend(_ context.Context, _ client.Client) (*stream.Backend, error) {
	b := stream.BatchJob
	return &b, nil
}

func (u *UnstructuredWrapper) extractStreamingJobRef(from string, target *corev1.ObjectReference) error {
	uRef, found, err := unstructured.NestedFieldCopy(u.Underlying.Object, "spec", from)
	if err != nil { // coverage-ignore
		return err
	}

	if !found {
		return fmt.Errorf("spec/jobTemplateRef field not found in object")
	}

	m, ok := uRef.(map[string]interface{})
	if !ok {
		return fmt.Errorf("spec/streamingJobRef is not an object")
	}

	var ref corev1.ObjectReference
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(m, &ref); err != nil {
		return fmt.Errorf("failed to convert streamingJobRef to ObjectReference: %w", err)
	}

	*target = ref
	return nil
}

func (u *UnstructuredWrapper) extractSuspended() error {
	suspended, found, err := getNestedBool(u.Underlying, "spec", "suspended")
	if err != nil { // coverage-ignore
		return err
	}

	if !found {
		return fmt.Errorf("spec/suspended field not found in uRef object")
	}
	u.suspended = suspended
	return nil
}

func getNestedBool(u *unstructured.Unstructured, path ...string) (bool, bool, error) {
	return unstructured.NestedBool(u.Object, path...)
}
