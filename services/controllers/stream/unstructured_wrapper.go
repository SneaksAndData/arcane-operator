package stream

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

var (
	_ Definition                       = (*unstructuredWrapper)(nil)
	_ services.JobConfiguratorProvider = (*unstructuredWrapper)(nil)
)

type unstructuredWrapper struct {
	underlying      *unstructured.Unstructured
	phase           Phase
	suspended       bool
	configuration   string
	streamingJobRef corev1.ObjectReference
	backfillJobRef  corev1.ObjectReference
}

func (u *unstructuredWrapper) ToConfiguratorProvider() services.JobConfiguratorProvider {
	return u
}

func (u *unstructuredWrapper) GetPhase() Phase {
	return u.phase
}

func (u *unstructuredWrapper) Suspended() bool {
	return u.suspended
}

func (u *unstructuredWrapper) CurrentConfiguration(request *v1.BackfillRequest) (string, error) {
	spec, found, err := unstructured.NestedFieldCopy(u.underlying.Object, "spec")

	if err != nil {
		return "", err
	}

	if !found {
		return "", fmt.Errorf("spec field not found in unstructured object")
	}

	b, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}

	sum := md5.Sum(b)
	selfConfiguration := hex.EncodeToString(sum[:])

	if request == nil {
		return selfConfiguration, nil
	}

	// Include backfill request spec in the configuration hash
	bRequest, err := json.Marshal(request.Spec)
	if err != nil {
		return "", err
	}

	combinedSum := md5.Sum(bRequest)
	requestConfiguration := hex.EncodeToString(combinedSum[:])

	return fmt.Sprintf("%x:%x", selfConfiguration, requestConfiguration), nil
}

func (u *unstructuredWrapper) LastAppliedConfiguration() string {
	return u.configuration
}

func (u *unstructuredWrapper) RecomputeConfiguration(request *v1.BackfillRequest) error {
	currentConfig, err := u.CurrentConfiguration(request)
	if err != nil {
		return err
	}

	u.underlying.Object["status"].(map[string]interface{})["configurationHash"] = currentConfig
	u.configuration = currentConfig
	return nil
}

func (u *unstructuredWrapper) NamespacedName() types.NamespacedName {
	return types.NamespacedName{
		Name:      u.underlying.GetName(),
		Namespace: u.underlying.GetNamespace(),
	}
}

func (u *unstructuredWrapper) ToUnstructured() *unstructured.Unstructured {
	return u.underlying
}

func (u *unstructuredWrapper) SetPhase(phase Phase) error {
	return setNestedPhase(u.underlying, phase, "status", "phase")
}

func (u *unstructuredWrapper) StateString() string {
	phase := u.GetPhase()
	return fmt.Sprintf("phase=%s", phase)
}

func (u *unstructuredWrapper) GetStreamingJobName() types.NamespacedName {
	return types.NamespacedName{
		Name:      u.streamingJobRef.Name,
		Namespace: u.streamingJobRef.Namespace,
	}
}

func (u *unstructuredWrapper) GetBackfillJobName() types.NamespacedName {
	return types.NamespacedName{
		Name:      u.backfillJobRef.Name,
		Namespace: u.backfillJobRef.Namespace,
	}
}

func (u *unstructuredWrapper) ToOwnerReference() metav1.OwnerReference {
	ctrl := true
	return metav1.OwnerReference{
		APIVersion: u.underlying.GetAPIVersion(),
		Kind:       u.underlying.GetKind(),
		Name:       u.underlying.GetName(),
		UID:        u.underlying.GetUID(),
		Controller: &ctrl,
	}
}

func (u *unstructuredWrapper) JobConfigurator() services.JobConfigurator {
	return NewFromStreamDefinition(u)
}

func (u *unstructuredWrapper) Validate() error {
	err := u.extractPhase()
	if err != nil {
		return err
	}

	err = u.extractSuspended()
	if err != nil {
		return err
	}

	err = u.extractConfigurationHash()
	if err != nil {
		return err
	}

	err = u.extractStreamingJobRef("jobTemplateRef", &u.streamingJobRef)
	if err != nil {
		return err
	}

	err = u.extractStreamingJobRef("backfillJobTemplateRef", &u.backfillJobRef)
	if err != nil {
		return err
	}

	return nil
}

func (u *unstructuredWrapper) extractStreamingJobRef(from string, target *corev1.ObjectReference) error {
	uRef, found, err := unstructured.NestedFieldCopy(u.underlying.Object, "spec", from)
	if err != nil {
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

func (u *unstructuredWrapper) extractConfigurationHash() error {
	currentConfiguration, found, err := getNestedString(u.underlying, "status", "configurationHash")
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("status/configurationHash field not found in uRef object")
	}
	u.configuration = currentConfiguration
	return nil
}

func (u *unstructuredWrapper) extractSuspended() error {
	suspended, found, err := getNestedBool(u.underlying, "spec", "suspended")
	if err != nil {
		return err
	}

	if !found {
		return fmt.Errorf("spec/suspended field not found in uRef object")
	}
	u.suspended = suspended
	return nil
}

func (u *unstructuredWrapper) extractPhase() error {
	phase, found, err := getNestedString(u.underlying, "status", "phase")
	if err != nil {
		return err
	}
	if !found {
		u.phase = New
	} else {
		u.phase = Phase(phase)
	}
	return nil
}

// getNestedString reads a string at the given path (e.g. "status","phase").
func getNestedString(u *unstructured.Unstructured, path ...string) (string, bool, error) {
	return unstructured.NestedString(u.Object, path...)
}

func setNestedPhase(u *unstructured.Unstructured, value Phase, path ...string) error {
	return unstructured.SetNestedField(u.Object, string(value), path...)
}

func getNestedBool(u *unstructured.Unstructured, path ...string) (bool, bool, error) {
	return unstructured.NestedBool(u.Object, path...)
}
