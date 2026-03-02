package v0

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"

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
	_ stream.Definition           = (*UnstructuredWrapper)(nil)
	_ job.ConfiguratorProvider    = (*UnstructuredWrapper)(nil)
	_ job.SecretReferenceProvider = (*UnstructuredWrapper)(nil)
)

type UnstructuredWrapper struct {
	Underlying      *unstructured.Unstructured
	phase           stream.Phase
	suspended       bool
	configuration   string
	streamingJobRef corev1.ObjectReference
	backfillJobRef  corev1.ObjectReference
}

func (u *UnstructuredWrapper) GetPhase() stream.Phase {
	return u.phase
}

func (u *UnstructuredWrapper) Suspended() bool {
	return u.suspended
}

func (u *UnstructuredWrapper) CurrentConfiguration(request *v1.BackfillRequest) (string, error) {
	spec, found, err := unstructured.NestedFieldCopy(u.Underlying.Object, "spec")

	if err != nil { // coverage-ignore
		return "", err
	}

	if !found { // coverage-ignore
		return "", fmt.Errorf("spec field not found in unstructured object")
	}

	b, err := json.Marshal(spec)
	if err != nil { // coverage-ignore
		return "", err
	}

	sum := md5.Sum(b)
	selfConfiguration := hex.EncodeToString(sum[:])

	if request == nil {
		return selfConfiguration, nil
	}

	// Include backfill request spec in the configuration hash
	bRequest, err := json.Marshal(request.Spec)
	if err != nil { // coverage-ignore
		return "", err
	}

	combinedSum := md5.Sum(bRequest)
	requestConfiguration := hex.EncodeToString(combinedSum[:])

	return fmt.Sprintf("%x:%x", selfConfiguration, requestConfiguration), nil
}

func (u *UnstructuredWrapper) LastAppliedConfiguration() string {
	return u.configuration
}

func (u *UnstructuredWrapper) RecomputeConfiguration(request *v1.BackfillRequest) error {
	currentConfig, err := u.CurrentConfiguration(request)
	if err != nil { // coverage-ignore
		return err
	}

	u.Underlying.Object["status"].(map[string]interface{})["configurationHash"] = currentConfig
	u.configuration = currentConfig
	return nil
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

func (u *UnstructuredWrapper) SetPhase(phase stream.Phase) error {
	u.phase = phase
	return setNestedPhase(u.Underlying, phase, "status", "phase")
}

func (u *UnstructuredWrapper) SetSuspended(suspended bool) error {
	u.suspended = suspended
	return unstructured.SetNestedField(u.Underlying.Object, suspended, "spec", "suspended")
}

func (u *UnstructuredWrapper) StateString() string {
	phase := u.GetPhase()
	return fmt.Sprintf("phase=%s", phase)
}

func (u *UnstructuredWrapper) ToOwnerReference() metav1.OwnerReference {
	ctrl := true
	return metav1.OwnerReference{
		APIVersion: u.Underlying.GetAPIVersion(),
		Kind:       u.Underlying.GetKind(),
		Name:       u.Underlying.GetName(),
		UID:        u.Underlying.GetUID(),
		Controller: &ctrl,
	}
}

func (u *UnstructuredWrapper) ToConfiguratorProvider() job.ConfiguratorProvider { // coverage-ignore
	return u
}

func (u *UnstructuredWrapper) GetJobTemplate(request *v1.BackfillRequest) types.NamespacedName {
	if request == nil {
		namespace := u.streamingJobRef.Namespace
		if namespace == "" {
			namespace = u.Underlying.GetNamespace()
		}
		return types.NamespacedName{
			Name:      u.streamingJobRef.Name,
			Namespace: namespace,
		}
	}
	namespace := u.streamingJobRef.Namespace
	if namespace == "" {
		namespace = u.Underlying.GetNamespace()
	}
	return types.NamespacedName{
		Name:      u.backfillJobRef.Name,
		Namespace: namespace,
	}
}

func (u *UnstructuredWrapper) SetConditions(conditions []metav1.Condition) error {
	// Convert conditions to []interface{} for unstructured
	conditionsSlice := make([]interface{}, len(conditions))
	for i, cond := range conditions {
		condMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&cond)
		if err != nil { // coverage-ignore
			return fmt.Errorf("failed to convert condition to unstructured: %w", err)
		}
		conditionsSlice[i] = condMap
	}

	return unstructured.SetNestedSlice(u.Underlying.Object, conditionsSlice, "status", "conditions")
}

func (u *UnstructuredWrapper) ComputeConditions(bfr *v1.BackfillRequest) []metav1.Condition { // coverage-ignore
	switch u.GetPhase() {
	case stream.Pending:
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
	case stream.Backfilling:
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
	case stream.Running:
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
	case stream.Suspended:
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
	case stream.Failed:
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

func (u *UnstructuredWrapper) JobConfigurator() (job.Configurator, error) {
	configurator := job.NewConfiguratorChainBuilder().
		WithConfigurator(job.NewNameConfigurator(u.Underlying.GetName())).
		WithConfigurator(job.NewNamespaceConfigurator(u.Underlying.GetNamespace())).
		WithConfigurator(job.NewMetadataConfigurator(u.Underlying.GetName(), u.Underlying.GetKind())).
		WithConfigurator(job.NewBackfillConfigurator(false)).
		WithConfigurator(job.NewEnvironmentConfigurator(u.Underlying.Object["spec"], "SPEC")).
		WithConfigurator(job.NewOwnerConfigurator(u.ToOwnerReference())).
		Build()
	return configurator, nil
}

func (u *UnstructuredWrapper) GetReferenceForSecret(fieldName string) (*corev1.LocalObjectReference, error) {
	secretRef, found, err := unstructured.NestedFieldCopy(u.Underlying.Object, "spec", fieldName)
	if err != nil || !found { // coverage-ignore
		return nil, fmt.Errorf("spec/%s field not found in object", fieldName)
	}

	m, ok := secretRef.(map[string]interface{})
	if !ok { // coverage-ignore
		return nil, fmt.Errorf("spec/%s is not an object", fieldName)
	}

	var ref corev1.LocalObjectReference
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(m, &ref); err != nil {
		return nil, fmt.Errorf("failed to convert %s to LocalObjectReference: %w", fieldName, err)
	}

	return &ref, nil
}

func (u *UnstructuredWrapper) Validate() error {
	err := u.extractPhase()
	if err != nil { // coverage-ignore
		return err
	}

	err = u.extractSuspended()
	if err != nil { // coverage-ignore
		return err
	}

	err = u.extractConfigurationHash()
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

func (u *UnstructuredWrapper) extractConfigurationHash() error {
	currentConfiguration, found, err := getNestedString(u.Underlying, "status", "configurationHash")
	if err != nil { // coverage-ignore
		return err
	}
	if !found {
		u.configuration = ""
	}
	u.configuration = currentConfiguration
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

func (u *UnstructuredWrapper) extractPhase() error {
	phase, found, err := getNestedString(u.Underlying, "status", "phase")
	if err != nil { // coverage-ignore
		return err
	}
	if !found {
		u.phase = stream.New
	} else {
		u.phase = stream.Phase(phase)
	}
	return nil
}

// getNestedString reads a string at the given path (e.g. "status","phase").
func getNestedString(u *unstructured.Unstructured, path ...string) (string, bool, error) {
	return unstructured.NestedString(u.Object, path...)
}

func setNestedPhase(u *unstructured.Unstructured, value stream.Phase, path ...string) error {
	return unstructured.SetNestedField(u.Object, string(value), path...)
}

func getNestedBool(u *unstructured.Unstructured, path ...string) (bool, bool, error) {
	return unstructured.NestedBool(u.Object, path...)
}
