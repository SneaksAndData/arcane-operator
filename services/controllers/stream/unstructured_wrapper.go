package stream

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/job"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

var (
	_ Definition                  = (*unstructuredWrapper)(nil)
	_ job.ConfiguratorProvider    = (*unstructuredWrapper)(nil)
	_ job.SecretReferenceProvider = (*unstructuredWrapper)(nil)
)

type unstructuredWrapper struct {
	underlying      *unstructured.Unstructured
	phase           Phase
	suspended       bool
	configuration   string
	streamingJobRef corev1.ObjectReference
	backfillJobRef  corev1.ObjectReference
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
	u.phase = phase
	return setNestedPhase(u.underlying, phase, "status", "phase")
}

func (u *unstructuredWrapper) SetSuspended(suspended bool) error {
	u.suspended = suspended
	return unstructured.SetNestedField(u.underlying.Object, suspended, "spec", "suspended")
}

func (u *unstructuredWrapper) StateString() string {
	phase := u.GetPhase()
	return fmt.Sprintf("phase=%s", phase)
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

func (u *unstructuredWrapper) ToConfiguratorProvider() job.ConfiguratorProvider {
	return u
}

func (u *unstructuredWrapper) GetJobTemplate(request *v1.BackfillRequest) types.NamespacedName {
	if request == nil {
		namespace := u.streamingJobRef.Namespace
		if namespace == "" {
			namespace = u.underlying.GetNamespace()
		}
		return types.NamespacedName{
			Name:      u.streamingJobRef.Name,
			Namespace: namespace,
		}
	} else {
		namespace := u.streamingJobRef.Namespace
		if namespace == "" {
			namespace = u.underlying.GetNamespace()
		}
		return types.NamespacedName{
			Name:      u.backfillJobRef.Name,
			Namespace: namespace,
		}
	}
}

func (u *unstructuredWrapper) SetConditions(conditions []metav1.Condition) error {
	// Convert conditions to []interface{} for unstructured
	conditionsSlice := make([]interface{}, len(conditions))
	for i, cond := range conditions {
		condMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&cond)
		if err != nil {
			return fmt.Errorf("failed to convert condition to unstructured: %w", err)
		}
		conditionsSlice[i] = condMap
	}

	return unstructured.SetNestedSlice(u.underlying.Object, conditionsSlice, "status", "conditions")
}

func (u *unstructuredWrapper) ComputeConditions(bfr *v1.BackfillRequest) []metav1.Condition {
	switch u.GetPhase() {
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

func (u *unstructuredWrapper) JobConfigurator() (job.Configurator, error) {
	configurator := job.NewConfiguratorChainBuilder().
		WithConfigurator(job.NewNameConfigurator(u.underlying.GetName())).
		WithConfigurator(job.NewNamespaceConfigurator(u.underlying.GetNamespace())).
		WithConfigurator(job.NewMetadataConfigurator(u.underlying.GetName(), u.underlying.GetKind())).
		WithConfigurator(job.NewBackfillConfigurator(false)).
		WithConfigurator(job.NewEnvironmentConfigurator(u.underlying.Object["spec"], "SPEC")).
		WithConfigurator(job.NewOwnerConfigurator(u.ToOwnerReference())).
		Build()
	return configurator, nil
}

func (u *unstructuredWrapper) GetReferenceForSecret(fieldName string) (*corev1.LocalObjectReference, error) {
	secretRef, found, err := unstructured.NestedFieldCopy(u.underlying.Object, "spec", fieldName)
	if err != nil || !found {
		return nil, fmt.Errorf("spec/%s field not found in object", fieldName)
	}

	m, ok := secretRef.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("spec/%s is not an object", fieldName)
	}

	var ref corev1.LocalObjectReference
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(m, &ref); err != nil {
		return nil, fmt.Errorf("failed to convert %s to LocalObjectReference: %w", fieldName, err)
	}

	return &ref, nil
}

func (u *unstructuredWrapper) UpdateUnderlyingObject(processor func(*unstructured.Unstructured) error) error { // coverage-ignore (trivial)
	return processor(u.underlying)
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
		u.configuration = ""
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
