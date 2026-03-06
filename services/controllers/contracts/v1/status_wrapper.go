package v1

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

type StatusWrapper struct {
	phase         stream.Phase
	underlying    *unstructured.Unstructured
	configuration string
}

func (s *StatusWrapper) GetPhase() stream.Phase {
	return s.phase
}

func (s *StatusWrapper) SetPhase(phase stream.Phase) error {
	s.phase = phase
	return setNestedPhase(s.underlying, phase, "status", "phase")
}

func (u *StatusWrapper) RecomputeConfiguration(request *v1.BackfillRequest) error {
	currentConfig, err := u.CurrentConfiguration(request)
	if err != nil { // coverage-ignore
		return err
	}

	u.underlying.Object["status"].(map[string]interface{})["configurationHash"] = currentConfig
	u.configuration = currentConfig
	return nil
}

func (u *StatusWrapper) CurrentConfiguration(request *v1.BackfillRequest) (string, error) {
	spec, found, err := unstructured.NestedFieldCopy(u.underlying.Object, "spec")

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

func (u *StatusWrapper) LastAppliedConfiguration() string {
	return u.configuration
}

func (u *StatusWrapper) SetConditions(conditions []metav1.Condition) error {
	// Convert conditions to []interface{} for unstructured
	conditionsSlice := make([]interface{}, len(conditions))
	for i, cond := range conditions {
		condMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&cond)
		if err != nil { // coverage-ignore
			return fmt.Errorf("failed to convert condition to unstructured: %w", err)
		}
		conditionsSlice[i] = condMap
	}

	return unstructured.SetNestedSlice(u.underlying.Object, conditionsSlice, "status", "conditions")
}

func (u *StatusWrapper) ComputeConditions(bfr *v1.BackfillRequest) []metav1.Condition { // coverage-ignore
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

func (u *StatusWrapper) extractConfigurationHash() error {
	currentConfiguration, found, err := getNestedString(u.underlying, "status", "configurationHash")
	if err != nil { // coverage-ignore
		return err
	}
	if !found {
		u.configuration = ""
	}
	u.configuration = currentConfiguration
	return nil
}

func (u *StatusWrapper) extractPhase() error {
	phase, found, err := getNestedString(u.underlying, "status", "phase")
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

func (s *StatusWrapper) validate() error {
	err := s.extractPhase()
	if err != nil { // coverage-ignore
		return err
	}

	err = s.extractConfigurationHash()
	if err != nil { // coverage-ignore
		return err
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
