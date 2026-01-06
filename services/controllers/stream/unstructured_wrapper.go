package stream

import (
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

var _ Definition = (*unstructuredWrapper)(nil)

type unstructuredWrapper struct {
	underlying *unstructured.Unstructured
	phase      Phase
	suspended  bool
}

func (u *unstructuredWrapper) GetPhase() Phase {
	return u.phase
}

func (u *unstructuredWrapper) Suspended() bool {
	return u.suspended
}

func (u *unstructuredWrapper) CurrentConfiguration() string {
	//TODO implement me
	panic("implement me")
}

func (u *unstructuredWrapper) LastObservedConfiguration() string {
	//TODO implement me
	panic("implement me")
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

func (u *unstructuredWrapper) SetStatus(phase Phase) error {
	return setNestedPhase(u.underlying, phase, "status", "phase")
}

func (u *unstructuredWrapper) StateString() string {
	//TODO implement me
	panic("implement me")
}

func (u *unstructuredWrapper) GetStreamingJobName() (types.NamespacedName, error) {
	//TODO implement me
	panic("implement me")
}

func (u *unstructuredWrapper) GetBackfillJobName() (types.NamespacedName, error) {
	//TODO implement me
	panic("implement me")
}

func (u *unstructuredWrapper) ToOwnerReference() (metav1.OwnerReference, error) {
	//TODO implement me
	panic("implement me")
}

func (u *unstructuredWrapper) JobConfigurator() JobConfigurator {
	//TODO implement me
	panic("implement me")
}

func (u *unstructuredWrapper) Validate() error {
	phase, found, err := getNestedString(u.underlying, "status", "phase")
	if err != nil {
		return err
	}
	if !found {
		u.phase = New
	} else {
		u.phase = Phase(phase)
	}

	suspended, found, err := getNestedBool(u.underlying, "spec", "suspend")
	if err != nil {
		return err
	}

	if !found {
		return fmt.Errorf("spec/suspend field not found in unstructured object")
	}

	u.suspended = suspended

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

func setNestedBool(u *unstructured.Unstructured, value bool, path ...string) error {
	return unstructured.SetNestedField(u.Object, value, path...)
}
