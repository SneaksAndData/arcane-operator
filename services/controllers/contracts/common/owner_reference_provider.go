package common

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type OwnerReferenceProvider struct {
	underlying *unstructured.Unstructured
}

func NewOwnerReferenceProvider(underlying *unstructured.Unstructured) *OwnerReferenceProvider {
	return &OwnerReferenceProvider{
		underlying: underlying,
	}
}

func (e *OwnerReferenceProvider) ToOwnerReference() metav1.OwnerReference {
	ctrl := true
	return metav1.OwnerReference{
		APIVersion: e.underlying.GetAPIVersion(),
		Kind:       e.underlying.GetKind(),
		Name:       e.underlying.GetName(),
		UID:        e.underlying.GetUID(),
		Controller: &ctrl,
	}
}
