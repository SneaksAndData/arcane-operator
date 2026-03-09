package common

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

type SecretReferenceReader struct {
	underlying *unstructured.Unstructured
}

func NewSecretReferenceReader(u *unstructured.Unstructured) SecretReferenceReader {
	return SecretReferenceReader{
		underlying: u,
	}
}

func (u *SecretReferenceReader) GetReferenceForSecret(fieldName string) (*corev1.LocalObjectReference, error) {
	secretRef, found, err := unstructured.NestedFieldCopy(u.underlying.Object, "spec", fieldName)
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
