package job

import corev1 "k8s.io/api/core/v1"

type SecretReferenceProvider interface {
	GetReferenceForSecret(name string) (*corev1.LocalObjectReference, error)
}
