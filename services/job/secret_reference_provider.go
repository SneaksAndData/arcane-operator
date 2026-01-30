package job

import corev1 "k8s.io/api/core/v1"

// SecretReferenceProvider defines an interface for types that can provide a reference to a Kubernetes Secret.
type SecretReferenceProvider interface {

	// GetReferenceForSecret retrieves a LocalObjectReference for the specified secret name.
	GetReferenceForSecret(name string) (*corev1.LocalObjectReference, error)
}
