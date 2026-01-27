package job

import (
	"fmt"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

var _ Configurator = &secretReferenceConfigurator{}

type secretReferenceConfigurator struct {
	reference *corev1.LocalObjectReference
}

func (s secretReferenceConfigurator) ConfigureJob(job *batchv1.Job) error {
	if s.reference == nil {
		return fmt.Errorf("secretReferenceConfigurator reference is nil")
	}
	if s.reference.Name == "" {
		return fmt.Errorf("secretReferenceConfigurator reference name is empty")
	}
	if job.Spec.Template.Spec.Containers == nil || len(job.Spec.Template.Spec.Containers) == 0 {
		return nil
	}

	// Apply secret reference to all containers
	for i := range job.Spec.Template.Spec.Containers {
		if job.Spec.Template.Spec.Containers[i].EnvFrom == nil {
			job.Spec.Template.Spec.Containers[i].EnvFrom = []corev1.EnvFromSource{}
		}

		job.Spec.Template.Spec.Containers[i].EnvFrom = append(job.Spec.Template.Spec.Containers[i].EnvFrom, corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: *s.reference,
			},
		})
	}

	return nil
}

func NewSecretReferenceConfigurator(reference *corev1.LocalObjectReference) Configurator {
	return &secretReferenceConfigurator{
		reference: reference,
	}
}
