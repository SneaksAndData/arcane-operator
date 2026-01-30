package job

import (
	"fmt"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

var _ Configurator = &secretReferenceConfigurator{}

type secretReferenceConfigurator struct {
	referenceFieldName string
	streamDefinition   SecretReferenceProvider
}

func (s secretReferenceConfigurator) ConfigureJob(job *batchv1.Job) error {
	reference, err := s.streamDefinition.GetReferenceForSecret(s.referenceFieldName)
	if err != nil {
		return fmt.Errorf("error getting secret reference: %w", err)
	}
	if reference.Name == "" {
		return fmt.Errorf("secretReferenceConfigurator reference name is empty")
	}
	if len(job.Spec.Template.Spec.Containers) == 0 {
		return nil
	}

	// Apply secret reference to all containers
	for i := range job.Spec.Template.Spec.Containers {
		if job.Spec.Template.Spec.Containers[i].EnvFrom == nil {
			job.Spec.Template.Spec.Containers[i].EnvFrom = []corev1.EnvFromSource{}
		}

		job.Spec.Template.Spec.Containers[i].EnvFrom = append(job.Spec.Template.Spec.Containers[i].EnvFrom, corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: *reference,
			},
		})
	}

	return nil
}

func NewSecretReferenceConfigurator(referenceFieldName string, sd SecretReferenceProvider) Configurator {
	return &secretReferenceConfigurator{
		referenceFieldName: referenceFieldName,
		streamDefinition:   sd,
	}
}
