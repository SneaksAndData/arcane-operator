package job

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

var _ Configurator = &backfillDynamicIdConfigurator{}

// backfillDynamicIdConfigurator sets the backfill status in the job's environment variables and labels.
// It adds STREAMCONTEXT__BACKFILL_ID environment variable
type backfillDynamicIdConfigurator struct {
}

func (f backfillDynamicIdConfigurator) ConfigureJob(job *batchv1.Job) error {
	found := false

	for k := range job.Spec.Template.Spec.Containers {
		for v := range job.Spec.Template.Spec.Containers[k].Env {
			if job.Spec.Template.Spec.Containers[k].Env[v].Name == "STREAMCONTEXT__BACKFILL_ID" {
				job.Spec.Template.Spec.Containers[k].Env[v].ValueFrom = &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.labels['job-name']",
					},
				}
				found = true
				break
			}
		}

		if !found {
			envVar := corev1.EnvVar{
				Name: "STREAMCONTEXT__BACKFILL_ID",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.labels['job-name']",
					},
				},
			}
			job.Spec.Template.Spec.Containers[k].Env = append(job.Spec.Template.Spec.Containers[k].Env, envVar)
		}
		found = false
	}

	return nil
}

func NewBackfillDynamicIdConfigurator() Configurator {
	return &backfillDynamicIdConfigurator{}
}
