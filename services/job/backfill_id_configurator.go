package job

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

var _ Configurator = &backfillStaticIdConfigurator{}

// backfillStaticIdConfigurator sets the backfill status in the job's environment variables and labels.
// It adds STREAMCONTEXT__BACKFILL_ID environment variable
type backfillStaticIdConfigurator struct {
	value string
}

func (f backfillStaticIdConfigurator) ConfigureJob(job *batchv1.Job) error {
	found := false

	for k := range job.Spec.Template.Spec.Containers {
		for v := range job.Spec.Template.Spec.Containers[k].Env {
			if job.Spec.Template.Spec.Containers[k].Env[v].Name == "STREAMCONTEXT__BACKFILL_ID" {
				job.Spec.Template.Spec.Containers[k].Env[v].Value = f.value
				found = true
				break
			}
		}

		if !found {
			envVar := corev1.EnvVar{
				Name:  "STREAMCONTEXT__BACKFILL_ID",
				Value: f.value,
			}
			job.Spec.Template.Spec.Containers[k].Env = append(job.Spec.Template.Spec.Containers[k].Env, envVar)
		}
		found = false
	}

	return nil
}

func NewBackfillStaticIdConfigurator(value string) Configurator {
	return &backfillStaticIdConfigurator{
		value: value,
	}
}
