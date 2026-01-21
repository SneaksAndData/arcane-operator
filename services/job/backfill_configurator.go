package job

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"strconv"
)

var _ Configurator = &backfillConfigurator{}

// backfillConfigurator sets the backfill status in the job's environment variables and labels.
// It adds STREAMCONTEXT__BACKFILL environment variable and arcane/backfilling label.
type backfillConfigurator struct {
	value bool
}

func (f backfillConfigurator) ConfigureJob(job *batchv1.Job) error {
	found := false

	for k := range job.Spec.Template.Spec.Containers {
		for v := range job.Spec.Template.Spec.Containers[k].Env {
			if job.Spec.Template.Spec.Containers[k].Env[v].Name == "STREAMCONTEXT__BACKFILL" {
				job.Spec.Template.Spec.Containers[k].Env[v].Value = strconv.FormatBool(f.value)
				found = true
				break
			}
		}

		if !found {
			envVar := corev1.EnvVar{
				Name:  "STREAMCONTEXT__BACKFILL",
				Value: strconv.FormatBool(f.value),
			}
			job.Spec.Template.Spec.Containers[k].Env = append(job.Spec.Template.Spec.Containers[k].Env, envVar)
		}
		found = false
	}

	if job.Labels == nil {
		job.Labels = make(map[string]string)
	}
	job.Labels[BackfillLabel] = strconv.FormatBool(f.value)

	return nil
}

func NewBackfillConfigurator(value bool) Configurator {
	return &backfillConfigurator{
		value: value,
	}
}
