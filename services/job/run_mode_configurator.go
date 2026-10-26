package job

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	RunModeStream   = "Stream"
	RunModeBackfill = "Backfill"
	RunModeMigrate  = "Migrate"
)

var _ Configurator = &runModeConfigurator{}

// runModeConfigurator sets the run mode in the job's environment variables.
// It adds a STREAMCONTEXT__RUNMODE environment variable.
type runModeConfigurator struct {
	value string
}

func (f runModeConfigurator) ConfigureJob(job *batchv1.Job) error {
	found := false

	for k := range job.Spec.Template.Spec.Containers {
		for v := range job.Spec.Template.Spec.Containers[k].Env {
			if job.Spec.Template.Spec.Containers[k].Env[v].Name == "STREAMCONTEXT__RUNMODE" {
				job.Spec.Template.Spec.Containers[k].Env[v].Value = f.value
				found = true
				break
			}
		}

		if !found {
			envVar := corev1.EnvVar{
				Name:  "STREAMCONTEXT__RUNMODE",
				Value: f.value,
			}
			job.Spec.Template.Spec.Containers[k].Env = append(job.Spec.Template.Spec.Containers[k].Env, envVar)
		}
		found = false
	}

	return nil
}

func NewRunModeConfigurator(value string) Configurator {
	return &runModeConfigurator{
		value: value,
	}
}
