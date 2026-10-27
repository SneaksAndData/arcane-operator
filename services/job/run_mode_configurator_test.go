package job

import (
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

func Test_RunModeConfigurator_EnvVar_Stream(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	configurator := NewRunModeConfigurator(RunModeStream)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	require.NotEmpty(t, job.Spec.Template.Spec.Containers[0].Env)

	found := false
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "STREAMCONTEXT__RUNMODE" {
			require.Equal(t, RunModeStream, env.Value)
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__RUNMODE environment variable should be set")
}

func Test_RunModeConfigurator_EnvVar_Backfill(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	configurator := NewRunModeConfigurator(RunModeBackfill)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	require.NotEmpty(t, job.Spec.Template.Spec.Containers[0].Env)

	found := false
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "STREAMCONTEXT__RUNMODE" {
			require.Equal(t, RunModeBackfill, env.Value)
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__RUNMODE environment variable should be set")
}

func Test_RunModeConfigurator_EnvVar_Migrate(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	configurator := NewRunModeConfigurator(RunModeMigrate)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	require.NotEmpty(t, job.Spec.Template.Spec.Containers[0].Env)

	found := false
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "STREAMCONTEXT__RUNMODE" {
			require.Equal(t, RunModeMigrate, env.Value)
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__RUNMODE environment variable should be set")
}

func Test_RunModeConfigurator_EnvVar_Update_Existing(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{
		Name: "test-container",
		Env: []corev1.EnvVar{{
			Name:  "STREAMCONTEXT__RUNMODE",
			Value: "Legacy",
		}},
	}}

	configurator := NewRunModeConfigurator(RunModeBackfill)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	found := false
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "STREAMCONTEXT__RUNMODE" {
			require.Equal(t, RunModeBackfill, env.Value)
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__RUNMODE environment variable should be updated")
}

func Test_RunModeConfigurator_EnvVar_Multiple_Containers(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{
		{Name: "container-1"},
		{Name: "container-2"},
	}

	configurator := NewRunModeConfigurator(RunModeStream)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.Spec.Template.Spec.Containers, 2)

	for i, container := range job.Spec.Template.Spec.Containers {
		require.NotEmpty(t, container.Env, "Container %d should have environment variables", i)

		found := false
		for _, env := range container.Env {
			if env.Name == "STREAMCONTEXT__RUNMODE" {
				require.Equal(t, RunModeStream, env.Value)
				found = true
				break
			}
		}
		require.True(t, found, "STREAMCONTEXT__RUNMODE should be set in container %d", i)
	}
}
