package job

import (
	"github.com/stretchr/testify/require"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

func Test_BackfillConfigurator_Labels_True(t *testing.T) {
	job := &batchv1.Job{}

	configurator := NewBackfillConfigurator(true)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Contains(t, job.Labels, "arcane/backfilling")
	require.Equal(t, "true", job.Labels["arcane/backfilling"])
}

func Test_BackfillConfigurator_Labels_False(t *testing.T) {
	job := &batchv1.Job{}
	configurator := NewBackfillConfigurator(false)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Contains(t, job.Labels, "arcane/backfilling")
	require.Equal(t, "false", job.Labels["arcane/backfilling"])
}

func Test_BackfillConfigurator_Labels_Not_Empty(t *testing.T) {
	job := &batchv1.Job{}
	job.Labels["arcane/backfilling"] = "SomeValue"
	configurator := NewBackfillConfigurator(false)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Contains(t, job.Labels, "arcane/backfilling")
	require.Equal(t, "false", job.Labels["arcane/backfilling"])
}

func Test_BackfillConfigurator_Labels_Nil_Map(t *testing.T) {
	job := &batchv1.Job{}
	job.Labels = nil
	configurator := NewBackfillConfigurator(true)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Contains(t, job.Labels, "arcane/backfilling")
	require.Equal(t, "true", job.Labels["arcane/backfilling"])
}

func Test_BackfillConfigurator_EnvVar_True(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	configurator := NewBackfillConfigurator(true)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	require.NotEmpty(t, job.Spec.Template.Spec.Containers[0].Env)

	found := false
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "STREAMCONTEXT__BACKFILL" {
			require.Equal(t, "true", env.Value)
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__BACKFILL environment variable should be set")
}

func Test_BackfillConfigurator_EnvVar_False(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	configurator := NewBackfillConfigurator(false)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	require.NotEmpty(t, job.Spec.Template.Spec.Containers[0].Env)

	found := false
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "STREAMCONTEXT__BACKFILL" {
			require.Equal(t, "false", env.Value)
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__BACKFILL environment variable should be set")
}

func Test_BackfillConfigurator_EnvVar_Update_Existing(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{
		Name: "test-container",
		Env: []corev1.EnvVar{
			{Name: "STREAMCONTEXT__BACKFILL", Value: "SomeValue"},
		},
	}}

	configurator := NewBackfillConfigurator(false)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	require.NotEmpty(t, job.Spec.Template.Spec.Containers[0].Env)

	found := false
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "STREAMCONTEXT__BACKFILL" {
			require.Equal(t, "false", env.Value)
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__BACKFILL environment variable should be updated")
}

func Test_BackfillConfigurator_EnvVar_Multiple_Containers(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{
		{Name: "container-1"},
		{Name: "container-2"},
	}

	configurator := NewBackfillConfigurator(true)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.Spec.Template.Spec.Containers, 2)

	// Verify env var is set in all containers
	for i, container := range job.Spec.Template.Spec.Containers {
		require.NotEmpty(t, container.Env, "Container %d should have environment variables", i)

		found := false
		for _, env := range container.Env {
			if env.Name == "STREAMCONTEXT__BACKFILL" {
				require.Equal(t, "true", env.Value)
				found = true
				break
			}
		}
		require.True(t, found, "STREAMCONTEXT__BACKFILL should be set in container %d", i)
	}
}
