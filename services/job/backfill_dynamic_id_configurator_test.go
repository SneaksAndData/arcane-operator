package job

import (
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

func Test_BackfillDynamicIdConfigurator_EnvVar_Set(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	configurator := NewBackfillDynamicIdConfigurator()
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	require.NotEmpty(t, job.Spec.Template.Spec.Containers[0].Env)

	env, found := findBackfillIdEnv(job.Spec.Template.Spec.Containers[0].Env)
	require.True(t, found, "STREAMCONTEXT__BACKFILL_ID environment variable should be set")
	require.NotNil(t, env.ValueFrom)
	require.NotNil(t, env.ValueFrom.FieldRef)
	require.Equal(t, "metadata.labels['job-name']", env.ValueFrom.FieldRef.FieldPath)
}

func Test_BackfillDynamicIdConfigurator_EnvVar_Update_Existing(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{
		Name: "test-container",
		Env: []corev1.EnvVar{
			{Name: "STREAMCONTEXT__BACKFILL_ID", Value: "old-id"},
		},
	}}

	configurator := NewBackfillDynamicIdConfigurator()
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	require.Len(t, job.Spec.Template.Spec.Containers[0].Env, 1)

	env, found := findBackfillIdEnv(job.Spec.Template.Spec.Containers[0].Env)
	require.True(t, found, "STREAMCONTEXT__BACKFILL_ID environment variable should be updated")
	require.NotNil(t, env.ValueFrom)
	require.NotNil(t, env.ValueFrom.FieldRef)
	require.Equal(t, "metadata.labels['job-name']", env.ValueFrom.FieldRef.FieldPath)
}

func Test_BackfillDynamicIdConfigurator_EnvVar_Multiple_Containers(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{
		{Name: "container-1"},
		{
			Name: "container-2",
			Env: []corev1.EnvVar{
				{Name: "STREAMCONTEXT__BACKFILL_ID", Value: "old-id"},
			},
		},
	}

	configurator := NewBackfillDynamicIdConfigurator()
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.Spec.Template.Spec.Containers, 2)

	for i, container := range job.Spec.Template.Spec.Containers {
		require.NotEmpty(t, container.Env, "Container %d should have environment variables", i)

		env, found := findBackfillIdEnv(container.Env)
		require.True(t, found, "STREAMCONTEXT__BACKFILL_ID should be set in container %d", i)
		require.NotNil(t, env.ValueFrom)
		require.NotNil(t, env.ValueFrom.FieldRef)
		require.Equal(t, "metadata.labels['job-name']", env.ValueFrom.FieldRef.FieldPath)
	}
}

func Test_BackfillDynamicIdConfigurator_EnvVar_Empty_Containers(t *testing.T) {
	job := &batchv1.Job{}

	configurator := NewBackfillDynamicIdConfigurator()
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Empty(t, job.Spec.Template.Spec.Containers)
}

func findBackfillIdEnv(envs []corev1.EnvVar) (corev1.EnvVar, bool) {
	for _, env := range envs {
		if env.Name == "STREAMCONTEXT__BACKFILL_ID" {
			return env, true
		}
	}

	return corev1.EnvVar{}, false
}
