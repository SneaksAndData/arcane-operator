package job

import (
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

func Test_BackfillStaticIdConfigurator_EnvVar_Set(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	configurator := NewBackfillStaticIdConfigurator("backfill-id-1")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	require.NotEmpty(t, job.Spec.Template.Spec.Containers[0].Env)

	found := false
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "STREAMCONTEXT__BACKFILL_ID" {
			require.Equal(t, "backfill-id-1", env.Value)
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__BACKFILL_ID environment variable should be set")
}

func Test_BackfillStaticIdConfigurator_EnvVar_Update_Existing(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{
		Name: "test-container",
		Env: []corev1.EnvVar{
			{Name: "STREAMCONTEXT__BACKFILL_ID", Value: "old-id"},
		},
	}}

	configurator := NewBackfillStaticIdConfigurator("new-id")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	require.NotEmpty(t, job.Spec.Template.Spec.Containers[0].Env)

	found := false
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "STREAMCONTEXT__BACKFILL_ID" {
			require.Equal(t, "new-id", env.Value)
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__BACKFILL_ID environment variable should be updated")
}

func Test_BackfillStaticIdConfigurator_EnvVar_Multiple_Containers(t *testing.T) {
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

	configurator := NewBackfillStaticIdConfigurator("backfill-id-2")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.Spec.Template.Spec.Containers, 2)

	for i, container := range job.Spec.Template.Spec.Containers {
		require.NotEmpty(t, container.Env, "Container %d should have environment variables", i)

		found := false
		for _, env := range container.Env {
			if env.Name == "STREAMCONTEXT__BACKFILL_ID" {
				require.Equal(t, "backfill-id-2", env.Value)
				found = true
				break
			}
		}
		require.True(t, found, "STREAMCONTEXT__BACKFILL_ID should be set in container %d", i)
	}
}

func Test_BackfillStaticIdConfigurator_EnvVar_Empty_Containers(t *testing.T) {
	job := &batchv1.Job{}

	configurator := NewBackfillStaticIdConfigurator("backfill-id-3")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Empty(t, job.Spec.Template.Spec.Containers)
}

func Test_BackfillStaticIdConfigurator_EnvVar_Empty_Value(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	configurator := NewBackfillStaticIdConfigurator("")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.NotEmpty(t, job.Spec.Template.Spec.Containers[0].Env)

	found := false
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "STREAMCONTEXT__BACKFILL_ID" {
			require.Equal(t, "", env.Value)
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__BACKFILL_ID should be set even when value is empty")
}
