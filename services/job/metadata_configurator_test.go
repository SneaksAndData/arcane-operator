package job

import (
	"github.com/stretchr/testify/require"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

func Test_MetadataConfigurator_Labels_Set(t *testing.T) {
	job := &batchv1.Job{}

	configurator := NewMetadataConfigurator("stream-123", "StreamDefinition")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Contains(t, job.Labels, "arcane/stream-id")
	require.Equal(t, "stream-123", job.Labels["arcane/stream-id"])
	require.Contains(t, job.Labels, "arcane/stream-kind")
	require.Equal(t, "StreamDefinition", job.Labels["arcane/stream-kind"])
}

func Test_MetadataConfigurator_Labels_Nil_Map(t *testing.T) {
	job := &batchv1.Job{}
	job.Labels = nil

	configurator := NewMetadataConfigurator("stream-456", "TestStream")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Contains(t, job.Labels, "arcane/stream-id")
	require.Equal(t, "stream-456", job.Labels["arcane/stream-id"])
	require.Contains(t, job.Labels, "arcane/stream-kind")
	require.Equal(t, "TestStream", job.Labels["arcane/stream-kind"])
}

func Test_MetadataConfigurator_Labels_Update_Existing(t *testing.T) {
	job := &batchv1.Job{}
	job.Labels = map[string]string{
		"arcane/stream-id":   "old-stream",
		"arcane/stream-kind": "OldKind",
	}

	configurator := NewMetadataConfigurator("new-stream", "NewKind")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Equal(t, "new-stream", job.Labels["arcane/stream-id"])
	require.Equal(t, "NewKind", job.Labels["arcane/stream-kind"])
}

func Test_MetadataConfigurator_Labels_Preserve_Other(t *testing.T) {
	job := &batchv1.Job{}
	job.Labels = map[string]string{
		"other-label": "other-value",
		"custom-key":  "custom-value",
	}

	configurator := NewMetadataConfigurator("stream-789", "CustomStream")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Equal(t, "stream-789", job.Labels["arcane/stream-id"])
	require.Equal(t, "CustomStream", job.Labels["arcane/stream-kind"])
	require.Equal(t, "other-value", job.Labels["other-label"])
	require.Equal(t, "custom-value", job.Labels["custom-key"])
	require.Len(t, job.Labels, 4)
}

func Test_MetadataConfigurator_EnvVars_Single_Container(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	configurator := NewMetadataConfigurator("stream-abc", "StreamType")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	container := job.Spec.Template.Spec.Containers[0]
	require.NotEmpty(t, container.Env)

	// Check for STREAMCONTEXT__STREAM_ID
	foundStreamId := false
	foundStreamKind := false
	for _, env := range container.Env {
		if env.Name == "STREAMCONTEXT__STREAM_ID" {
			require.Equal(t, "stream-abc", env.Value)
			foundStreamId = true
		}
		if env.Name == "STREAMCONTEXT__STREAM_KIND" {
			require.Equal(t, "StreamType", env.Value)
			foundStreamKind = true
		}
	}
	require.True(t, foundStreamId, "STREAMCONTEXT__STREAM_ID should be set")
	require.True(t, foundStreamKind, "STREAMCONTEXT__STREAM_KIND should be set")
}

func Test_MetadataConfigurator_EnvVars_Multiple_Containers(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{
		{Name: "container-1"},
		{Name: "container-2"},
	}

	configurator := NewMetadataConfigurator("stream-multi", "MultiStream")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	// Verify env vars are set in all containers
	for i, container := range job.Spec.Template.Spec.Containers {
		require.NotEmpty(t, container.Env, "Container %d should have environment variables", i)

		foundStreamId := false
		foundStreamKind := false
		for _, env := range container.Env {
			if env.Name == "STREAMCONTEXT__STREAM_ID" {
				require.Equal(t, "stream-multi", env.Value)
				foundStreamId = true
			}
			if env.Name == "STREAMCONTEXT__STREAM_KIND" {
				require.Equal(t, "MultiStream", env.Value)
				foundStreamKind = true
			}
		}
		require.True(t, foundStreamId, "STREAMCONTEXT__STREAM_ID should be set in container %d", i)
		require.True(t, foundStreamKind, "STREAMCONTEXT__STREAM_KIND should be set in container %d", i)
	}
}

func Test_MetadataConfigurator_EnvVars_Duplicate_StreamId_Error(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{
		Name: "test-container",
		Env: []corev1.EnvVar{
			{Name: "STREAMCONTEXT__STREAM_ID", Value: "existing-stream"},
		},
	}}

	configurator := NewMetadataConfigurator("new-stream", "NewKind")
	err := configurator.ConfigureJob(job)
	require.Error(t, err)
	require.Contains(t, err.Error(), "STREAMCONTEXT__STREAM_ID already present")
}

func Test_MetadataConfigurator_EnvVars_Duplicate_StreamKind_Error(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{
		Name: "test-container",
		Env: []corev1.EnvVar{
			{Name: "STREAMCONTEXT__STREAM_KIND", Value: "existing-kind"},
		},
	}}

	configurator := NewMetadataConfigurator("stream-id", "NewKind")
	err := configurator.ConfigureJob(job)
	require.Error(t, err)
	require.Contains(t, err.Error(), "STREAMCONTEXT__STREAM_KIND already present")
}

func Test_MetadataConfigurator_EnvVars_Preserve_Other_EnvVars(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{
		Name: "test-container",
		Env: []corev1.EnvVar{
			{Name: "OTHER_VAR", Value: "other-value"},
			{Name: "CUSTOM_VAR", Value: "custom-value"},
		},
	}}

	configurator := NewMetadataConfigurator("stream-preserve", "PreserveStream")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	container := job.Spec.Template.Spec.Containers[0]

	// Verify original env vars are preserved
	foundOther := false
	foundCustom := false
	for _, env := range container.Env {
		if env.Name == "OTHER_VAR" {
			require.Equal(t, "other-value", env.Value)
			foundOther = true
		}
		if env.Name == "CUSTOM_VAR" {
			require.Equal(t, "custom-value", env.Value)
			foundCustom = true
		}
	}
	require.True(t, foundOther, "OTHER_VAR should be preserved")
	require.True(t, foundCustom, "CUSTOM_VAR should be preserved")
}

func Test_MetadataConfigurator_EmptyStreamId(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	configurator := NewMetadataConfigurator("", "StreamKind")
	err := configurator.ConfigureJob(job)
	require.Error(t, err)
	require.EqualError(t, err, "streamId cannot be empty")
	require.NotContains(t, job.Labels, "arcane/stream-id")
}

func Test_MetadataConfigurator_EmptyStreamKind(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	configurator := NewMetadataConfigurator("stream-id", "")
	err := configurator.ConfigureJob(job)
	require.Error(t, err)
	require.EqualError(t, err, "streamKind cannot be empty")
	require.NotContains(t, job.Labels, "arcane/stream-kind")
}

func Test_MetadataConfigurator_BothEmpty(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	configurator := NewMetadataConfigurator("", "")
	err := configurator.ConfigureJob(job)
	require.Error(t, err)
	require.EqualError(t, err, "streamId cannot be empty")
	require.NotContains(t, job.Labels, "arcane/stream-id")
	require.NotContains(t, job.Labels, "arcane/stream-kind")
}

func Test_MetadataConfigurator_NoContainers(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{}

	configurator := NewMetadataConfigurator("stream-no-containers", "NoContainers")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	// Labels should still be set
	require.Equal(t, "stream-no-containers", job.Labels["arcane/stream-id"])
	require.Equal(t, "NoContainers", job.Labels["arcane/stream-kind"])
}
