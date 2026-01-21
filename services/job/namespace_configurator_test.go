package job

import (
	"github.com/stretchr/testify/require"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
)

func Test_NamespaceConfigurator_Set_Namespace(t *testing.T) {
	job := &batchv1.Job{}

	configurator := NewNamespaceConfigurator("test-namespace")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Equal(t, "test-namespace", job.Namespace)
}

func Test_NamespaceConfigurator_Update_Existing_Namespace(t *testing.T) {
	job := &batchv1.Job{}
	job.Namespace = "old-namespace"

	configurator := NewNamespaceConfigurator("new-namespace")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Equal(t, "new-namespace", job.Namespace)
}

func Test_NamespaceConfigurator_Empty_Namespace(t *testing.T) {
	job := &batchv1.Job{}

	configurator := NewNamespaceConfigurator("")
	err := configurator.ConfigureJob(job)
	require.Error(t, err)
	require.EqualError(t, err, "namespace cannot be empty")
	require.Equal(t, "", job.Namespace)
}

func Test_NamespaceConfigurator_Does_Not_Affect_Other_Properties(t *testing.T) {
	job := &batchv1.Job{}
	job.Name = "test-job"
	job.Labels = map[string]string{
		"app": "test-app",
	}

	configurator := NewNamespaceConfigurator("test-namespace")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Equal(t, "test-namespace", job.Namespace)
	require.Equal(t, "test-job", job.Name)
	require.Equal(t, "test-app", job.Labels["app"])
}
