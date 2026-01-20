package job

import (
	"github.com/stretchr/testify/require"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
)

func Test_NameConfigurator_Set_Name(t *testing.T) {
	job := &batchv1.Job{}

	configurator := NewNameConfigurator("test-job-name")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Equal(t, "test-job-name", job.Name)
}

func Test_NameConfigurator_Update_Existing_Name(t *testing.T) {
	job := &batchv1.Job{}
	job.Name = "old-job-name"

	configurator := NewNameConfigurator("new-job-name")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Equal(t, "new-job-name", job.Name)
}

func Test_NameConfigurator_Empty_Name(t *testing.T) {
	job := &batchv1.Job{}

	configurator := NewNameConfigurator("")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Equal(t, "", job.Name)
}

func Test_NameConfigurator_Name_Does_Not_Affect_Other_Properties(t *testing.T) {
	job := &batchv1.Job{}
	job.Namespace = "test-namespace"
	job.Labels = map[string]string{
		"app": "test-app",
	}

	configurator := NewNameConfigurator("new-job")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Equal(t, "new-job", job.Name)
	require.Equal(t, "test-namespace", job.Namespace)
	require.Equal(t, "test-app", job.Labels["app"])
}

func Test_NameConfigurator_Multiple_Calls(t *testing.T) {
	job := &batchv1.Job{}

	configurator1 := NewNameConfigurator("first-name")
	err := configurator1.ConfigureJob(job)
	require.NoError(t, err)
	require.Equal(t, "first-name", job.Name)

	configurator2 := NewNameConfigurator("second-name")
	err = configurator2.ConfigureJob(job)
	require.NoError(t, err)
	require.Equal(t, "second-name", job.Name)
}

func Test_NameConfigurator_Generated_Name_Pattern(t *testing.T) {
	job := &batchv1.Job{}

	// Simulate a generated name pattern like stream-123-20060102-150405
	generatedName := "stream-abc-20260120-143000"
	configurator := NewNameConfigurator(generatedName)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Equal(t, generatedName, job.Name)
}
