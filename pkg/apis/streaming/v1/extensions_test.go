package v1

import (
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newTestJob() *batchv1.Job {
	return &batchv1.Job{
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "test-container"}},
				},
			},
		},
	}
}

func findEnvVar(job *batchv1.Job, name string) (corev1.EnvVar, bool) {
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == name {
			return env, true
		}
	}
	return corev1.EnvVar{}, false
}

func Test_BackfillRequest_JobConfigurator_DataBackfillType(t *testing.T) {
	request := &BackfillRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-backfill"},
		Spec: BackfillRequestSpec{
			BackfillType: BackfillTypeData,
		},
	}

	configurator, err := request.JobConfigurator()
	require.NoError(t, err)

	job := newTestJob()
	require.NoError(t, configurator.ConfigureJob(job))

	env, found := findEnvVar(job, "STREAMCONTEXT__RUNMODE")
	require.True(t, found)
	require.Equal(t, "backfill", env.Value)
}

func Test_BackfillRequest_JobConfigurator_DefaultBackfillType(t *testing.T) {
	request := &BackfillRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-backfill"},
	}

	configurator, err := request.JobConfigurator()
	require.NoError(t, err)

	job := newTestJob()
	require.NoError(t, configurator.ConfigureJob(job))

	env, found := findEnvVar(job, "STREAMCONTEXT__RUNMODE")
	require.True(t, found)
	require.Equal(t, "backfill", env.Value)
}

func Test_BackfillRequest_JobConfigurator_MigrationBackfillType(t *testing.T) {
	request := &BackfillRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "test-backfill"},
		Spec: BackfillRequestSpec{
			BackfillType: BackfillTypeMigration,
		},
	}

	configurator, err := request.JobConfigurator()
	require.NoError(t, err)

	job := newTestJob()
	require.NoError(t, configurator.ConfigureJob(job))

	env, found := findEnvVar(job, "STREAMCONTEXT__RUNMODE")
	require.True(t, found)
	require.Equal(t, "migrate", env.Value)
}

func Test_BackfillRequest_JobConfigurator_NilReceiver(t *testing.T) {
	var request *BackfillRequest

	configurator, err := request.JobConfigurator()
	require.NoError(t, err)

	job := newTestJob()
	require.NoError(t, configurator.ConfigureJob(job))

	env, found := findEnvVar(job, "STREAMCONTEXT__RUNMODE")
	require.True(t, found)
	require.Equal(t, "stream", env.Value)
}
