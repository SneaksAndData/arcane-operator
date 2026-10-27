package integration_tests

import (
	"testing"

	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/stretchr/testify/require"
	"k8s.io/api/batch/v1"
)

func validateBackfillInCronJob(t *testing.T, expected string) func(job stream.BackendResource) {
	return func(job stream.BackendResource) {
		_, ok := job.ToObject().(*v1.CronJob)
		require.True(t, ok, "Expected a CronJob resource for a job")

		envVars := job.ToObject().(*v1.CronJob).Spec.JobTemplate.Spec.Template.Spec.Containers[0].Env
		for _, envVar := range envVars {
			if envVar.Name == "STREAMCONTEXT__BACKFILL" {
				require.Equal(t, expected, envVar.Value)
			}
		}
	}
}

func validateBackfillId(t *testing.T) func(job stream.BackendResource) {
	return func(job stream.BackendResource) {
		var foundBackfillId bool
		_, ok := job.ToObject().(*v1.CronJob)
		require.True(t, ok, "Expected a CronJob resource for a job")

		envVars := job.ToObject().(*v1.CronJob).Spec.JobTemplate.Spec.Template.Spec.Containers[0].Env
		var backfillId string
		for _, envVar := range envVars {
			if envVar.Name == "STREAMCONTEXT__BACKFILL_ID" {
				backfillId = envVar.ValueFrom.FieldRef.FieldPath
				// The value should be set from the job's metadata.labels['job-name']
				require.Equal(t, "metadata.labels['job-name']", backfillId,
					"Expected STREAMCONTEXT__BACKFILL_ID to be sourced from job labels")
				foundBackfillId = true
			}
		}

		require.True(t, foundBackfillId, "Expected to find a backfill job name in the STREAMCONTEXT__BACKFILL_ID environment variable")
	}
}
