package helpers

import (
	"testing"
	"time"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	testv2 "github.com/SneaksAndData/arcane-operator/pkg/test/apis_test/streaming/v2"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend/job"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func AssertStreamDefinitionPhase(t *testing.T, k8sClient client.Client, name types.NamespacedName, phase stream.Phase) {
	sd := &testv2.MockStreamDefinition{}
	err := k8sClient.Get(t.Context(), name, sd)
	require.NoError(t, err)
	require.Equal(t, string(phase), sd.Status.Phase)
}

func AssertJobExists(t *testing.T, k8sClient client.Client, name types.NamespacedName) {
	newJob := &batchv1.Job{}
	err := k8sClient.Get(t.Context(), name, newJob)
	require.NoError(t, err)
}

func AssertCronJobExists(t *testing.T, k8sClient client.Client, name types.NamespacedName, additionalAssert func(*testing.T, *batchv1.CronJob)) {
	cj := &batchv1.CronJob{}
	err := k8sClient.Get(t.Context(), name, cj)
	require.NoError(t, err)
	if additionalAssert != nil {
		additionalAssert(t, cj)
	}
}

func AssertCronJobNotExists(t *testing.T, k8sClient client.Client, name types.NamespacedName) {
	newJob := &batchv1.CronJob{}
	err := k8sClient.Get(t.Context(), name, newJob)
	require.True(t, errors.IsNotFound(err))
}

func AssertJobNotExists(t *testing.T, k8sClient client.Client, name types.NamespacedName) {
	newJob := &batchv1.Job{}
	err := k8sClient.Get(t.Context(), name, newJob)
	require.True(t, errors.IsNotFound(err))
}

func AssertJobConfiguration(t *testing.T, k8sClient client.Client, name types.NamespacedName, expectedConfiguration string) {
	newJob := &batchv1.Job{}
	err := k8sClient.Get(t.Context(), name, newJob)
	require.NoError(t, err)

	j, err := job.FromResource(newJob)
	require.NoError(t, err)

	jobConfiguration, err := j.CurrentConfiguration()
	require.NoError(t, err)
	require.Equal(t, jobConfiguration, expectedConfiguration)
}

func AssertBackfillRequestNotCompleted(t *testing.T, k8sClient client.Client, objectName types.NamespacedName) {
	backfillRequest := &v1.BackfillRequest{}
	err := k8sClient.Get(t.Context(), types.NamespacedName{Name: "backfill1", Namespace: objectName.Namespace}, backfillRequest)
	require.NoError(t, err)
	require.False(t, backfillRequest.Spec.Completed)
}

func AssertBackfillRequestCompleted(t *testing.T, k8sClient client.Client, objectName types.NamespacedName) {
	backfillRequest := &v1.BackfillRequest{}
	err := k8sClient.Get(t.Context(), types.NamespacedName{Name: "backfill1", Namespace: objectName.Namespace}, backfillRequest)
	require.NoError(t, err)
	require.True(t, backfillRequest.Spec.Completed)
}

// AssertEventRecorded drains all events currently buffered in the FakeRecorder
// and runs the provided assertion against each one. Each event has the format
// "<type> <reason> <message>" as produced by record.FakeRecorder.
// If additionalAssert is nil, only asserts that at least one event was recorded.
func AssertEventRecorded(t *testing.T, recorder *record.FakeRecorder, _ types.NamespacedName, additionalAssert func(*testing.T, string)) {
	t.Helper()
	events := drainEvents(recorder)
	require.NotEmpty(t, events, "expected at least one event to be recorded")
	if additionalAssert == nil {
		return
	}
	for _, ev := range events {
		additionalAssert(t, ev)
	}
}

func drainEvents(recorder *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case ev, ok := <-recorder.Events:
			if !ok {
				return out
			}
			out = append(out, ev)
		case <-time.After(10 * time.Millisecond):
			return out
		}
	}
}
