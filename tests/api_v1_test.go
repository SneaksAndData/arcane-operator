package integration_tests

import (
	"context"
	"testing"
	"time"

	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-stream-mock/pkg/apis/streaming/v2"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
)

// Test_CreateStream verifies that creating a TestStreamDefinition results in the creation of both backfill and regular streaming jobs.
// It watches for Job events in the Kubernetes cluster and checks that at least one backfill job and one regular job are created and completed.
func Test_StreamStateTransitionToScheduled(t *testing.T) {
	// Arrange

	name := configureV2StreamDefinition(t, func(definition *v2.TestStreamDefinitionV2) {
		definition.Spec.ExecutionSettings.Suspended = true
		definition.Spec.ExecutionSettings.APIVersion = "v1"
		definition.Spec.ExecutionSettings.StreamingBackend.BatchJobBackend = &v2.BatchJobBackend{
			JobTemplateRef: corev1.ObjectReference{
				APIVersion: "streaming.sneaksanddata.com/v1",
				Kind:       "StreamingJobTemplate",
				Name:       "arcane-stream-mock",
				Namespace:  "default",
			},
		}
	})

	waitForStatus(t, name, stream.Suspended)
	wakeUp(t, name, stream.Running)

	// Collect job events from the watcher channel
	aggregator := make(map[types.UID]stream.BackendResource)

	// Act
	waitForBackendResource(t, name,

		func(backendResource stream.BackendResource) {

			testStream, err := streamingClientSet.
				StreamingV2().
				TestStreamDefinitionV2s("default").
				Get(t.Context(), name, metav1.GetOptions{})
			require.NoError(t, err, "Failed to get TestStreamDefinition for update")

			switch stream.Phase(testStream.Status.Phase) {
			case stream.Running:
				t.Logf("TestStreamDefinition %s/%s is in Running phase, waiting for Scheduled phase", testStream.Namespace, testStream.Name)
				updateStream(t, name, func(definition *v2.TestStreamDefinitionV2) {
					definition.Spec.ExecutionSettings.StreamingBackend.BatchJobBackend = nil
					definition.Spec.ExecutionSettings.StreamingBackend.CronJobBackend = &v2.CronJobBackend{
						Schedule: "*/1 * * * *",
						JobTemplateRef: corev1.ObjectReference{
							Kind:      "StreamingJobTemplate",
							Name:      "arcane-stream-mock",
							Namespace: "default",
						},
					}
				})

				aggregator[backendResource.UID()] = backendResource
			default:
				t.Logf("TestStreamDefinition %s/%s is in unexpected phase %s, waiting for Scheduled phase", testStream.Namespace, testStream.Name, testStream.Status.Phase)
				aggregator[backendResource.UID()] = backendResource
			}
		},

		func(backendResource stream.BackendResource) bool {
			_, ok := backendResource.ToObject().(*batchv1.CronJob)
			return ok
		})

	assertObjectTypes(t, aggregator)
}

// Test_CreateStream verifies that creating a TestStreamDefinition results in the creation of both backfill and regular streaming jobs.
// It watches for Job events in the Kubernetes cluster and checks that at least one backfill job and one regular job are created and completed.
func Test_StreamStateTransitionToRunning(t *testing.T) {
	// Arrange

	name := configureV2StreamDefinition(t, func(definition *v2.TestStreamDefinitionV2) {
		definition.Spec.ExecutionSettings.Suspended = true
		definition.Spec.ExecutionSettings.StreamingBackend.CronJobBackend = &v2.CronJobBackend{
			Schedule: "*/1 * * * *",
			JobTemplateRef: corev1.ObjectReference{
				Kind:      "StreamingJobTemplate",
				Name:      "arcane-stream-mock",
				Namespace: "default",
			},
		}
	})

	waitForStatus(t, name, stream.Suspended)
	wakeUp(t, name, stream.Scheduled)

	// Collect job events from the watcher channel
	aggregator := make(map[types.UID]stream.BackendResource)

	// Act
	waitForBackendResource(t, name,

		func(backendResource stream.BackendResource) {

			testStream, err := streamingClientSet.
				StreamingV2().
				TestStreamDefinitionV2s("default").
				Get(t.Context(), name, metav1.GetOptions{})
			require.NoError(t, err, "Failed to get TestStreamDefinition for update")

			switch stream.Phase(testStream.Status.Phase) {
			case stream.Scheduled:
				t.Logf("TestStreamDefinition %s/%s is in Running phase, waiting for Scheduled phase", testStream.Namespace, testStream.Name)
				updateStream(t, name, func(definition *v2.TestStreamDefinitionV2) {
					definition.Spec.ExecutionSettings.APIVersion = "v1"
					definition.Spec.ExecutionSettings.StreamingBackend.CronJobBackend = nil
					definition.Spec.ExecutionSettings.StreamingBackend.BatchJobBackend = &v2.BatchJobBackend{
						JobTemplateRef: corev1.ObjectReference{
							APIVersion: "streaming.sneaksanddata.com/v1",
							Kind:       "StreamingJobTemplate",
							Name:       "arcane-stream-mock",
							Namespace:  "default",
						},
					}
				})

				aggregator[backendResource.UID()] = backendResource
				return
			default:
				t.Logf("TestStreamDefinition %s/%s is in unexpected phase %s, waiting for Scheduled phase", testStream.Namespace, testStream.Name, testStream.Status.Phase)
				aggregator[backendResource.UID()] = backendResource
				return
			}
		},

		func(backendResource stream.BackendResource) bool {
			t.Logf("Observed backend resource with UID %s and type %T", backendResource.UID(), backendResource.ToObject())
			_, ok := backendResource.ToObject().(*batchv1.Job)
			return ok
		})

	assertObjectTypes(t, aggregator)
}

func wakeUp(t *testing.T, name string, targetPhase stream.Phase) {
	testStream, err := streamingClientSet.
		StreamingV2().
		TestStreamDefinitionV2s("default").
		Get(t.Context(), name, metav1.GetOptions{})
	require.NoError(t, err, "Failed to get TestStreamDefinition for update")

	testStream.Spec.ExecutionSettings.Suspended = false
	err = mgr.GetClient().Update(t.Context(), testStream)
	require.NoError(t, err, "Failed to update TestStreamDefinition to trigger job creation")

	waitForStatus(t, name, targetPhase)
}

func buildV2StreamDefinition(configure func(definition *v2.TestStreamDefinitionV2)) *v2.TestStreamDefinitionV2 {
	// Create a TestStreamDefinition with dummy data
	testStream := v2.TestStreamDefinitionV2{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "streaming.sneaksanddata.com/v1",
			Kind:       "TestStreamDefinition",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "integration-test-stream-",
			Namespace:    "default",
		},
		Spec: v2.TestsStreamDefinitionSpec{
			Source:      "mock-source",
			Destination: "mock-destination",
			RunDuration: "5s",
			TestSecretRef: &corev1.LocalObjectReference{
				Name: "test-secret",
			},
			ExecutionSettings: v2.ExecutionSettings{
				APIVersion: "v1",
				BackfillJobTemplateRef: corev1.ObjectReference{
					Kind:      "StreamingJobTemplate",
					Name:      "arcane-stream-mock",
					Namespace: "default",
				},
			},
		},
	}

	if configure != nil {
		configure(&testStream)
	}

	return &testStream
}

func configureV2StreamDefinition(t *testing.T, configure func(definition *v2.TestStreamDefinitionV2)) string {
	testStream := buildV2StreamDefinition(configure)
	newStream, err := streamingClientSet.
		StreamingV2().
		TestStreamDefinitionV2s(testStream.Namespace).
		Create(t.Context(), testStream, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Logf("Created TestStreamDefinition: %s/%s", newStream.Namespace, newStream.Name)

	return newStream.Name
}

func waitForStatus(t *testing.T, name string, desiredStatus stream.Phase) {
	err := wait.PollUntilContextCancel(t.Context(), 1*time.Second, true, func(ctx context.Context) (done bool, err error) {
		testStream, err := streamingClientSet.
			StreamingV2().
			TestStreamDefinitionV2s("default").
			Get(t.Context(), name, metav1.GetOptions{})
		return stream.Phase(testStream.Status.Phase) == desiredStatus, err
	})
	require.NoError(t, err)
}

func updateStream(t *testing.T, name string, update func(*v2.TestStreamDefinitionV2)) {
	err := wait.PollUntilContextCancel(t.Context(), 1*time.Second, true, func(ctx context.Context) (done bool, err error) {
		testStream, err := streamingClientSet.
			StreamingV2().
			TestStreamDefinitionV2s("default").
			Get(t.Context(), name, metav1.GetOptions{})
		require.NoError(t, err, "Failed to get TestStreamDefinition for update")

		update(testStream)
		_, err = streamingClientSet.
			StreamingV2().
			TestStreamDefinitionV2s("default").
			Update(t.Context(), testStream, metav1.UpdateOptions{})
		if errors.IsConflict(err) {
			return false, nil
		}
		return err == nil, err
	})
	require.NoError(t, err)
}

func assertObjectTypes(t *testing.T, jobs map[types.UID]stream.BackendResource) {
	var jobCount, cronJobCount int
	for _, ber := range jobs {
		switch ber.ToObject().(type) {
		case *batchv1.Job:
			jobCount++
		case *batchv1.CronJob:
			cronJobCount++
		}
	}
	require.GreaterOrEqual(t, jobCount, 1)
	require.GreaterOrEqual(t, cronJobCount, 1)
}
