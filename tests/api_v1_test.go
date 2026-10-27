package integration_tests

import (
	"testing"

	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-stream-mock/pkg/apis/streaming/v2"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Test_CreateStream verifies that creating a TestStreamDefinition results in the creation of both backfill and regular streaming jobs.
// It watches for Job events in the Kubernetes cluster and checks that at least one backfill job and one regular job are created and completed.
func Test_StreamStateTransitionToScheduled(t *testing.T) {
	t.Parallel()

	events := watchBackendResources(t)

	// Arrange
	name := configureV2StreamDefinition(t, "v1", func(definition *v2.TestStreamDefinitionV2) {
		definition.Spec.ExecutionSettings.Suspended = true
		definition.Spec.ExecutionSettings.LayoutVersion = "v1"
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
	waitForBackendResource(t, events, name,

		[]func(job stream.BackendResource){
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
	t.Parallel()

	events := watchBackendResources(t)

	// Arrange
	name := configureV2StreamDefinition(t, "v2", func(definition *v2.TestStreamDefinitionV2) {
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
	waitForBackendResource(t, events, name,

		[]func(job stream.BackendResource){
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
						definition.Spec.ExecutionSettings.LayoutVersion = "v1"
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
		},

		func(backendResource stream.BackendResource) bool {
			t.Logf("Observed backend resource with UID %s and type %T", backendResource.UID(), backendResource.ToObject())
			_, ok := backendResource.ToObject().(*batchv1.Job)
			return ok
		})

	assertObjectTypes(t, aggregator)
}

// Test_DynamicBackfillId verifies that the backfill job contains the correct backfill ID in its environment variables.
func Test_DynamicBackfillId(t *testing.T) {
	t.Parallel()

	events := watchBackendResources(t)

	// Act
	name := configureV2StreamDefinition(t, "v1", func(definition *v2.TestStreamDefinitionV2) {
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

	// Watch for job events in the main thread
	waitForBackendResource(t, events, name,
		[]func(job stream.BackendResource){
			validateBackfillInCronJob(t, "true"),
			validateBackfillId(t),
		},

		func(backendResource stream.BackendResource) bool {
			_, ok := backendResource.ToObject().(*batchv1.CronJob)
			return ok
		})

}
