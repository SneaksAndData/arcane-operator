package integration_tests

import (
	"testing"
	"time"

	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend/job"
	mockv1 "github.com/SneaksAndData/arcane-stream-mock/pkg/apis/streaming/v1"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
)

// Test_CreateStream verifies that creating a TestStreamDefinition results in the creation of both backfill and regular streaming jobs.
// It watches for Job events in the Kubernetes cluster and checks that at least one backfill job and one regular job are created and completed.
func Test_CreateStream(t *testing.T) {
	// Arrange
	jobClient := clientSet.BatchV1().Jobs("")
	require.NotNil(t, jobClient)

	watcher, err := jobClient.Watch(t.Context(), metav1.ListOptions{})
	t.Cleanup(func() {
		watcher.Stop()
	})
	require.NoError(t, err)

	// Act
	name := createTestStreamDefinition(t, false)

	// Collect job events from the watcher channel
	jobs := make(map[types.UID]bool)

	// Watch for job events in the main thread
	waitForJob(t, watcher, name,

		func(job stream.BackendResource) {
			jobs[job.UID()] = job.IsBackfill()
		},

		func(job stream.BackendResource) bool {
			if job.IsCompleted() && !job.IsBackfill() {
				t.Log("Job is completed, stopping watcher")
				return true
			}
			return false
		})

	require.GreaterOrEqual(t, len(jobs), 2, "Should have received at least 2 jobs (1 backfill and 1 regular), but got %d", len(jobs))

	backfillFound := false
	regularFound := false
	for _, isBackfill := range jobs {
		if isBackfill {
			backfillFound = true
		} else {
			regularFound = true
		}
	}
	require.True(t, backfillFound, "Should have received at least 1 backfill job")
	require.True(t, regularFound, "Should have received at least 1 regular job")
}

// Test_CreateStream verifies that creating a TestStreamDefinition results in the creation of both backfill and regular streaming jobs.
// It watches for Job events in the Kubernetes cluster and checks that at least one backfill job and one regular job are created and completed.
func Test_CreateFailedStream(t *testing.T) {
	// Arrange
	jobClient := clientSet.BatchV1().Jobs("")
	require.NotNil(t, jobClient)

	watcher, err := jobClient.Watch(t.Context(), metav1.ListOptions{})
	t.Cleanup(func() {
		watcher.Stop()
	})
	require.NoError(t, err)

	// Act
	name := createTestStreamDefinition(t, true)

	// Collect job events from the watcher channel
	jobs := make(map[types.UID]bool)

	// Watch for job events in the main thread
	waitForJob(t, watcher, name,

		func(job stream.BackendResource) {
			jobs[job.UID()] = job.IsFailed()
		},

		func(job stream.BackendResource) bool {
			if job.IsFailed() {
				t.Log("Job is expectedly failed, stopping watcher")
				return true
			}
			return false
		})

	require.Equal(t, 1, len(jobs))

	// Verify that the stream definition is marked as Failed
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-t.Context().Done():
			return
		case <-ticker.C:
			streamDefinition := &mockv1.TestStreamDefinition{}
			err := mgr.GetClient().Get(t.Context(), types.NamespacedName{
				Name:      name,
				Namespace: "default",
			}, streamDefinition)
			require.NoError(t, err)

			if streamDefinition.Status.Phase == "Failed" {
				t.Logf("StreamDefinition %s/%s is in Failed phase as expected", streamDefinition.Namespace, streamDefinition.Name)

				// Verify that the job does not exist in the cluster
				err = mgr.GetClient().Get(t.Context(), types.NamespacedName{Name: name, Namespace: "default"}, &batchv1.Job{})
				require.Error(t, err)
				require.True(t, apierrors.IsNotFound(err), "Expected job to be not found after failure")

				return
			}

			t.Logf("StreamDefinition %s/%s is in %s phase, waiting for Failed phase", streamDefinition.Namespace, streamDefinition.Name, streamDefinition.Status.Phase)
			time.Sleep(1 * time.Second)
		}
	}
}

func waitForJob(t *testing.T, watcher watch.Interface, name string, handleEvent func(job stream.BackendResource), isCompleted func(job stream.BackendResource) bool) {
	for {
		select {
		case event, ok := <-watcher.ResultChan():
			if !ok {
				t.Error("watcher channel closed")
				return
			}
			rawJob, ok := event.Object.(*batchv1.Job)
			if !ok {
				t.Fatalf("expected Job object, got %T", event.Object)
				return
			}

			if rawJob.Name != name {
				t.Logf("unexpected resource name: %s, skipping", rawJob.Name)
				continue
			}

			t.Logf("Received resource event: Type=%s, Object=%T", event.Type, event.Object)
			resource, err := job.FromResource(rawJob)
			require.NoError(t, err)
			handleEvent(resource)
			if isCompleted(resource) {
				t.Log("Job is isCompleted, stopping watcher")
				return
			}
		case <-t.Context().Done():
			t.Fatal("Job watcher stopped with timeout or cancellation")
			return
		}
	}
}

func createTestStreamDefinition(t *testing.T, shouldFail bool) string {
	// Create a TestStreamDefinition with dummy data
	testStream := mockv1.TestStreamDefinition{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "streaming.sneaksanddata.com/v1",
			Kind:       "TestStreamDefinition",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "integration-test-stream-",
			Namespace:    "default",
		},
		Spec: mockv1.TestsStreamDefinitionSpec{
			Source:      "mock-source",
			Destination: "mock-destination",
			Suspended:   false,
			ShouldFail:  shouldFail,
			JobTemplateRef: corev1.ObjectReference{
				APIVersion: "streaming.sneaksanddata.com/v1",
				Kind:       "StreamingJobTemplate",
				Name:       "arcane-stream-mock",
				Namespace:  "default",
			},
			BackfillJobTemplateRef: corev1.ObjectReference{
				APIVersion: "streaming.sneaksanddata.com/v1",
				Kind:       "StreamingJobTemplate",
				Name:       "arcane-stream-mock",
				Namespace:  "default",
			},
			RunDuration: "15s",
			TestSecretRef: &corev1.LocalObjectReference{
				Name: "test-secret",
			},
		},
	}

	newStream, err := streamingClientSet.
		StreamingV1().
		TestStreamDefinitions(testStream.Namespace).
		Create(t.Context(), &testStream, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Logf("Created TestStreamDefinition: %s/%s", newStream.Namespace, newStream.Name)

	return newStream.Name
}
