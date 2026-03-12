package integration_tests

import (
	"testing"

	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-stream-mock/pkg/apis/streaming/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Test_CreateStream verifies that creating a TestStreamDefinition results in the creation of both backfill and regular streaming jobs.
// It watches for Job events in the Kubernetes cluster and checks that at least one backfill job and one regular job are created and completed.

func Test_CreateRunningStream(t *testing.T) {
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

// // Test_CreateStream verifies that creating a TestStreamDefinition results in the creation of both backfill and regular streaming jobs.
// // It watches for Job events in the Kubernetes cluster and checks that at least one backfill job and one regular job are created and completed.
//
//	func Test_CreateFailedStream(t *testing.T) {
//		// Arrange
//		jobClient := clientSet.BatchV1().Jobs("")
//		require.NotNil(t, jobClient)
//
//		watcher, err := jobClient.Watch(t.Context(), metav1.ListOptions{})
//		t.Cleanup(func() {
//			watcher.Stop()
//		})
//		require.NoError(t, err)
//
//		// Act
//		name := createTestStreamDefinition(t, true)
//
//		// Collect job events from the watcher channel
//		jobs := make(map[types.UID]bool)
//
//		// Watch for job events in the main thread
//		waitForJob(t, watcher, name,
//
//			func(job stream.BackendResource) {
//				jobs[job.UID()] = job.IsFailed()
//			},
//
//			func(job stream.BackendResource) bool {
//				if job.IsFailed() {
//					t.Log("Job is expectedly failed, stopping watcher")
//					return true
//				}
//				return false
//			})
//
//		require.Equal(t, 1, len(jobs))
//
//		// Verify that the stream definition is marked as Failed
//		ticker := time.NewTicker(100 * time.Millisecond)
//		defer ticker.Stop()
//
//		for {
//			select {
//			case <-t.Context().Done():
//				return
//			case <-ticker.C:
//				streamDefinition := &v1.TestStreamDefinition{}
//				err := mgr.GetClient().Get(t.Context(), types.NamespacedName{
//					Name:      name,
//					Namespace: "default",
//				}, streamDefinition)
//				require.NoError(t, err)
//
//				if streamDefinition.Status.Phase == "Failed" {
//					t.Logf("StreamDefinition %s/%s is in Failed phase as expected", streamDefinition.Namespace, streamDefinition.Name)
//
//					// Verify that the job does not exist in the cluster
//					err = mgr.GetClient().Get(t.Context(), types.NamespacedName{Name: name, Namespace: "default"}, &batchv1.Job{})
//					require.Error(t, err)
//					require.True(t, apierrors.IsNotFound(err), "Expected job to be not found after failure")
//
//					return
//				}
//
//				t.Logf("StreamDefinition %s/%s is in %s phase, waiting for Failed phase", streamDefinition.Namespace, streamDefinition.Name, streamDefinition.Status.Phase)
//				time.Sleep(1 * time.Second)
//			}
//		}
//	}
//

func configureV1StreamDefinition(t *testing.T, configure func(definition v1.TestStreamDefinition)) string {
	// Create a TestStreamDefinition with dummy data
	testStream := v1.TestStreamDefinition{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "streaming.sneaksanddata.com/v1",
			Kind:       "TestStreamDefinition",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "integration-test-stream-",
			Namespace:    "default",
		},
		Spec: v1.TestsStreamDefinitionSpec{
			Source:      "mock-source",
			Destination: "mock-destination",
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

	if configure != nil {
		configure(testStream)
	}

	newStream, err := streamingClientSet.
		StreamingV1().
		TestStreamDefinitions(testStream.Namespace).
		Create(t.Context(), &testStream, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Logf("Created TestStreamDefinition: %s/%s", newStream.Namespace, newStream.Name)

	return newStream.Name
}
