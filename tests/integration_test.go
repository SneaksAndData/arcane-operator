package integration_tests

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream_class"
	"github.com/SneaksAndData/arcane-operator/services/job/job_builder"
	mockv1 "github.com/SneaksAndData/arcane-stream-mock/pkg/apis/streaming/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"os"
	"os/exec"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"strings"
	"testing"
	"time"
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
	name := createTestStreamDefinition(t, mgr, false)

	// Collect job events from the watcher channel
	jobs := make(map[types.UID]bool)

	// Watch for job events in the main thread
	waitForJob(t, watcher, name,

		func(job stream.StreamingJob) {
			jobs[job.UID] = job.IsBackfill()
		},

		func(job stream.StreamingJob) bool {
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
	name := createTestStreamDefinition(t, mgr, true)

	// Collect job events from the watcher channel
	jobs := make(map[types.UID]bool)

	// Watch for job events in the main thread
	waitForJob(t, watcher, name,

		func(job stream.StreamingJob) {
			jobs[job.UID] = job.IsFailed()
		},

		func(job stream.StreamingJob) bool {
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
			} else {
				t.Logf("StreamDefinition %s/%s is in %s phase, waiting for Failed phase", streamDefinition.Namespace, streamDefinition.Name, streamDefinition.Status.Phase)
			}
		}
	}
}

func waitForJob(t *testing.T, watcher watch.Interface, name string, handleEvent func(job stream.StreamingJob), isCompleted func(job stream.StreamingJob) bool) {
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
				t.Logf("unexpected job name: %s, skipping", rawJob.Name)
				continue
			}

			t.Logf("Received job event: Type=%s, Object=%T", event.Type, event.Object)
			job := stream.NewStreamingJobFromV1Job(rawJob)
			handleEvent(job)
			if isCompleted(job) {
				t.Log("Job is isCompleted, stopping watcher")
				return
			}
		case <-t.Context().Done():
			t.Fatal("Job watcher stopped with timeout or cancellation")
			return
		}
	}
}

func createTestStreamDefinition(t *testing.T, mgr manager.Manager, shouldFail bool) string {
	// Create a TestStreamDefinition with dummy data
	streamId, err := uuid.NewUUID()
	require.NoError(t, err)
	testStream := &mockv1.TestStreamDefinition{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "streaming.sneaksanddata.com/v1",
			Kind:       "TestStreamDefinition",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      streamId.String(),
			Namespace: "default",
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

	// Create the TestStreamDefinition in the cluster
	err = mgr.GetClient().Create(t.Context(), testStream)
	require.NoError(t, err)
	t.Logf("Created TestStreamDefinition: %s/%s", testStream.Namespace, testStream.Name)

	return streamId.String()
}

func createManager(ctx context.Context, g *errgroup.Group) (manager.Manager, error) {
	mgr, err := controllerruntime.NewManager(kubeConfig, controllerruntime.Options{
		Scheme: scheme,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to start manager: %w", err)
	}

	jobBuilder := job_builder.NewDefaultJobBuilder(mgr.GetClient())

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: clientSet.CoreV1().Events("")})
	eventRecorder := eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "Arcane-Operator-Test"})
	controllerFactory := stream.NewStreamControllerFactory(mgr.GetClient(), jobBuilder, mgr, eventRecorder)

	err = stream_class.NewStreamClassReconciler(mgr.GetClient(), controllerFactory).SetupWithManager(mgr)
	if err != nil {
		return nil, fmt.Errorf("unable to setup StreamClassReconciler: %w", err)
	}

	g.Go(func() error {
		return mgr.Start(ctx)
	})

	return mgr, nil
}

var (
	kubeconfigCmd string
	kubeConfig    *rest.Config
	scheme        = apiruntime.NewScheme()
	clientSet     *kubernetes.Clientset
	mgr           manager.Manager
)

func TestMain(m *testing.M) {
	flag.StringVar(&kubeconfigCmd, "kubeconfig-cmd", "/opt/homebrew/bin/kind get kubeconfig", "Command to execute that outputs kubeconfig YAML content")
	flag.Parse()

	// Initialize logger to avoid controller-runtime warnings
	klog.InitFlags(nil)
	logger := klog.Background()
	controllerruntime.SetLogger(logger)

	if testing.Short() {
		fmt.Println("Skipping integration tests in short mode")
		return
	}
	setupScheme()

	var err error
	kubeConfig, err = readKubeconfig()
	if err != nil {
		panic(fmt.Errorf("error reading kubeconfig: %w", err))
	}

	clientSet, err = kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		panic(fmt.Errorf("error creating kubernetes clientSet: %w", err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)
	mgr, err = createManager(ctx, g)
	if err != nil {
		panic(fmt.Errorf("error creating manager: %w", err))
	}
	defer func() {
		err := g.Wait()
		if errors.Is(err, context.Canceled) {
			return
		}
		if err != nil {
			panic(fmt.Errorf("error running manager: %w", err))
		}
	}()

	// Act
	<-mgr.Elected()
	// Run tests
	exitCode := m.Run()

	os.Exit(exitCode)
}

func setupScheme() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))
	utilruntime.Must(mockv1.AddToScheme(scheme))
}

func readKubeconfig() (*rest.Config, error) {

	// Parse and execute the command
	cmdParts := strings.Fields(kubeconfigCmd)
	if len(cmdParts) == 0 {
		return nil, errors.New("kubeconfig-cmd cannot be empty")
	}

	cmd := exec.Command(cmdParts[0], cmdParts[1:]...)
	output, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil, fmt.Errorf("error executing command: %w\nStderr: %s", err, string(exitErr.Stderr))
		}
		return nil, fmt.Errorf("error executing command: %w", err)
	}

	// Load the kubeconfig from bytes and convert to rest.Config
	clientConfig, err := clientcmd.NewClientConfigFromBytes(output)
	if err != nil {
		return nil, fmt.Errorf("error loading kubeconfig: %w", err)
	}

	restConfig, err := clientConfig.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("error converting to rest.Config: %w", err)
	}

	return restConfig, nil
}
