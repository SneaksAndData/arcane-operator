package integration_tests

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services"
	"github.com/SneaksAndData/arcane-operator/services/controllers/contracts"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend/cron_job"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend/job"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream_class"
	"github.com/SneaksAndData/arcane-operator/services/job/job_builder"
	"github.com/SneaksAndData/arcane-operator/telemetry"
	mockv1 "github.com/SneaksAndData/arcane-stream-mock/pkg/apis/streaming/v1"
	mockv2 "github.com/SneaksAndData/arcane-stream-mock/pkg/apis/streaming/v2"
	streaming "github.com/SneaksAndData/arcane-stream-mock/pkg/generated/clientset/versioned"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

var (
	kubeconfigCmd      string
	kubeConfig         *rest.Config
	scheme             = apiruntime.NewScheme()
	streamingClientSet *streaming.Clientset
	clientSet          *kubernetes.Clientset
	mgr                manager.Manager
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

	streamingClientSet, err = streaming.NewForConfig(kubeConfig)
	if err != nil {
		panic(fmt.Errorf("error creating streaming clientSet: %w", err))
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
	utilruntime.Must(mockv2.AddToScheme(scheme))
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
	controllerFactory := services.NewStreamControllerFactory(mgr.GetClient(), jobBuilder, mgr, eventRecorder, contracts.FromUnstructured)

	reporter := telemetry.NewPeriodicMetricsReporter(telemetry.GetClient(ctx), &telemetry.PeriodicMetricsReporterConfig{
		ReportInterval: 1 * time.Minute,
		InitialDelay:   1 * time.Minute,
	})
	// We don't start the reporter here, as we don't need metrics for the tests.
	err = stream_class.NewStreamClassReconciler(mgr.GetClient(), controllerFactory, reporter, eventRecorder).SetupWithManager(mgr)
	if err != nil {
		return nil, fmt.Errorf("unable to setup StreamClassReconciler: %w", err)
	}

	g.Go(func() error {
		return mgr.Start(ctx)
	})

	return mgr, nil
}

func waitForBackendResource(t *testing.T, name string, handleEvent func(job stream.BackendResource), isCompleted func(job stream.BackendResource) bool) {
	jobClient := clientSet.BatchV1().Jobs("")
	require.NotNil(t, jobClient)

	jobWatcher, err := jobClient.Watch(t.Context(), metav1.ListOptions{})
	t.Cleanup(func() {
		jobWatcher.Stop()
	})
	require.NoError(t, err)

	cronJobClient := clientSet.BatchV1().CronJobs("")
	require.NotNil(t, cronJobClient)

	cronJobWatcher, err := cronJobClient.Watch(t.Context(), metav1.ListOptions{})
	t.Cleanup(func() {
		cronJobWatcher.Stop()
	})
	require.NoError(t, err)

	for {
		select {
		case event, ok := <-merge(t.Context(), jobWatcher.ResultChan(), cronJobWatcher.ResultChan()):
			if !ok {
				t.Error("jobWatcher channel closed")
				return
			}

			var resource stream.BackendResource
			switch obj := event.Object.(type) {
			case *batchv1.Job:
				t.Logf("Got a Job: %s", obj.Name)
				resource, err = job.FromResource(obj)
				require.NoError(t, err)
			case *batchv1.CronJob:
				t.Logf("Got a CronJob: %s", obj.Name)
				resource, err = cron_job.FromResource(obj)
				require.NoError(t, err)
			default:
				t.Fatal("Job jobWatcher stopped with timeout or cancellation")
			}

			if resource.Name() != name {
				t.Logf("unexpected resource name: %s, skipping", resource.Name())
				continue
			}

			handleEvent(resource)
			if isCompleted(resource) {
				t.Log("Job is isCompleted, stopping jobWatcher")
				return
			}
		case <-t.Context().Done():
			t.Fatal("Job jobWatcher stopped with timeout or cancellation")
			return
		}
	}
}

func merge[T any](ctx context.Context, ch1, ch2 <-chan T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for {
			select {
			case v, ok := <-ch1:
				if !ok {
					ch1 = nil
				} else {
					out <- v
				}
			case v, ok := <-ch2:
				if !ok {
					ch2 = nil
				} else {
					out <- v
				}
			case <-ctx.Done():
				return
			}
			if ch1 == nil && ch2 == nil {
				return
			}
		}
	}()
	return out
}
