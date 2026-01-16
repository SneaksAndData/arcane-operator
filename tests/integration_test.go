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
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"os"
	"os/exec"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"strings"
	"testing"
)

var (
	kubeconfigCmd string
	kubeConfig    *rest.Config
	scheme        = apiruntime.NewScheme()
)

func TestDummyKubeconfigPrint(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(t.Context())
	g, ctx := errgroup.WithContext(ctx)
	mgr := createManager(t, ctx, g)
	t.Cleanup(func() {
		cancel()
		err := g.Wait()
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("manager stopped with error: %v", err)
		}
	})

	// Act
	// Wait for manager to be elected/ready

	<-mgr.Elected()

	// Create Kubernetes job client
	clientset, err := kubernetes.NewForConfig(kubeConfig)
	require.NoError(t, err)

	jobClient := clientset.BatchV1().Jobs("")
	require.NotNil(t, jobClient)

	t.Log("Job client created successfully")

	// Create a watcher for jobs
	watcher, err := jobClient.Watch(ctx, metav1.ListOptions{})
	require.NoError(t, err)
	require.NotNil(t, watcher)

	t.Cleanup(func() {
		watcher.Stop()
	})

	t.Log("Job watcher created successfully")

	// Collect job events from the watcher channel
	var jobEvents []interface{}

	// Watch for job events in the main thread
	for {
		select {
		case event, ok := <-watcher.ResultChan():
			if !ok {
				t.Log("Watcher channel closed")
				return
			}
			t.Logf("Received job event: Type=%s, Object=%T", event.Type, event.Object)
			job := stream.NewStreamingJobFromV1Job(event.Object.(*batchv1.Job))
			jobEvents = append(jobEvents, job)
			if job.IsCompleted() && job.Labels["arcane/backfilling"] == "false" {
				t.Log("Job is completed, stopping watcher")
				t.Logf("Total job events collected: %d", len(jobEvents))
				return
			}
		case <-ctx.Done():
			t.Log("Context cancelled, stopping job watcher")
			t.Logf("Total job events collected: %d", len(jobEvents))
			return
		}
	}

}

func createManager(t *testing.T, ctx context.Context, g *errgroup.Group) manager.Manager {
	mgr, err := controllerruntime.NewManager(kubeConfig, controllerruntime.Options{
		Scheme: scheme,
	})
	require.NoError(t, err)

	jobBuilder := job_builder.NewDefaultJobBuilder(mgr.GetClient())
	controllerFactory := stream.NewStreamControllerFactory(mgr.GetClient(), jobBuilder, mgr)
	err = stream_class.NewStreamClassReconciler(mgr.GetClient(), controllerFactory).SetupWithManager(mgr)

	g.Go(func() error {
		return mgr.Start(ctx)
	})

	return mgr
}

func TestMain(m *testing.M) {
	flag.Parse()
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

	// Run tests
	exitCode := m.Run()

	os.Exit(exitCode)
}

func setupScheme() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))
}

func readKubeconfig() (*rest.Config, error) {
	flag.StringVar(&kubeconfigCmd, "kubeconfig-cmd", "/opt/homebrew/bin/kind get kubeconfig", "Command to execute that outputs kubeconfig YAML content")
	flag.Parse()

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
