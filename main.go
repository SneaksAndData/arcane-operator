package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/pkg/signals"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream_class"
	"github.com/SneaksAndData/arcane-operator/services/health"
	"github.com/SneaksAndData/arcane-operator/services/job/job_builder"
	"github.com/SneaksAndData/arcane-operator/telemetry"
	corev1 "k8s.io/api/core/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"os/exec"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"strings"
	"time"
)

var (
	scheme   = apiruntime.NewScheme()
	setupLog = controllerruntime.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var kubeconfigCmd string
	flag.StringVar(&kubeconfigCmd, "kubeconfig-cmd", "/opt/homebrew/bin/kind get kubeconfig", "Command to execute that outputs kubeconfig YAML content")
	flag.Parse()

	ctx := signals.SetupSignalHandler()

	ctx = telemetry.WithStatsd(ctx, "arcane.operator")
	appLogger, err := telemetry.ConfigureLogger(ctx, map[string]string{"environment": "local"}, "info")
	klog.SetSlogLogger(appLogger)
	klog.InitFlags(nil)
	logger := klog.FromContext(ctx)
	controllerruntime.SetLogger(logger)

	if err != nil {
		logger.V(0).Error(err, "one of the logging handlers cannot be configured")
	}

	probesService := health.NewProbesService(health.ProbesConfig{
		Addr:            "[::]:8080",
		WriteTimeout:    5 * time.Second,
		ReadTimeout:     5 * time.Second,
		ShutdownTimeout: 5 * time.Second,
	})

	err = probesService.ListenAndServe(ctx)
	if err != nil {
		setupLog.V(0).Error(err, "unable to start health probes server")
		panic(err)
	}

	config, err := initKubeconfig(kubeconfigCmd, logger)
	if err != nil {
		setupLog.V(0).Error(err, "unable to get kubeconfig")
		panic(err)
	}

	mgr, err := controllerruntime.NewManager(config, controllerruntime.Options{
		Scheme: scheme,
	})
	if err != nil {
		setupLog.V(0).Error(err, "unable to start manager")
		panic(err)
	}

	eventRecorder, err := createEventRecorder(mgr)
	if err != nil {
		setupLog.V(0).Error(err, "unable to create event recorder")
		panic(err)
	}

	controllerFactory := stream.NewStreamControllerFactory(
		mgr.GetClient(),
		job_builder.NewDefaultJobBuilder(mgr.GetClient()),
		mgr,
		eventRecorder,
	)
	err = stream_class.NewStreamClassReconciler(mgr.GetClient(), controllerFactory).SetupWithManager(mgr)

	if err != nil {
		setupLog.V(0).Error(err, "unable to create controller", "controller", "StreamClass")
		panic(err)
	}

	err = mgr.Start(context.Background())
	if err != nil {
		setupLog.V(0).Error(err, "problem running manager")
		panic(err)
	}
}

func initKubeconfig(kubeconfigCmd string, logger klog.Logger) (*rest.Config, error) {
	config, err := controllerruntime.GetConfig()
	if err == nil {
		return config, nil
	}

	logger.V(0).Error(err, "unable to get kubeconfig from in-cluster config, trying command")
	cmdParts := strings.Fields(kubeconfigCmd)
	cmd := exec.Command(cmdParts[0], cmdParts[1:]...)
	output, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil, fmt.Errorf("error executing command: %w\nStderr: %s", err, string(exitErr.Stderr))
		}
		return nil, fmt.Errorf("error executing command: %w", err)
	}
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

func createEventRecorder(mgr manager.Manager) (record.EventRecorderLogger, error) {
	clientSet, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		setupLog.V(0).Error(err, "unable to create clientSet")
		return nil, err
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: clientSet.CoreV1().Events("")})
	eventRecorder := eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "arcane-operator"})
	return eventRecorder, nil
}
