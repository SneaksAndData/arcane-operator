package main

import (
	"context"
	"errors"
	"flag"

	"github.com/SneaksAndData/arcane-operator/config"
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/pkg/signals"
	"github.com/SneaksAndData/arcane-operator/services"
	"github.com/SneaksAndData/arcane-operator/services/controllers/contracts"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream_class"
	"github.com/SneaksAndData/arcane-operator/services/health"
	"github.com/SneaksAndData/arcane-operator/services/job/job_builder"
	"github.com/SneaksAndData/arcane-operator/services/providers"
	"github.com/SneaksAndData/arcane-operator/services/providers/hooks"
	"github.com/SneaksAndData/arcane-operator/telemetry"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
)

var (
	scheme   = apiruntime.NewScheme()
	setupLog = controllerruntime.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1.AddToScheme(scheme))
}

func main() {
	var kubeconfigCmd string
	flag.StringVar(&kubeconfigCmd, "kubeconfig-cmd", "/opt/homebrew/bin/kind get kubeconfig", "Command to execute that outputs kubeconfig YAML content")
	klog.InitFlags(nil)
	flag.Parse()

	ctx := signals.SetupSignalHandler()

	appConfig, err := config.LoadConfig[config.AppConfig](ctx)
	if err != nil {
		panic(err)
	}

	err = hooks.SetupLogging(ctx, appConfig)
	logger := klog.FromContext(ctx)
	if err != nil {
		logger.V(0).Error(err, "one of the logging handlers cannot be configured")
	}

	probesService := health.NewProbesService(appConfig.ProbesConfiguration)
	go func() {
		err := probesService.ListenAndServe(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			setupLog.V(0).Error(err, "unable to start health probes server")
			panic(err)
		}
	}()

	reporter := telemetry.NewPeriodicMetricsReporter(telemetry.GetClient(ctx), &appConfig.PeriodicMetricsReporterConfiguration)
	go reporter.RunPeriodicMetricsReporter(ctx)

	kubeconfig, err := providers.Kubeconfig(kubeconfigCmd)
	if err != nil {
		setupLog.V(0).Error(err, "unable to get kubeconfig")
		panic(err)
	}

	mgr, err := providers.ControllerManager(kubeconfig, appConfig)
	if err != nil {
		setupLog.V(0).Error(err, "unable to start manager")
		panic(err)
	}

	eventRecorder, err := providers.NewEventRecorder(mgr)
	if err != nil {
		setupLog.V(0).Error(err, "unable to create event recorder")
		panic(err)
	}

	controllerFactory := services.NewStreamControllerFactory(
		mgr.GetClient(),
		job_builder.NewDefaultJobBuilder(mgr.GetClient()),
		mgr,
		eventRecorder,
		contracts.FromUnstructured,
	)
	err = stream_class.NewStreamClassReconciler(mgr.GetClient(), controllerFactory, reporter, eventRecorder).SetupWithManager(mgr)

	if err != nil {
		setupLog.V(0).Error(err, "unable to create controller", "controller", "StreamClass")
		panic(err)
	}

	err = mgr.Start(ctx)
	if errors.Is(err, context.Canceled) {
		logger.V(0).Info("App stopped due to context cancellation")
		return
	}

	if err != nil {
		setupLog.V(0).Error(err, "problem running manager")
		panic(err)
	}
}
