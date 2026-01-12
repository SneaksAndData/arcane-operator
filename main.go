package main

import (
	"context"
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/pkg/signals"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream_class"
	"github.com/SneaksAndData/arcane-operator/services/job/job_builder"
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
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	ctx := signals.SetupSignalHandler()

	ctx = telemetry.WithStatsd(ctx, "arcane.operator")
	appLogger, err := telemetry.ConfigureLogger(ctx, map[string]string{"environment": "local"}, "info")
	logger := klog.FromContext(ctx)

	if err != nil {
		logger.V(0).Error(err, "one of the logging handlers cannot be configured")
	}

	klog.SetSlogLogger(appLogger)
	klog.InitFlags(nil)
	controllerruntime.SetLogger(logger)

	config := controllerruntime.GetConfigOrDie()
	mgr, err := controllerruntime.NewManager(config, controllerruntime.Options{
		Scheme: scheme,
	})
	if err != nil {
		setupLog.V(0).Error(err, "unable to start manager")
		return
	}

	jobBuilder := job_builder.NewDefaultJobBuilder(mgr.GetClient())
	controllerFactory := stream.NewStreamControllerFactory(
		mgr.GetClient(),
		jobBuilder,
		mgr,
	)
	err = stream_class.NewStreamClassReconciler(mgr.GetClient(), controllerFactory).SetupWithManager(mgr)
	if err != nil {
		setupLog.V(0).Error(err, "unable to create controller", "controller", "StreamClass")
		return
	}

	err = mgr.Start(context.Background())
	if err != nil {
		setupLog.V(0).Error(err, "problem running manager")
		return
	}
}
