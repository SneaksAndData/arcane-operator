package main

import (
	"context"
	"github.com/SneaksAndData/arcane-operator/configuration"
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/pkg/signals"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream_class"
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
	appConfig := configuration.LoadConfig(ctx)

	ctx = telemetry.WithStatsd(ctx, "nexus")
	appLogger, err := telemetry.ConfigureLogger(ctx, map[string]string{"environment": appConfig.AppEnvironment}, appConfig.LogLevel)
	logger := klog.FromContext(ctx)

	if err != nil {
		logger.V(0).Error(err, "one of the logging handlers cannot be configured")
	}

	klog.SetSlogLogger(appLogger)

	mgr, err := controllerruntime.NewManager(controllerruntime.GetConfigOrDie(), controllerruntime.Options{
		Scheme: scheme,
	})
	if err != nil {
		setupLog.V(0).Error(err, "unable to start manager")
		return
	}

	err = stream_class.NewStreamClassReconciler(mgr.GetClient(), nil).SetupWithManager(mgr)
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
