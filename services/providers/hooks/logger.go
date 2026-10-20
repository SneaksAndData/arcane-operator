package hooks

import (
	"context"
	"os"

	"github.com/SneaksAndData/arcane-operator/config"
	"github.com/SneaksAndData/arcane-operator/telemetry"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
)

func ConfigureLogging(ctx context.Context, appConfig *config.AppConfig, configuration *telemetry.DatadogLoggerConfiguration) error { // coverage-ignore (should be tested with integration tests)

	tags := map[string]string{
		"environment":  os.Getenv("APPLICATION_ENVIRONMENT"),
		"application":  "Arcane.Operator",
		"cluster-name": appConfig.Telemetry.ClusterName,
	}
	appLogger, err := telemetry.ConfigureLogger(ctx, configuration, tags, "info")
	if err != nil {
		return err
	}

	klog.SetSlogLogger(appLogger)
	logger := klog.FromContext(ctx)
	controllerruntime.SetLogger(logger)

	return nil
}
