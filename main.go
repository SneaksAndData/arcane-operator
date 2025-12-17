package main

import (
	"github.com/SneaksAndData/arcane-operator/configuration"
	"github.com/SneaksAndData/arcane-operator/pkg/signals"
	"github.com/SneaksAndData/arcane-operator/telemetry"
	"k8s.io/klog/v2"
)

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
}
