package config

import (
	"github.com/SneaksAndData/arcane-operator/services/health"
	"github.com/SneaksAndData/arcane-operator/telemetry"
)

// AppConfig holds the application configuration settings.
type AppConfig struct {

	// ProbesConfiguration holds the configuration for health probes.
	ProbesConfiguration health.ProbesConfig `mapstructure:"probes,omitempty"`

	// PeriodicMetricsReporterConfiguration holds the configuration for periodic metrics reporting.
	PeriodicMetricsReporterConfiguration telemetry.PeriodicMetricsReporterConfig `mapstructure:"periodic-metrics-reporter,omitempty"`

	// Telemetry holds the telemetry configuration settings.
	Telemetry telemetry.Config `mapstructure:"logging,omitempty"`
}
