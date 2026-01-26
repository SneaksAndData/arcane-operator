package config

import "github.com/SneaksAndData/arcane-operator/services/health"

// AppConfig holds the application configuration settings.
type AppConfig struct {

	// ProbesConfiguration holds the configuration for health probes.
	ProbesConfiguration health.ProbesConfig `mapstructure:"probes,omitempty"`
}
