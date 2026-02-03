package telemetry

type Config struct {
	// LogLevel sets the logging level (e.g., "debug", "info", "warn", "error").
	LogLevel string `mapstructure:"log-level,omitempty"`

	// ClusterName sets the name of the cluster where the application is running.
	ClusterName string `mapstructure:"cluster-name,omitempty"`

	// MetricsBindAddress sets the address for binding the metrics server.
	MetricsBindAddress string `mapstructure:"metrics-bind-address,omitempty"`
}
