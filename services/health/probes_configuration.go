package health

import "time"

// ProbesConfig holds configuration for health probes service.
type ProbesConfig struct {
	// Addr is the address the health probes server listens on.
	Addr string `mapstructure:"addr,omitempty"`

	// WriteTimeout is the maximum duration before timing out writes of the health probes server.
	WriteTimeout time.Duration `mapstructure:"write-timeout,omitempty"`

	// ReadTimeout is the maximum duration before timing out reads of the health probes server.
	ReadTimeout time.Duration `mapstructure:"read-timeout,omitempty"`

	// ShutdownTimeout is the maximum duration for the health probes server to shut down gracefully.
	ShutdownTimeout time.Duration `mapstructure:"shutdown-timeout,omitempty"`
}
