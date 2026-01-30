package telemetry

import "time"

type PeriodicMetricsReporterConfig struct {

	// InitialDelay is the delay before the first metrics report is sent.
	InitialDelay time.Duration `mapstructure:"initial-delay,omitempty"`

	// ReportInterval is the interval between successive metrics reports.
	ReportInterval time.Duration `mapstructure:"report-interval,omitempty"`
}
