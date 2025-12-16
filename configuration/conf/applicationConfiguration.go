package conf

// ApplicationConfiguration holds the configuration for the application.
type ApplicationConfiguration struct {
	LogLevel string `mapstructure:"log-level,omitempty"`

	AppEnvironment string `mapstructure:"log-level,omitempty"`

	// StreamClassOperator holds the configuration for the StreamClassOperatorService.
	StreamClassOperator StreamClassOperatorConfiguration

	// StreamingJobOperator holds the configuration for the StreamOperatorService.
	StreamingJobOperator StreamingJobOperatorConfiguration

	// StreamingJobTemplate holds the configuration for the StreamingJobTemplate CRD.
	StreamingJobTemplate StreamingJobTemplateConfiguration
}
