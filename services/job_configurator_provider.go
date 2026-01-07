package services

type JobConfiguratorProvider interface {
	// JobConfigurator returns a JobConfigurator for the stream definition.
	JobConfigurator() JobConfigurator
}
