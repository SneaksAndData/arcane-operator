package jobs

// JobConfiguratorProvider defines an interface for types that can provide a JobConfigurator.
type JobConfiguratorProvider interface {

	// JobConfigurator returns a JobConfigurator for the current instance.
	JobConfigurator() JobConfigurator
}
