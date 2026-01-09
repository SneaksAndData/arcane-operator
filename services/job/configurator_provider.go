package job

// ConfiguratorProvider defines an interface for types that can provide a JobConfigurator.
type ConfiguratorProvider interface {

	// JobConfigurator returns a JobConfigurator for the current instance.
	JobConfigurator() Configurator
}
