package job

import (
	batchv1 "k8s.io/api/batch/v1"
)

var _ Configurator = &ConfigurationChainBuilder{}

type ConfigurationChainBuilder struct {
	configurators []Configurator
}

func (b *ConfigurationChainBuilder) ConfigureJob(job *batchv1.Job) error {
	for _, configurator := range b.configurators {
		if configurator != nil {
			err := configurator.ConfigureJob(job)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func NewConfiguratorChainBuilder() *ConfigurationChainBuilder {
	return &ConfigurationChainBuilder{
		configurators: []Configurator{},
	}
}

func (b *ConfigurationChainBuilder) WithConfigurator(configurator Configurator) *ConfigurationChainBuilder {
	b.configurators = append(b.configurators, configurator)
	return b
}

func (b *ConfigurationChainBuilder) Build() Configurator {
	return b
}
