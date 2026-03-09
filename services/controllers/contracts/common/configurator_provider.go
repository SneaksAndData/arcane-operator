package common

import (
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/job"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var _ job.ConfiguratorProvider = (*ConfiguratorProvider)(nil)

type ConfiguratorProvider struct {
	underlying *unstructured.Unstructured
	parent     stream.Definition
}

func NewConfiguratorProvider(u *unstructured.Unstructured, parent stream.Definition) *ConfiguratorProvider {
	return &ConfiguratorProvider{
		underlying: u,
		parent:     parent,
	}
}

func (c *ConfiguratorProvider) JobConfigurator() (job.Configurator, error) {
	configurator := job.NewConfiguratorChainBuilder().
		WithConfigurator(job.NewNameConfigurator(c.underlying.GetName())).
		WithConfigurator(job.NewNamespaceConfigurator(c.underlying.GetNamespace())).
		WithConfigurator(job.NewMetadataConfigurator(c.underlying.GetName(), c.underlying.GetKind())).
		WithConfigurator(job.NewBackfillConfigurator(false)).
		WithConfigurator(job.NewEnvironmentConfigurator(c.underlying.Object["spec"], "SPEC")).
		WithConfigurator(job.NewOwnerConfigurator(c.parent.ToOwnerReference())).
		Build()
	return configurator, nil
}
