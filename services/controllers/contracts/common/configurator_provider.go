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

func (c *ConfiguratorProvider) JobConfigurator() (job.Configurator, error) { // coverage-ignore (should be tested in integration tests)
	// For job streams this configurator always emit a non-backfill configurator
	// which could be overridden by backfill request later in the reconciliation loop if BFR is provided.
	// For cronjob streams this configurator always emit a backfill configurator since cronjob stream only supports
	// backfill mode.
	alwaysBackfill := c.parent.GetBackend() == stream.CronJob

	configuratorBuilder := job.NewConfiguratorChainBuilder().
		WithConfigurator(job.NewNameConfigurator(c.underlying.GetName())).
		WithConfigurator(job.NewNamespaceConfigurator(c.underlying.GetNamespace())).
		WithConfigurator(job.NewMetadataConfigurator(c.underlying.GetName(), c.underlying.GetKind())).
		WithConfigurator(job.NewBackfillConfigurator(alwaysBackfill)).
		WithConfigurator(job.NewEnvironmentConfigurator(c.underlying.Object["spec"], "SPEC")).
		WithConfigurator(job.NewOwnerConfigurator(c.parent.ToOwnerReference()))

	return configuratorBuilder.Build(), nil
}
