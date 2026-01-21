package job

import (
	"github.com/stretchr/testify/require"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
)

func Test_ConfiguratorChainBuilder_Empty_Chain(t *testing.T) {
	job := &batchv1.Job{}

	builder := NewConfiguratorChainBuilder()
	configurator := builder.Build()

	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
}

func Test_ConfiguratorChainBuilder_Single_Configurator(t *testing.T) {
	job := &batchv1.Job{}

	builder := NewConfiguratorChainBuilder()
	configurator := builder.
		WithConfigurator(NewBackfillConfigurator(true)).
		Build()

	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Contains(t, job.Labels, "arcane/backfilling")
	require.Equal(t, "true", job.Labels["arcane/backfilling"])
}

func Test_ConfiguratorChainBuilder_Multiple_Configurators(t *testing.T) {
	job := &batchv1.Job{}

	builder := NewConfiguratorChainBuilder()
	configurator := builder.
		WithConfigurator(NewBackfillConfigurator(true)).
		WithConfigurator(NewConfigurationChecksumConfigurator("hash123")).
		Build()

	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Contains(t, job.Labels, "arcane/backfilling")
	require.Equal(t, "true", job.Labels["arcane/backfilling"])
	require.Contains(t, job.Annotations, ConfigurationHashAnnotation)
	require.Equal(t, "hash123", job.Annotations[ConfigurationHashAnnotation])
}

func Test_ConfiguratorChainBuilder_Nil_Configurator_Skipped(t *testing.T) {
	job := &batchv1.Job{}

	builder := NewConfiguratorChainBuilder()
	configurator := builder.
		WithConfigurator(NewBackfillConfigurator(true)).
		WithConfigurator(nil).
		WithConfigurator(NewConfigurationChecksumConfigurator("hash456")).
		Build()

	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Contains(t, job.Labels, "arcane/backfilling")
	require.Equal(t, "true", job.Labels["arcane/backfilling"])
	require.Contains(t, job.Annotations, ConfigurationHashAnnotation)
	require.Equal(t, "hash456", job.Annotations[ConfigurationHashAnnotation])
}

func Test_ConfiguratorChainBuilder_Error_Stops_Chain(t *testing.T) {
	job := &batchv1.Job{}

	builder := NewConfiguratorChainBuilder()
	configurator := builder.
		WithConfigurator(NewBackfillConfigurator(true)).
		WithConfigurator(NewConfigurationChecksumConfigurator("")). // This will error
		WithConfigurator(NewBackfillConfigurator(false)).
		Build()

	err := configurator.ConfigureJob(job)
	require.Error(t, err)
	require.EqualError(t, err, "configuration checksum cannot be empty")

	// First configurator should have run
	require.Contains(t, job.Labels, "arcane/backfilling")
	require.Equal(t, "true", job.Labels["arcane/backfilling"])

	// Last configurator should NOT have run (chain stopped at error)
	// The label value should still be "true", not updated to "false"
}

func Test_ConfiguratorChainBuilder_Chaining_Pattern(t *testing.T) {
	builder := NewConfiguratorChainBuilder()

	// Test that WithConfigurator returns the builder for chaining
	result := builder.WithConfigurator(NewBackfillConfigurator(true))
	require.Equal(t, builder, result, "WithConfigurator should return the same builder instance")

	// Test that Build returns a Configurator
	configurator := builder.Build()
	require.NotNil(t, configurator)
	require.Implements(t, (*Configurator)(nil), configurator)
}

func Test_ConfiguratorChainBuilder_Order_Matters(t *testing.T) {
	job := &batchv1.Job{}

	// Pre-populate with a value that will be overwritten
	job.Labels = map[string]string{
		"arcane/backfilling": "initial",
	}

	builder := NewConfiguratorChainBuilder()
	configurator := builder.
		WithConfigurator(NewBackfillConfigurator(true)).
		WithConfigurator(NewBackfillConfigurator(false)).
		Build()

	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	// Last configurator in chain should win
	require.Equal(t, "false", job.Labels["arcane/backfilling"])
}
