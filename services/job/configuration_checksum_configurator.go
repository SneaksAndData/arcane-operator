package job

import (
	batchv1 "k8s.io/api/batch/v1"
)

var _ Configurator = (*ConfigurationChecksumConfigurator)(nil)

// ConfigurationChecksumConfigurator sets the configuration checksum annotation on a job.
type ConfigurationChecksumConfigurator struct {
	configurationChecksum string
}

// ConfigureJob sets the configuration checksum annotation on the job.
func (c *ConfigurationChecksumConfigurator) ConfigureJob(job *batchv1.Job) error {
	if job.Annotations == nil {
		job.Annotations = make(map[string]string)
	}

	job.Annotations[ConfigurationHashAnnotation] = c.configurationChecksum
	return nil
}

// NewConfigurationChecksumConfigurator creates a new ConfigurationChecksumConfigurator.
func NewConfigurationChecksumConfigurator(configurationChecksum string) *ConfigurationChecksumConfigurator {
	return &ConfigurationChecksumConfigurator{
		configurationChecksum: configurationChecksum,
	}
}
