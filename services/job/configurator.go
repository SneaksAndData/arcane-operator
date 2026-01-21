package job

import batchv1 "k8s.io/api/batch/v1"

// ConfigurationHashAnnotation is the annotation key used to store the configuration hash of a Job.
const ConfigurationHashAnnotation = "configuration-hash"

// BackfillLabel is the label key used to indicate if a Job is a backfill.
const BackfillLabel = "arcane/backfilling"

// Configurator defines an interface for configuring Kubernetes Jobs. Each implementer
// can modify the Job object and chain to the next configurator in the sequence.
type Configurator interface {

	// ConfigureJob modifies the provided Job object according to the configurator's logic.
	ConfigureJob(job *batchv1.Job) error
}
