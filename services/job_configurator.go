package services

import batchv1 "k8s.io/api/batch/v1"

type JobTemplateType string

const (
	BackfillJobTemplate  JobTemplateType = "backfill"
	StreamingJobTemplate JobTemplateType = "streaming"
)

// JobConfigurator defines an interface for configuring Kubernetes Jobs. Each implementer
// can modify the Job object and chain to the next configurator in the sequence.
type JobConfigurator interface {

	// ConfigureJob modifies the provided Job object according to the configurator's logic.
	ConfigureJob(job *batchv1.Job) error

	// AddNext sets the next JobConfigurator in the chain.
	// Returns self to allow for method chaining.
	AddNext(configurator *JobConfigurator) JobConfigurator
}
