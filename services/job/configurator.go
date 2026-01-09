package job

import batchv1 "k8s.io/api/batch/v1"

type TemplateType string

const (
	BackfillJobTemplate  TemplateType = "backfill"
	StreamingJobTemplate TemplateType = "streaming"
)

// Configurator defines an interface for configuring Kubernetes Jobs. Each implementer
// can modify the Job object and chain to the next configurator in the sequence.
type Configurator interface {

	// ConfigureJob modifies the provided Job object according to the configurator's logic.
	ConfigureJob(job *batchv1.Job) error

	// AddNext sets the next JobConfigurator in the chain.
	// Returns self to allow for method chaining.
	AddNext(configurator Configurator) Configurator
}
