package job

import batchv1 "k8s.io/api/batch/v1"

var _ Configurator = (*nameConfigurator)(nil)

// nameConfigurator is a Configurator that sets the name property of a Kubernetes Job.
type nameConfigurator struct {
	name string
}

func (c *nameConfigurator) ConfigureJob(job *batchv1.Job) error {
	job.Name = c.name
	return nil
}

func NewNameConfigurator(name string) Configurator {
	return &nameConfigurator{
		name: name,
	}
}
