package job

import (
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
)

var _ Configurator = &namespaceConfigurator{}

// namespaceConfigurator is a Configurator that sets the namespace of a Kubernetes Job.
type namespaceConfigurator struct {
	Namespace string
}

func (f namespaceConfigurator) ConfigureJob(job *batchv1.Job) error {
	if f.Namespace == "" {
		return fmt.Errorf("namespace cannot be empty")
	}
	job.Namespace = f.Namespace
	return nil
}

func NewNamespaceConfigurator(namespace string) Configurator {
	return &namespaceConfigurator{
		Namespace: namespace,
	}
}
