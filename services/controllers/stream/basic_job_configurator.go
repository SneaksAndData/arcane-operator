package stream

import (
	"github.com/SneaksAndData/arcane-operator/services"
	batchv1 "k8s.io/api/batch/v1"
)

var _ services.JobConfigurator = &BasicJobConfigurator{}

type BasicJobConfigurator struct {
	definition Definition
	next       services.JobConfigurator
}

func (f BasicJobConfigurator) AddNext(configurator services.JobConfigurator) services.JobConfigurator {
	f.next = configurator
	return f
}

func (f BasicJobConfigurator) ConfigureJob(job *batchv1.Job) error {
	panic("not implemented")
}

func NewFromStreamDefinition(definition Definition) *BasicJobConfigurator {
	return &BasicJobConfigurator{
		definition: definition,
	}
}
