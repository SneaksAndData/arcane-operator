package stream

import (
	batchv1 "k8s.io/api/batch/v1"
)

var _ JobConfigurator = &BasicJobConfigurator{}

type BasicJobConfigurator struct {
	definition Definition
}

func (f BasicJobConfigurator) ConfigureJob(job *batchv1.Job) error {
	panic("not implemented")
}

func NewFromStreamDefinition(definition Definition) *BasicJobConfigurator {
	return &BasicJobConfigurator{
		definition: definition,
	}
}
