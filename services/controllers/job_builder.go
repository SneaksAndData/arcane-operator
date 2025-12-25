package controllers

import (
	streamingv1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	batchv1 "k8s.io/api/batch/v1"
)

type JobBuilder interface {
	BuildJob(definition StreamDefinition, v *streamingv1.BackfillRequest) (batchv1.Job, error)
}
