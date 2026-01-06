package stream

import (
	context "context"
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	batchv1 "k8s.io/api/batch/v1"
)

type JobBuilder interface {
	BuildJob(context.Context, Definition, *v1.BackfillRequest) (*batchv1.Job, error)
}

type JobConfigurator interface {
	ConfigureJob(job *batchv1.Job) error
}
