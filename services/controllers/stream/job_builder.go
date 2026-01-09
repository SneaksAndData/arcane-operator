package stream

import (
	"context"
	"github.com/SneaksAndData/arcane-operator/services/job"
	batchv1 "k8s.io/api/batch/v1"
)

type JobBuilder interface {
	BuildJob(ctx context.Context, jobType job.TemplateType, configurator job.Configurator) (*batchv1.Job, error)
}
