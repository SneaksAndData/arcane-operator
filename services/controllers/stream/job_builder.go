package stream

import (
	"context"
	"github.com/SneaksAndData/arcane-operator/services"
	batchv1 "k8s.io/api/batch/v1"
)

type JobBuilder interface {
	BuildJob(ctx context.Context, jobType services.JobTemplateType, configurator services.JobConfigurator) (*batchv1.Job, error)
}
