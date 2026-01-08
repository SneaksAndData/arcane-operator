package stream

import (
	"context"
	"github.com/SneaksAndData/arcane-operator/services/jobs"
	batchv1 "k8s.io/api/batch/v1"
)

type JobBuilder interface {
	BuildJob(ctx context.Context, jobType jobs.JobTemplateType, configurator jobs.JobConfigurator) (*batchv1.Job, error)
}
