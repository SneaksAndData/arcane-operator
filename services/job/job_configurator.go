package job

import batchv1 "k8s.io/api/batch/v1"

type JobConfigurator interface {
	ConfigureJob(job *batchv1.Job) error
}
