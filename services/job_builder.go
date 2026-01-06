package services

import batchv1 "k8s.io/api/batch/v1"

type JobBuilder interface {
}

type JobConfigurator interface {
	ConfigureJob(job *batchv1.Job) error
}
