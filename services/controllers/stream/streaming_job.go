package stream

import v1 "k8s.io/api/batch/v1"

type StreamingJob interface {
	IsCompleted() bool
	IsFailed() bool
	ToV1Job() *v1.Job
}

func FromV1Job(obj *v1.Job) StreamingJob {
	panic("implement me")
}
