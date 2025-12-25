package controllers

import v1 "k8s.io/api/batch/v1"

type StreamingJob interface {
	IsCompleted() bool
	IsFailed() bool
}

func FromV1Job(obj *v1.Job) StreamingJob {
	panic("implement me")
}
