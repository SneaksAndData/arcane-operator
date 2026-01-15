package stream

import (
	"fmt"
	v1 "k8s.io/api/batch/v1"
)

type StreamingJob v1.Job

func (job StreamingJob) CurrentConfiguration() (string, error) {
	value, ok := job.Annotations["configuration-hash"]
	if !ok {
		return "", fmt.Errorf("job does not contain configuration hash")
	}
	return value, nil
}

func (job StreamingJob) IsCompleted() bool {
	return job.Status.Succeeded > 0
}

func (job StreamingJob) IsFailed() bool {
	return job.Status.Failed > 0
}

func (job StreamingJob) ToV1Job() *v1.Job {
	j := v1.Job(job)
	return &j
}

func NewStreamingJobFromV1Job(job *v1.Job) StreamingJob {
	return StreamingJob(*job)
}
