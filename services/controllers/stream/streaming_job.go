package stream

import (
	"fmt"
	"github.com/SneaksAndData/arcane-operator/services/job"
	v1 "k8s.io/api/batch/v1"
	"strings"
)

type StreamingJob v1.Job

func (j StreamingJob) CurrentConfiguration() (string, error) {
	value, ok := j.Annotations[job.ConfigurationHashAnnotation]
	if !ok {
		return "", fmt.Errorf("job does not contain configuration hash")
	}
	return value, nil
}

func (j StreamingJob) IsCompleted() bool {
	return j.Status.Succeeded > 0
}

func (j StreamingJob) IsFailed() bool {
	return j.Status.Failed > 0
}

func (j StreamingJob) ToV1Job() *v1.Job {
	v := v1.Job(j)
	return &v
}

func (j StreamingJob) IsBackfill() bool {
	val, ok := j.Labels[job.BackfillLabel]
	if !ok {
		return false
	}
	return strings.ToLower(val) == "true"
}

func NewStreamingJobFromV1Job(job *v1.Job) StreamingJob {
	return StreamingJob(*job)
}
