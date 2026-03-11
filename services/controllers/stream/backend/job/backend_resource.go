package job

import (
	"fmt"
	"strings"

	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/job"
	v1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ stream.BackendResource = (*BackendResource)(nil)

type BackendResource struct {
	*v1.Job
}

func (j *BackendResource) Name() string { // coverage-ignore (trivial)
	return j.Job.Name
}

func (j *BackendResource) UID() types.UID { // coverage-ignore (trivial)
	return j.Job.UID
}

func (j *BackendResource) CurrentConfiguration() (string, error) { // coverage-ignore (trivial)
	value, ok := j.Annotations[job.ConfigurationHashAnnotation]
	if !ok {
		return "", fmt.Errorf("job does not contain configuration hash")
	}
	return value, nil
}

func (j *BackendResource) IsCompleted() bool { // coverage-ignore (trivial)
	for _, condition := range j.Status.Conditions {
		if condition.Type == v1.JobComplete && condition.Status == "True" {
			return true
		}
	}
	return false
}

func (j *BackendResource) IsFailed() bool { // coverage-ignore (trivial)
	for _, condition := range j.Status.Conditions {
		if condition.Type == v1.JobFailed && condition.Status == "True" {
			return true
		}
	}
	return false
}

func (j *BackendResource) ToObject() client.Object { // coverage-ignore (trivial)
	return j.Job
}

func (j *BackendResource) IsBackfill() bool { // coverage-ignore (trivial)
	val, ok := j.Labels[job.BackfillLabel]
	if !ok {
		return false
	}
	return strings.ToLower(val) == "true"
}

func FromResource(job *v1.Job) *BackendResource { // coverage-ignore (trivial)
	return &BackendResource{
		Job: job,
	}
}
