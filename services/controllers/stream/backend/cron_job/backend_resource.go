package cron_job

import (
	"fmt"
	"strings"

	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/job"
	"k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ stream.BackendResource = (*BackendResource)(nil)

type BackendResource struct {
	*v1.CronJob
}

func (j *BackendResource) Name() string { // coverage-ignore (trivial)
	return j.CronJob.Name
}

func (j *BackendResource) UID() types.UID { // coverage-ignore (trivial)
	return j.CronJob.UID
}

func (j *BackendResource) CurrentConfiguration() (string, error) { // coverage-ignore (trivial)
	value, ok := j.Annotations[job.ConfigurationHashAnnotation]
	if !ok {
		return "", fmt.Errorf("cron job does not contain configuration hash")
	}
	return value, nil
}

func (j *BackendResource) IsCompleted() bool { // coverage-ignore (trivial)
	return false
}

func (j *BackendResource) IsFailed() bool { // coverage-ignore (trivial)
	return false
}

func (j *BackendResource) ToObject() client.Object { // coverage-ignore (trivial)
	return j.CronJob
}

func (j *BackendResource) IsBackfill() bool { // coverage-ignore (trivial)
	val, ok := j.Labels[job.BackfillLabel]
	if !ok {
		return false
	}
	return strings.ToLower(val) == "true"
}

func FromResource(cj *v1.CronJob) *BackendResource { // coverage-ignore (trivial)
	return &BackendResource{
		CronJob: cj,
	}
}
