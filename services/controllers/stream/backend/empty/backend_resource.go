package empty

import (
	"fmt"

	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	v1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ stream.BackendResource = (*BackendResource)(nil)

type BackendResource struct {
	*v1.Job
}

func (j *BackendResource) Name() string { // coverage-ignore (trivial)
	return ""
}

func (j *BackendResource) UID() types.UID { // coverage-ignore (trivial)
	return ""
}

func (j *BackendResource) CurrentConfiguration() (string, error) { // coverage-ignore (trivial)
	return "", nil
}

func (j *BackendResource) IsCompleted() bool { // coverage-ignore (trivial)
	return true
}

func (j *BackendResource) IsFailed() bool { // coverage-ignore (trivial)
	return false
}

func (j *BackendResource) ToObject() client.Object { // coverage-ignore (trivial)
	return j.Job
}

func (j *BackendResource) IsBackfill() bool { // coverage-ignore (trivial)
	return false
}

func FromResource(job client.Object) (stream.BackendResource, error) { // coverage-ignore (trivial)
	jobObj, isJob := job.(*v1.Job)

	if !isJob {
		return nil, fmt.Errorf("object is not a Job")
	}

	return &BackendResource{Job: jobObj}, nil
}
