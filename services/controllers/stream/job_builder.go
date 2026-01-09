package stream

import (
	"context"
	"github.com/SneaksAndData/arcane-operator/services/job"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/types"
)

// JobBuilder defines an interface for building Kubernetes Jobs based on a specified template type
// and a configurator that modifies the Job object.
type JobBuilder interface {

	// BuildJob constructs a Kubernetes Job of the specified template type using the provided configurator.
	BuildJob(ctx context.Context, templateName types.NamespacedName, configurator job.Configurator) (*batchv1.Job, error)
}
