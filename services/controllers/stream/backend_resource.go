package stream

import (
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// BackendResource defines an interface for resources that represent the backend of a Stream.
// This could be a Kubernetes Job or CronJob, see implementation in the backend folder.
type BackendResource interface {
	// Name returns the name of the backend resource.
	Name() string

	// UID returns the Kubernetes UID of the backend resource.
	UID() types.UID

	// CurrentConfiguration returns the hash sum of the current configuration (spec) of the backend resource.
	CurrentConfiguration() (string, error)

	// IsCompleted return true if the workload represented by the backend resource has completed successfully.
	// e.g the job has completed with a Succeeded condition, or the cronjob has a last schedule time and no active jobs.
	IsCompleted() bool

	// IsFailed returns true if the workload represented by the backend resource has failed.
	IsFailed() bool

	// ToObject converts the backend resource to a client.Object for use with the Kubernetes API.
	ToObject() client.Object

	// IsBackfill returns true if the backend resource is associated with a backfill request.
	IsBackfill() bool
}
