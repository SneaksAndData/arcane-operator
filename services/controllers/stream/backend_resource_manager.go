package stream

import (
	"context"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// BackendResourceManager defines the interface for managing backend resources associated with a stream definition
type BackendResourceManager interface {

	// SetupWithController sets up the necessary watches and handlers for the backend resources with the provided controller.
	SetupWithController(cache cache.Cache, scheme *runtime.Scheme, mapper meta.RESTMapper, controller controller.Controller, manager PhaseManager, primaryGvk schema.GroupVersionKind) error

	// Get retrieves the current state of the backend resource associated with the given stream definition.
	Get(ctx context.Context, key client.ObjectKey) (*StreamingJob, error)

	// Remove deletes the backend resource associated with the given stream definition and updates the stream phase accordingly.
	Remove(ctx context.Context, definition Definition, nextPhase Phase, eventFunc controllers.EventFunc) (reconcile.Result, error)

	// Apply creates or updates the backend resource based on the provided stream definition and backfill request, and updates the stream phase accordingly.
	Apply(ctx context.Context, definition Definition, backfillRequest *v1.BackfillRequest, nextPhase Phase, streamClass *v1.StreamClass, eventFunc controllers.EventFunc) (reconcile.Result, error)
}
