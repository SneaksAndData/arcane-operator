package stream

import (
	"context"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type BackfillBackendResourceManager interface {
	BackendResourceManager

	// GetBackfillRequest returns the current backfill request associated with the given stream definition, if any.
	GetBackfillRequest(ctx context.Context, definition Definition) (*v1.BackfillRequest, error)

	// Complete handles the completion of a backfill request for the given stream definition,
	// transitioning to the next phase and invoking the provided event function.
	Complete(ctx context.Context, definition Definition, nextPhase Phase, streamClass *v1.StreamClass, eventFunc controllers.EventFunc) (reconcile.Result, error)
}
