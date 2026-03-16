package stream

import (
	"context"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// StatusManager defines an interface for managing the status sub resource of a stream definition.
type StatusManager interface {

	// UpdateStreamPhase updates the phase of the stream definition's status and emits an event if the phase has changed.
	UpdateStreamPhase(ctx context.Context, definition Definition, backfillRequest *v1.BackfillRequest, next Phase, eventFunc controllers.EventFunc) (reconcile.Result, error)
}
