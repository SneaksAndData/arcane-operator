package stream

import (
	"context"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type StatusManager interface {
	UpdateStreamPhase(ctx context.Context, definition Definition, backfillRequest *v1.BackfillRequest, next Phase, eventFunc controllers.EventFunc) (reconcile.Result, error)
}
