package stream

import (
	"context"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type BackendResourceManager interface {
	SetupWithController(cache cache.Cache, scheme *runtime.Scheme, mapper meta.RESTMapper, controller controller.Controller, manager PhaseManager) error
	Get(ctx context.Context, key client.ObjectKey) (*StreamingJob, error)
	Remove(ctx context.Context, definition Definition, nextPhase Phase, eventFunc controllers.EventFunc) (reconcile.Result, error)
	Apply(ctx context.Context, definition Definition, backfillRequest *v1.BackfillRequest, nextPhase Phase, streamClass *v1.StreamClass, eventFunc controllers.EventFunc) (reconcile.Result, error)
}
