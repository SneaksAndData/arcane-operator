package empty

import (
	"context"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ stream.BackendResourceManager = (*Backend)(nil)

type Backend struct {
	eventRecorder record.EventRecorder
}

func NewEmptyBackend(eventRecorder record.EventRecorder) *Backend {
	return &Backend{
		eventRecorder: eventRecorder,
	}
}

func (j *Backend) SetupWithController(_ cache.Cache, _ *runtime.Scheme, _ meta.RESTMapper, _ controller.Controller, _ schema.GroupVersionKind) error {
	return nil
}

func (j *Backend) Get(_ context.Context, _ types.NamespacedName) (stream.BackendResource, error) {
	return FromResource(nil)
}

func (j *Backend) Apply(_ context.Context, _ stream.Definition, _ *v1.BackfillRequest, _ stream.Phase, _ *v1.StreamClass, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	eventFunc()
	return reconcile.Result{}, nil
}

func (j *Backend) Remove(_ context.Context, _ stream.Definition, _ stream.Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	eventFunc()
	return reconcile.Result{}, nil
}

func (j *Backend) NoOp(_ context.Context, _ stream.Definition, _ *v1.BackfillRequest, _ stream.Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	eventFunc()
	return reconcile.Result{}, nil
}
