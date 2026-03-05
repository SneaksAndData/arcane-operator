package stream

import (
	"context"
	"fmt"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
	"github.com/SneaksAndData/arcane-operator/services/watchers"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ BackendResourceManager = (*BackfillBackendResourceManager)(nil)

type BackfillBackendResourceManager struct {
	streamClass   *v1.StreamClass
	client        client.Client
	statusManager StatusManager
}

func NewBackfillBackendResourceManager(class *v1.StreamClass, client client.Client, manager StatusManager) *BackfillBackendResourceManager { // coverage-ignore (constructor)
	return &BackfillBackendResourceManager{
		streamClass:   class,
		client:        client,
		statusManager: manager,
	}
}

func (b *BackfillBackendResourceManager) SetupWithController(cache cache.Cache, _ *runtime.Scheme, _ meta.RESTMapper, controller controller.Controller, _ schema.GroupVersionKind) error { // coverage-ignore (no need to test the wiring of the controller)
	return watchers.NewTypedSecondaryWatcherBuilder[*v1.BackfillRequest]().
		WithFilter(NewBackfillRequestFilter(b.streamClass.Name)).
		WithCache(cache).
		WithHandler(handler.TypedEnqueueRequestsFromMapFunc(func(ctx context.Context, obj *v1.BackfillRequest) []reconcile.Request {
			return []reconcile.Request{{
				NamespacedName: types.NamespacedName{
					Namespace: obj.Namespace,
					Name:      obj.Spec.StreamId,
				},
			}}
		})).
		Build().
		SetupWithController(controller, &v1.BackfillRequest{})
}

func (b *BackfillBackendResourceManager) Get(ctx context.Context, name types.NamespacedName) (*StreamingJob, error) {
	logger := b.getLogger(ctx, name)
	job := &batchv1.Job{}
	err := b.client.Get(ctx, name, job)

	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to fetch Stream Job")
		return nil, err
	}

	var streamingJob *StreamingJob
	if errors.IsNotFound(err) { // coverage-ignore
		streamingJob = nil
		logger.V(1).Info("streaming does not exist")
	} else {
		streamingJob = (*StreamingJob)(job)
		logger.V(1).Info("streaming job found")
	}

	return streamingJob, nil
}

func (b *BackfillBackendResourceManager) Remove(ctx context.Context, definition Definition, nextPhase Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	job, err := b.Get(ctx, definition.NamespacedName())
	if err != nil { // coverage-ignore
		return reconcile.Result{}, fmt.Errorf("failed to get backend resource: %w", err)
	}
	if job != nil {
		err := b.client.Delete(ctx, job.ToV1Job())
		if client.IgnoreNotFound(err) != nil { // coverage-ignore
			return reconcile.Result{}, err
		}
	}

	request, err := b.GetBackfillRequest(ctx, definition)
	if err != nil { // coverage-ignore
		return reconcile.Result{}, fmt.Errorf("failed to get backfill request: %w", err)
	}

	if request != nil {
		request.Spec.Completed = true
		err := b.client.Update(ctx, request)
		if err != nil { // coverage-ignore
			return reconcile.Result{}, err
		}
		err = b.client.Status().Update(ctx, request)
		if err != nil { // coverage-ignore
			return reconcile.Result{}, err
		}

	}

	return b.statusManager.UpdateStreamPhase(ctx, definition, nil, nextPhase, eventFunc)
}

func (b *BackfillBackendResourceManager) Apply(ctx context.Context, definition Definition, backfillRequest *v1.BackfillRequest, nextPhase Phase, _ *v1.StreamClass, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	logger := b.getLogger(ctx, definition.NamespacedName())
	logger.V(2).Info("starting backfill by creating a backfill request")

	err := b.client.Create(ctx, backfillRequest)
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "failed to create backfill request")
		return reconcile.Result{}, err
	}

	return b.statusManager.UpdateStreamPhase(ctx, definition, backfillRequest, nextPhase, eventFunc)
}

func (b *BackfillBackendResourceManager) NoOp(ctx context.Context, definition Definition, backfillRequest *v1.BackfillRequest, nextPhase Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) { // coverage-ignore (no need to test the no-op function since it's just a pass-through to the status manager)
	return b.statusManager.UpdateStreamPhase(ctx, definition, backfillRequest, nextPhase, eventFunc)
}

func (b *BackfillBackendResourceManager) getLogger(_ context.Context, request types.NamespacedName) klog.Logger { // coverage-ignore (no need to test the contents of the logger)
	return klog.Background().
		WithName("StreamReconciler").
		WithValues("namespace", request.Namespace, "streamId", request.Name, "streamKind", b.streamClass.Spec.KindRef)
}

func (b *BackfillBackendResourceManager) GetBackfillRequest(ctx context.Context, definition Definition) (*v1.BackfillRequest, error) {
	backfillRequestList := &v1.BackfillRequestList{}
	err := b.client.List(ctx, backfillRequestList, client.InNamespace(definition.NamespacedName().Namespace))
	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		return nil, err
	}

	for _, bfr := range backfillRequestList.Items {
		if bfr.Spec.StreamId == definition.NamespacedName().Name && !bfr.Spec.Completed {
			return &bfr, nil
		}
	}

	return nil, nil
}
