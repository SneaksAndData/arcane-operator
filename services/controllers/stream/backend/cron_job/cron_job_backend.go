package cron_job

import (
	"context"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/watchers"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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

var _ stream.BackendResourceManager = (*CronJobBackend)(nil)

type CronJobBackend struct {
	client client.Client
}

func (c *CronJobBackend) SetupWithController(cache cache.Cache, scheme *runtime.Scheme, mapper meta.RESTMapper, controller controller.Controller, primaryGvk schema.GroupVersionKind) error {
	primaryResource := &unstructured.Unstructured{}
	primaryResource.SetGroupVersionKind(primaryGvk)
	return watchers.NewTypedSecondaryWatcherBuilder[*batchv1.CronJob]().
		WithFilter(NewPredicate()).
		WithCache(cache).
		WithHandler(handler.TypedEnqueueRequestForOwner[*batchv1.CronJob](scheme, mapper, primaryResource, handler.OnlyControllerOwner())).
		Build().
		SetupWithController(controller, &batchv1.CronJob{})
}

func (c *CronJobBackend) Get(ctx context.Context, name client.ObjectKey) (stream.BackendResource, error) {
	logger := c.getLogger(ctx, name)
	cj := &batchv1.CronJob{}
	err := c.client.Get(ctx, name, cj)

	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to fetch Stream Cron Job")
		return nil, err
	}

	var streamingJob stream.BackendResource
	if errors.IsNotFound(err) {
		streamingJob = nil
		logger.V(0).Info("streaming does not exist")
	} else {
		streamingJob = FromResource(cj)
		logger.V(0).Info("streaming cj found")
	}

	return streamingJob, nil
}

func (c *CronJobBackend) Remove(ctx context.Context, definition stream.Definition, nextPhase stream.Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	job := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      definition.NamespacedName().Name,
			Namespace: definition.NamespacedName().Namespace,
		},
	}
	err := c.client.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationForeground))
	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		return reconcile.Result{}, err
	}

	return c.phaseManager.UpdateStreamPhase(ctx, definition, nil, nextPhase, eventFunc)
}

func (c *CronJobBackend) Apply(ctx context.Context, definition stream.Definition, backfillRequest *v1.BackfillRequest, nextPhase stream.Phase, streamClass *v1.StreamClass, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	//TODO implement me
	panic("implement me")
}

func (c *CronJobBackend) NoOp(ctx context.Context, definition stream.Definition, backfillRequest *v1.BackfillRequest, nextPhase stream.Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	//TODO implement me
	panic("implement me")
}

func (c *CronJobBackend) getLogger(_ context.Context, request types.NamespacedName) klog.Logger {
	return klog.Background().
		WithName("StreamReconciler").
		WithValues("namespace", request.Namespace, "streamId", request.Name)
}
