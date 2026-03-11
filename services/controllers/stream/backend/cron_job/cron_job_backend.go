package cron_job

import (
	"context"
	"fmt"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend"
	"github.com/SneaksAndData/arcane-operator/services/watchers"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

var _ stream.BackendResourceManager = (*Backend)(nil)

type Backend struct {
	backend.BaseResourceManager
	backend.ResourceReader

	client        client.Client
	statusManager stream.StatusManager
}

func (c *Backend) SetupWithController(cache cache.Cache, scheme *runtime.Scheme, mapper meta.RESTMapper, controller controller.Controller, primaryGvk schema.GroupVersionKind) error {
	primaryResource := &unstructured.Unstructured{}
	primaryResource.SetGroupVersionKind(primaryGvk)
	return watchers.NewTypedSecondaryWatcherBuilder[*batchv1.CronJob]().
		WithFilter(NewPredicate()).
		WithCache(cache).
		WithHandler(handler.TypedEnqueueRequestForOwner[*batchv1.CronJob](scheme, mapper, primaryResource, handler.OnlyControllerOwner())).
		Build().
		SetupWithController(controller, &batchv1.CronJob{})
}

func (c *Backend) Get(ctx context.Context, name client.ObjectKey) (stream.BackendResource, error) {
	cj := &batchv1.CronJob{}
	return c.ResourceReader.Get(ctx, name, cj, FromResource)
}

func (j *Backend) Remove(ctx context.Context, definition stream.Definition, nextPhase stream.Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	object := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      definition.NamespacedName().Name,
			Namespace: definition.NamespacedName().Namespace,
		},
	}

	return j.BaseResourceManager.Remove(ctx, object, func() (reconcile.Result, error) {
		return j.statusManager.UpdateStreamPhase(ctx, definition, nil, nextPhase, eventFunc)
	})
}

func (c *Backend) Apply(ctx context.Context, definition stream.Definition, backfillRequest *v1.BackfillRequest, nextPhase stream.Phase, streamClass *v1.StreamClass, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	logger := c.getLogger(ctx, definition.NamespacedName())
	object := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      definition.NamespacedName().Name,
			Namespace: definition.NamespacedName().Namespace,
		},
	}

	err := c.client.Get(ctx, types.NamespacedName{Name: object.Name, Namespace: object.Namespace}, object)
	if client.IgnoreNotFound(err) != nil {
		return reconcile.Result{}, fmt.Errorf("failed to fetch cronjob: %w", err)
	}

	if !apierrors.IsNotFound(err) {
		equals, err := c.ResourceReader.CompareConfigurations(ctx, object, definition, FromResource)
		if err != nil { // coverage-ignore
			return reconcile.Result{}, err
		}

		if equals {
			logger.V(1).Info("The job already exists with matching configuration, skipping creation")
			return c.statusManager.UpdateStreamPhase(ctx, definition, &v1.BackfillRequest{}, nextPhase, eventFunc)
		}

		_, err = c.BaseResourceManager.Remove(ctx, object, nil)
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("failed to remove cron job: %w", err)
		}
	}

	job, err := c.BaseResourceManager.BuildJob(ctx, definition, backfillRequest, streamClass)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to build job for cronjob backend: %w", err)
	}

	object.Spec.JobTemplate = batchv1.JobTemplateSpec{
		Spec: job.Spec,
	}

	err = c.client.Create(ctx, object)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to create cron job: %w", err)
	}

	return c.statusManager.UpdateStreamPhase(ctx, definition, backfillRequest, nextPhase, eventFunc)
}

func (c *Backend) NoOp(ctx context.Context, definition stream.Definition, backfillRequest *v1.BackfillRequest, nextPhase stream.Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	return c.statusManager.UpdateStreamPhase(ctx, definition, backfillRequest, nextPhase, eventFunc)
}

func (c *Backend) getLogger(_ context.Context, request types.NamespacedName) klog.Logger {
	return klog.Background().
		WithName("cron_job.Backend").
		WithValues("namespace", request.Namespace, "streamId", request.Name)
}
