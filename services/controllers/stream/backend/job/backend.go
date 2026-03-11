package job

import (
	"context"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend"
	"github.com/SneaksAndData/arcane-operator/services/watchers"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
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
	eventRecorder record.EventRecorder
}

func NewJobBackend(client client.Client, jobBuilder stream.JobBuilder, eventRecorder record.EventRecorder, phaseManager stream.StatusManager) *Backend {
	return &Backend{
		BaseResourceManager: backend.BaseResourceManager{
			Client:        client,
			JobBuilder:    jobBuilder,
			EventRecorder: eventRecorder,
		},
		ResourceReader: backend.ResourceReader{
			Client: client,
		},
		client:        client,
		eventRecorder: eventRecorder,
		statusManager: phaseManager,
	}
}

func (j *Backend) SetupWithController(cache cache.Cache, scheme *runtime.Scheme, mapper meta.RESTMapper, controller controller.Controller, primaryGvk schema.GroupVersionKind) error {
	primaryResource := &unstructured.Unstructured{}
	primaryResource.SetGroupVersionKind(primaryGvk)
	return watchers.NewTypedSecondaryWatcherBuilder[*batchv1.Job]().
		WithFilter(NewPredicate()).
		WithCache(cache).
		WithHandler(handler.TypedEnqueueRequestForOwner[*batchv1.Job](scheme, mapper, primaryResource, handler.OnlyControllerOwner())).
		Build().
		SetupWithController(controller, &batchv1.Job{})
}

func (j *Backend) Get(ctx context.Context, name types.NamespacedName) (stream.BackendResource, error) {
	logger := j.getLogger(ctx, name)
	obj := &batchv1.Job{}
	err := j.client.Get(ctx, name, obj)

	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to fetch Stream Job")
		return nil, err
	}

	if errors.IsNotFound(err) {
		logger.V(0).Info("streaming does not exist")
		return nil, nil
	}
	return FromResource(obj)
}

func (j *Backend) Apply(ctx context.Context, definition stream.Definition, backfillRequest *v1.BackfillRequest, nextPhase stream.Phase, streamClass *v1.StreamClass, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	logger := j.getLogger(ctx, definition.NamespacedName())
	v1job := batchv1.Job{}
	err := j.client.Get(ctx, definition.NamespacedName(), &v1job)

	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		return reconcile.Result{}, err
	}

	if errors.IsNotFound(err) {
		newJob, err := j.BuildJob(ctx, definition, backfillRequest, streamClass)
		if err != nil { // coverage-ignore
			logger.V(0).Error(err, "failed to build job for stream")
			return reconcile.Result{}, err
		}

		err = j.client.Create(ctx, newJob)
		if err != nil { // coverage-ignore
			return reconcile.Result{}, err
		}

		err = definition.SetSuspended(false)
		if err != nil { // coverage-ignore
			logger.V(0).Error(err, "unable to unsuspend Stream")
			return reconcile.Result{}, err
		}
		err = j.client.Update(ctx, definition.ToUnstructured())
		if err != nil { // coverage-ignore
			logger.V(0).Error(err, "unable to update Stream to unsuspended state")
			return reconcile.Result{}, err
		}
		return j.statusManager.UpdateStreamPhase(ctx, definition, backfillRequest, nextPhase, eventFunc)
	}

	equals, err := j.CompareConfigurations(ctx, &v1job, definition, FromResource)
	if err != nil { // coverage-ignore
		return reconcile.Result{}, err
	}

	if equals {
		logger.V(1).Info("The job already exists with matching configuration, skipping creation")
		return j.statusManager.UpdateStreamPhase(ctx, definition, &v1.BackfillRequest{}, nextPhase, eventFunc)
	}

	err = j.client.Delete(ctx, &v1job, client.PropagationPolicy(metav1.DeletePropagationBackground))
	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		return reconcile.Result{}, err
	}

	newJob, err := j.BuildJob(ctx, definition, backfillRequest, streamClass)
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "failed to build job for stream")
		return reconcile.Result{}, err
	}

	err = j.client.Create(ctx, newJob)
	if err != nil { // coverage-ignore
		return reconcile.Result{}, err
	}
	return j.statusManager.UpdateStreamPhase(ctx, definition, backfillRequest, nextPhase, eventFunc)
}

func (j *Backend) Remove(ctx context.Context, definition stream.Definition, nextPhase stream.Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	object := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      definition.NamespacedName().Name,
			Namespace: definition.NamespacedName().Namespace,
		},
	}

	return j.BaseResourceManager.Remove(ctx, object, func() (reconcile.Result, error) {
		return j.statusManager.UpdateStreamPhase(ctx, definition, nil, nextPhase, eventFunc)
	})
}

func (j *Backend) NoOp(ctx context.Context, definition stream.Definition, backfillRequest *v1.BackfillRequest, nextPhase stream.Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	return j.statusManager.UpdateStreamPhase(ctx, definition, backfillRequest, nextPhase, eventFunc)
}

func (j *Backend) getLogger(_ context.Context, request types.NamespacedName) klog.Logger {
	return klog.Background().
		WithName("job.Backend").
		WithValues("namespace", request.Namespace, "streamId", request.Name)
}
