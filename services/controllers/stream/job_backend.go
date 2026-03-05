package stream

import (
	"context"
	"fmt"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
	"github.com/SneaksAndData/arcane-operator/services/job"
	"github.com/SneaksAndData/arcane-operator/services/watchers"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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

var (
	_ BackendResourceManager = (*JobBackend)(nil)
)

type JobBackend struct {
	client        client.Client
	phaseManager  StatusManager
	jobBuilder    JobBuilder
	eventRecorder record.EventRecorder
}

func NewJobBackend(client client.Client, jobBuilder JobBuilder, eventRecorder record.EventRecorder, phaseManager StatusManager) *JobBackend {
	return &JobBackend{
		client:        client,
		jobBuilder:    jobBuilder,
		eventRecorder: eventRecorder,
		phaseManager:  phaseManager,
	}
}

func (j *JobBackend) SetupWithController(cache cache.Cache, scheme *runtime.Scheme, mapper meta.RESTMapper, controller controller.Controller, primaryGvk schema.GroupVersionKind) error {
	primaryResource := &unstructured.Unstructured{}
	primaryResource.SetGroupVersionKind(primaryGvk)
	return watchers.NewTypedSecondaryWatcherBuilder[*batchv1.Job]().
		WithFilter(NewJobFilter()).
		WithCache(cache).
		WithHandler(handler.TypedEnqueueRequestForOwner[*batchv1.Job](scheme, mapper, primaryResource, handler.OnlyControllerOwner())).
		Build().
		SetupWithController(controller, &batchv1.Job{})
}

func (j *JobBackend) Get(ctx context.Context, name types.NamespacedName) (*StreamingJob, error) {
	logger := j.getLogger(ctx, name)
	job := &batchv1.Job{}
	err := j.client.Get(ctx, name, job)

	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to fetch Stream Job")
		return nil, err
	}

	var streamingJob *StreamingJob
	if errors.IsNotFound(err) {
		streamingJob = nil
		logger.V(0).Info("streaming does not exist")
	} else {
		streamingJob = (*StreamingJob)(job)
		logger.V(0).Info("streaming job found")
	}

	return streamingJob, nil
}

func (j *JobBackend) Apply(ctx context.Context, definition Definition, backfillRequest *v1.BackfillRequest, nextPhase Phase, streamClass *v1.StreamClass, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	logger := j.getLogger(ctx, definition.NamespacedName())
	v1job := batchv1.Job{}
	err := j.client.Get(ctx, definition.NamespacedName(), &v1job)

	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		return reconcile.Result{}, err
	}

	if errors.IsNotFound(err) {
		err := j.startNewJob(ctx, definition, backfillRequest, streamClass)
		if err != nil { // coverage-ignore
			logger.V(0).Error(err, "failed to create new job for the stream")
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
		return j.phaseManager.UpdateStreamPhase(ctx, definition, backfillRequest, nextPhase, eventFunc)
	}

	equals, err := j.compareConfigurations(ctx, v1job, definition)
	if err != nil { // coverage-ignore
		return reconcile.Result{}, err
	}

	if equals {
		logger.V(1).Info("Backfill job already exists with matching configuration, skipping creation")
		return j.phaseManager.UpdateStreamPhase(ctx, definition, &v1.BackfillRequest{}, nextPhase, eventFunc)
	}

	err = j.client.Delete(ctx, &v1job, client.PropagationPolicy(metav1.DeletePropagationBackground))
	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		return reconcile.Result{}, err
	}

	err = j.startNewJob(ctx, definition, backfillRequest, streamClass)
	if err != nil { // coverage-ignore
		return reconcile.Result{}, err
	}
	return j.phaseManager.UpdateStreamPhase(ctx, definition, backfillRequest, nextPhase, eventFunc)
}

func (j *JobBackend) Remove(ctx context.Context, definition Definition, nextPhase Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	job := &batchv1.Job{}
	job.SetName(definition.NamespacedName().Name)
	job.SetNamespace(definition.NamespacedName().Namespace)
	err := j.client.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground))
	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		return reconcile.Result{}, err
	}

	return j.phaseManager.UpdateStreamPhase(ctx, definition, nil, nextPhase, eventFunc)

}

func (j *JobBackend) NoOp(ctx context.Context, definition Definition, backfillRequest *v1.BackfillRequest, nextPhase Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	return j.phaseManager.UpdateStreamPhase(ctx, definition, backfillRequest, nextPhase, eventFunc)
}

func (j *JobBackend) startNewJob(ctx context.Context, definition Definition, request *v1.BackfillRequest, streamClass *v1.StreamClass) error {
	templateReference := definition.GetJobTemplate(request)
	logger := j.getLogger(ctx, templateReference)

	streamConfiguration, err := definition.CurrentConfiguration(request)
	if err != nil { // coverage-ignore
		return fmt.Errorf("failed to compute stream configuration: %w", err)
	}

	definitionConfigurator, err := definition.ToConfiguratorProvider().JobConfigurator()
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "failed to get definition configurator")
		return err
	}

	secretsConfigurator, err := NewStreamMetadataService(streamClass, definition).JobConfigurator()
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "failed to get metadata configurator")
		return err
	}

	backfillRequestConfigurator, err := request.JobConfigurator()
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "failed to get backfill request configurator")
		return err
	}

	combinedConfigurator := job.NewConfiguratorChainBuilder().
		WithConfigurator(definitionConfigurator).
		WithConfigurator(backfillRequestConfigurator).
		WithConfigurator(job.NewConfigurationChecksumConfigurator(streamConfiguration)).
		WithConfigurator(secretsConfigurator)

	newJob, err := j.jobBuilder.BuildJob(ctx, templateReference, combinedConfigurator)
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "failed to build job")
		j.eventRecorder.Eventf(definition.ToUnstructured(),
			corev1.EventTypeWarning,
			"FailedCreateJob",
			"failed to create job: %v", err)
		return err
	}

	err = j.client.Create(ctx, newJob)
	if err != nil { // coverage-ignore
		return err
	}
	return nil
}

func (j *JobBackend) compareConfigurations(ctx context.Context, v1job batchv1.Job, definition Definition) (bool, error) {
	logger := j.getLogger(ctx, definition.NamespacedName())
	jobConfiguration, err := StreamingJob(v1job).CurrentConfiguration()
	if err != nil {
		logger.V(0).Error(err, "Failed to extract configuration from job")
		return false, err
	}

	// This is a new stream, so we start backfill even if the backfill request is not present.
	definitionConfiguration, err := definition.CurrentConfiguration(nil)
	if err != nil {
		logger.V(0).Error(err, "Failed to extract configuration from stream definition")
		return false, err
	}
	return jobConfiguration == definitionConfiguration, nil
}

func (j *JobBackend) getLogger(_ context.Context, request types.NamespacedName) klog.Logger {
	return klog.Background().
		WithName("StreamReconciler").
		WithValues("namespace", request.Namespace, "streamId", request.Name)
}
