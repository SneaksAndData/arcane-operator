package stream

import (
	"context"
	"fmt"
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
	"github.com/SneaksAndData/arcane-operator/services/job"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var (
	_ reconcile.Reconciler            = (*streamReconciler)(nil)
	_ controllers.UnmanagedReconciler = (*streamReconciler)(nil)
)

type streamReconciler struct {
	gvk           schema.GroupVersionKind
	client        client.Client
	jobBuilder    JobBuilder
	streamClass   *v1.StreamClass
	eventRecorder record.EventRecorder
}

func (s *streamReconciler) SetupUnmanaged(cache cache.Cache, scheme *runtime.Scheme, mapper meta.RESTMapper) (controller.Controller, error) {
	controllerName := s.streamClass.Name + "-controller"
	newController, err := controller.NewUnmanaged(controllerName, controller.Options{Reconciler: s})

	if err != nil {
		return nil, fmt.Errorf("failed to start unmanaged stream controller: %w", err)
	}

	resource := &unstructured.Unstructured{}
	resource.SetGroupVersionKind(s.gvk)
	newSource := source.Kind(cache, resource, &handler.TypedEnqueueRequestForObject[*unstructured.Unstructured]{})

	err = newController.Watch(newSource)
	if err != nil {
		return nil, fmt.Errorf("failed to watch stream resource: %w", err)
	}

	err = NewTypedSecondaryWatcherBuilder[*batchv1.Job]().
		WithFilter(NewJobFilter()).
		WithCache(cache).
		WithHandler(handler.TypedEnqueueRequestForOwner[*batchv1.Job](scheme, mapper, resource, handler.OnlyControllerOwner())).
		Build().
		SetupWithController(newController, &batchv1.Job{})

	if err != nil {
		return nil, fmt.Errorf("failed to watch jobs: %w", err)
	}

	err = NewTypedSecondaryWatcherBuilder[*v1.BackfillRequest]().
		WithFilter(NewBackfillRequestFilter(s.streamClass.Name)).
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
		SetupWithController(newController, &v1.BackfillRequest{})

	if err != nil {
		return nil, fmt.Errorf("failed to watch backfills: %w", err)
	}

	return newController, nil
}

// NewStreamReconciler creates a new StreamReconciler instance.
func NewStreamReconciler(client client.Client, gvk schema.GroupVersionKind, jobBuilder JobBuilder, streamClass *v1.StreamClass, eventRecorder record.EventRecorder) controllers.UnmanagedReconciler {
	return &streamReconciler{
		gvk:           gvk,
		jobBuilder:    jobBuilder,
		client:        client,
		streamClass:   streamClass,
		eventRecorder: eventRecorder,
	}
}

// Reconcile implements the reconciliation loop for Stream resources.
func (s *streamReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := s.getLogger(ctx, request.NamespacedName)
	logger.V(0).Info("Reconciling the Stream resource")

	maybeSd := unstructured.Unstructured{}
	maybeSd.SetGroupVersionKind(s.gvk)
	err := s.client.Get(ctx, request.NamespacedName, &maybeSd)

	if errors.IsNotFound(err) { // coverage-ignore
		logger.V(0).Info("stream resource not found, might have been deleted")
		return reconcile.Result{}, nil
	}

	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to fetch Stream resource")
		return reconcile.Result{}, err
	}

	streamDefinition, err := fromUnstructured(&maybeSd)
	if err != nil { // coverage-ignore
		logger.Error(err, "failed to parse Stream definition")
		return reconcile.Result{}, err
	}

	backfillRequest, err := s.getBackfillRequest(ctx, streamDefinition)
	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to fetch BackfillRequest for the Stream, cannot proceed")
		return reconcile.Result{}, err
	}

	j := &batchv1.Job{}
	err = s.client.Get(ctx, request.NamespacedName, j)

	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to fetch Stream Job")
		return reconcile.Result{}, err
	}

	var streamingJob *StreamingJob

	if errors.IsNotFound(err) {
		streamingJob = nil
		logger.V(0).Info("streaming does not exist")
	} else {
		streamingJob = (*StreamingJob)(j)
		logger.V(0).Info("streaming job found")
	}

	return s.moveFsm(ctx, streamDefinition, streamingJob, backfillRequest)
}

func (s *streamReconciler) moveFsm(ctx context.Context, definition Definition, job *StreamingJob, backfillRequest *v1.BackfillRequest) (reconcile.Result, error) {
	phase := definition.GetPhase()

	switch {
	case job != nil && job.IsFailed():
		return s.stopStream(ctx, definition, Failed, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Warning",
				"StreamingJobFailed",
				"The streaming job %s has failed", job.Name)
		})

	case phase == Failed && definition.Suspended() && backfillRequest != nil:
		return s.completeBackfill(ctx, job, definition, backfillRequest, Suspended, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"StreamSuspended",
				"The stream was suspended")
		})

	case phase == Failed && definition.Suspended() && backfillRequest == nil:
		return s.stopStream(ctx, definition, Suspended, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"StreamSuspended",
				"The stream was suspended")
		})

	case phase == Failed:
		return s.stopStream(ctx, definition, Failed, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Warning",
				"StreamingJobFailed",
				"The stream %s has failed", definition.NamespacedName().Name)
		})

	case phase == New && definition.Suspended():
		return s.stopStream(ctx, definition, Suspended, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"StreamSuspended",
				"The new stream %s was added in the suspended state, nothing to do", definition.NamespacedName().Name)
		})

	case phase == New && !definition.Suspended():
		return s.startBackfill(ctx, definition, Pending, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"StreamCreated",
				"Backfill was requested for the new stream definition: %s", definition.NamespacedName().Name)
		})

	case phase == Pending && backfillRequest == nil:
		return s.reconcileJob(ctx, definition, nil, Running, nil)

	case phase == Pending && backfillRequest != nil:
		return s.reconcileJob(ctx, definition, backfillRequest, Backfilling, nil)

	case phase == Running && definition.Suspended():
		return s.stopStream(ctx, definition, Suspended, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"StreamSuspended",
				"The streaming job for stream %s has been suspended", definition.NamespacedName().Name)
		})

	case phase == Running && backfillRequest != nil:
		return s.stopStream(ctx, definition, Pending, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"BackfillRequested",
				"A backfill requested for stream %s, stopping the streaming job to start backfilling", definition.NamespacedName().Name)
		})

	case phase == Running && backfillRequest == nil:
		return s.reconcileJob(ctx, definition, backfillRequest, Running, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"StreamingContinued",
				"The streaming job for stream %s is continuing", definition.NamespacedName().Name)
		})

	case phase == Suspended && backfillRequest != nil:
		return s.updateStreamPhase(ctx, definition, backfillRequest, Pending, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"BackfillRequested",
				"A backfill requested for suspended stream %s", definition.NamespacedName().Name)
		})

	case phase == Suspended && backfillRequest == nil && definition.Suspended():
		return s.stopStream(ctx, definition, Suspended, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"StreamSuspended",
				"The stream %s remains suspended", definition.NamespacedName().Name)
		})

	case phase == Suspended && !definition.Suspended():
		return s.updateStreamPhase(ctx, definition, backfillRequest, Pending, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"StreamResumed",
				"The stream %s has been resumed", definition.NamespacedName().Name)
		})

	case phase == Backfilling && definition.Suspended():
		return s.stopStream(ctx, definition, Suspended, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"StreamSuspended",
				"The backfilling for stream %s has been suspended", definition.NamespacedName().Name)
		})

	case phase == Backfilling && job == nil:
		return s.reconcileJob(ctx, definition, backfillRequest, Backfilling, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"BackfillStarted",
				"Backfill job for stream %s has been started", definition.NamespacedName().Name)
		})

	case phase == Backfilling && job != nil && job.IsCompleted():
		return s.completeBackfill(ctx, job, definition, backfillRequest, Pending, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"BackfillCompleted",
				"Backfill for stream %s has been completed", definition.NamespacedName().Name)
		})

	case phase == Backfilling && job != nil && !job.IsCompleted():
		return s.updateStreamPhase(ctx, definition, backfillRequest, Backfilling, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"BackfillInProgress",
				"Backfill for stream %s is still in progress", definition.NamespacedName().Name)
		})

	}

	return reconcile.Result{}, fmt.Errorf("failed to reconcile Stream FSM for %s/%s. Current state: %s",
		definition.NamespacedName().Namespace,
		definition.NamespacedName().Name,
		definition.StateString(),
	)
}

func (s *streamReconciler) stopStream(ctx context.Context, definition Definition, nextPhase Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	j := &batchv1.Job{}
	j.SetName(definition.NamespacedName().Name)
	j.SetNamespace(definition.NamespacedName().Namespace)
	err := s.client.Delete(ctx, j, client.PropagationPolicy(metav1.DeletePropagationBackground))
	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		return reconcile.Result{}, err
	}

	return s.updateStreamPhase(ctx, definition, nil, nextPhase, eventFunc)
}

func (s *streamReconciler) startBackfill(ctx context.Context, definition Definition, nextPhase Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {

	logger := s.getLogger(ctx, definition.NamespacedName())
	logger.V(2).Info("starting backfill by creating a backfill request")

	backfillRequest := &v1.BackfillRequest{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-initial-backfill-", definition.NamespacedName().Name),
			Namespace:    definition.NamespacedName().Namespace,
		},
		Spec: v1.BackfillRequestSpec{
			StreamId:    definition.NamespacedName().Name,
			StreamClass: s.streamClass.Name,
		},
	}

	err := s.client.Create(ctx, backfillRequest)
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "failed to create backfill request")
		return reconcile.Result{}, err
	}

	return s.updateStreamPhase(ctx, definition, backfillRequest, nextPhase, eventFunc)
}

func (s *streamReconciler) reconcileJob(ctx context.Context, definition Definition, backfillRequest *v1.BackfillRequest, nextPhase Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	logger := s.getLogger(ctx, definition.NamespacedName())
	v1job := batchv1.Job{}
	err := s.client.Get(ctx, definition.NamespacedName(), &v1job)

	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		return reconcile.Result{}, err
	}

	if errors.IsNotFound(err) {
		err := s.startNewJob(ctx, definition, backfillRequest)
		if err != nil { // coverage-ignore
			logger.V(0).Error(err, "failed to create new job for the stream")
			return reconcile.Result{}, err
		}
		err = definition.SetSuspended(false)
		if err != nil { // coverage-ignore
			logger.V(0).Error(err, "unable to unsuspend Stream")
			return reconcile.Result{}, err
		}
		err = s.client.Update(ctx, definition.ToUnstructured())
		if err != nil { // coverage-ignore
			logger.V(0).Error(err, "unable to update Stream to unsuspended state")
			return reconcile.Result{}, err
		}
		return s.updateStreamPhase(ctx, definition, backfillRequest, nextPhase, eventFunc)
	}

	equals, err := s.compareConfigurations(ctx, v1job, definition)
	if err != nil { // coverage-ignore
		return reconcile.Result{}, err
	}

	if equals {
		logger.V(1).Info("Backfill job already exists with matching configuration, skipping creation")
		return s.updateStreamPhase(ctx, definition, &v1.BackfillRequest{}, nextPhase, eventFunc)
	}

	err = s.client.Delete(ctx, &v1job, client.PropagationPolicy(metav1.DeletePropagationBackground))
	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		return reconcile.Result{}, err
	}

	err = s.startNewJob(ctx, definition, backfillRequest)
	if err != nil { // coverage-ignore
		return reconcile.Result{}, err
	}
	return s.updateStreamPhase(ctx, definition, backfillRequest, nextPhase, eventFunc)
}

func (s *streamReconciler) compareConfigurations(ctx context.Context, v1job batchv1.Job, definition Definition) (bool, error) {
	logger := s.getLogger(ctx, definition.NamespacedName())
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

func (s *streamReconciler) completeBackfill(ctx context.Context, job *StreamingJob, definition Definition, request *v1.BackfillRequest, nextStatus Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	if job != nil {
		err := s.client.Delete(ctx, job.ToV1Job())
		if client.IgnoreNotFound(err) != nil {
			return reconcile.Result{}, err
		}
	}

	request.Spec.Completed = true
	err := s.client.Update(ctx, request)
	if err != nil { // coverage-ignore
		return reconcile.Result{}, err
	}
	err = s.client.Status().Update(ctx, request)
	if err != nil { // coverage-ignore
		return reconcile.Result{}, err
	}

	return s.updateStreamPhase(ctx, definition, nil, nextStatus, eventFunc)
}

func (s *streamReconciler) startNewJob(ctx context.Context, definition Definition, request *v1.BackfillRequest) error {
	templateReference := definition.GetJobTemplate(request)
	logger := s.getLogger(ctx, templateReference)

	streamConfiguration, err := definition.CurrentConfiguration(request)
	if err != nil { // coverage-ignore
		return fmt.Errorf("failed to compute stream configuration: %w", err)
	}

	definitionConfigurator, err := definition.ToConfiguratorProvider().JobConfigurator()
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "failed to get definition configurator")
		return err
	}

	secretsConfigurator, err := NewStreamMetadataService(s.streamClass, definition).JobConfigurator()
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

	j, err := s.jobBuilder.BuildJob(ctx, templateReference, combinedConfigurator)
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "failed to build job")
		s.eventRecorder.Eventf(definition.ToUnstructured(),
			corev1.EventTypeWarning,
			"FailedCreateJob",
			"failed to create job: %v", err)
		return err
	}

	err = s.client.Create(ctx, j)
	if err != nil { // coverage-ignore
		return err
	}
	return nil
}

func (s *streamReconciler) getBackfillRequest(ctx context.Context, definition Definition) (*v1.BackfillRequest, error) {
	backfillRequestList := &v1.BackfillRequestList{}
	err := s.client.List(ctx, backfillRequestList, client.InNamespace(definition.NamespacedName().Namespace))
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

func (s *streamReconciler) getLogger(_ context.Context, request types.NamespacedName) klog.Logger {
	return klog.Background().
		WithName("StreamReconciler").
		WithValues("namespace", request.Namespace, "streamId", request.Name, "streamKind", s.gvk.Kind)
}

func (s *streamReconciler) updateStreamPhase(ctx context.Context, definition Definition, backfillRequest *v1.BackfillRequest, next Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	logger := s.getLogger(ctx, definition.NamespacedName())
	if definition.GetPhase() == next { // coverage-ignore
		logger.V(0).Info("Stream phase is already set", "phase", definition.GetPhase())
		return reconcile.Result{}, nil
	}
	logger.V(0).Info("updating Stream status", "from", definition.GetPhase(), "to", next)
	err := definition.SetPhase(next)
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to set Stream status")
		return reconcile.Result{}, err
	}

	err = definition.RecomputeConfiguration(backfillRequest)
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to recompute Stream configuration hash")
		return reconcile.Result{}, err
	}

	err = definition.SetConditions(definition.ComputeConditions(backfillRequest))

	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to set Stream conditions")
		return reconcile.Result{}, err
	}

	statusUpdate := definition.ToUnstructured().DeepCopy()
	err = s.client.Status().Update(ctx, statusUpdate)
	if err != nil { // coverage-ignore
		logger.V(1).Error(err, "unable to update Stream status")
		return reconcile.Result{}, err
	}

	if eventFunc != nil {
		eventFunc()
	}

	return reconcile.Result{}, nil

}
