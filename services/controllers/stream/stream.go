package stream

import (
	"context"
	"fmt"
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	runtime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var _ reconcile.Reconciler = (*StreamReconciler)(nil)

type StreamReconciler struct {
	gvk        schema.GroupVersionKind
	client     client.Client
	jobBuilder controllers.JobBuilder
}

// NewStreamReconciler creates a new StreamReconciler instance.
func NewStreamReconciler(gvk schema.GroupVersionKind, jobBuilder controllers.JobBuilder) *StreamReconciler {
	return &StreamReconciler{
		gvk:        gvk,
		jobBuilder: jobBuilder,
	}
}

// Reconcile implements the reconciliation loop for Stream resources.
func (s *StreamReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := s.getLogger(ctx, request.NamespacedName)
	logger.V(2).Info("reconciling Stream resource")

	streamDefinition, err := s.getStreamDefinition(ctx, request)
	if err != nil {
		return reconcile.Result{}, err
	}
	if streamDefinition == nil {
		logger.V(1).Info("Stream resource not found, might have been deleted")
		return reconcile.Result{}, nil
	}

	job, err := s.getJob(ctx, request, logger)
	if err != nil {
		return reconcile.Result{}, err
	}

	backfillRequest, err := s.getBackfillRequestForStream(ctx, streamDefinition)
	if err != nil {
		return reconcile.Result{}, err
	}

	return s.moveFsm(ctx, controllers.FromUnstructured(streamDefinition), job, backfillRequest)
}

// SetupUnmanaged sets up the controller with the Manager.
func (s *StreamReconciler) SetupUnmanaged(mgr runtime.Manager) (controller.Controller, error) {
	resource := &unstructured.Unstructured{}
	resource.SetGroupVersionKind(s.gvk)

	newController, err := controller.NewUnmanaged("stream-controller", controller.Options{
		Reconciler: s,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to start unmanaged stream controller: %w", err)
	}

	// Watch for changes to primary resource Stream
	newSource := source.Kind(mgr.GetCache(), resource, &handler.TypedEnqueueRequestForObject[*unstructured.Unstructured]{})

	err = newController.Watch(newSource)
	if err != nil {
		return nil, fmt.Errorf("failed to watch stream resource: %w", err)
	}

	// Watch for changes to secondary resource Jobs and requeue the owner Stream
	h := handler.TypedEnqueueRequestForOwner[*batchv1.Job](
		mgr.GetScheme(),
		mgr.GetRESTMapper(),
		&batchv1.Job{},
		handler.OnlyControllerOwner(),
	)

	jobSource := source.Kind(mgr.GetCache(), &batchv1.Job{}, h, nil)
	err = newController.Watch(jobSource)

	return newController, nil
}

func (s *StreamReconciler) mapBfrToReconcile(ctx context.Context, obj client.Object) []runtime.Request {
	logger := klog.FromContext(ctx).WithName("mapBfrToReconcile")
	bfr, ok := obj.(*v1.BackfillRequest)
	if !ok {
		logger.V(0).Error(nil, "object is not a BackfillRequest",
			"namespace", obj.GetNamespace(), "name", obj.GetName())
		return nil
	}

	if bfr.Spec.StreamClass == "" {
		logger.V(0).Error(nil, "BackfillRequest does not specify a StreamClass",
			"namespace", obj.GetNamespace(), "name", obj.GetName())
		return nil
	}

	if bfr.Spec.StreamId == "" {
		logger.V(0).Error(nil, "BackfillRequest does not specify a StreamId",
			"namespace", obj.GetNamespace(), "name", obj.GetName())
		return nil
	}

	return []runtime.Request{{NamespacedName: client.ObjectKey{
		Namespace: obj.GetNamespace(),
		Name:      bfr.Spec.StreamId,
	}}}
}

func (s *StreamReconciler) moveFsm(ctx context.Context, definition controllers.StreamDefinition, job controllers.StreamingJob, backfillRequest *v1.BackfillRequest) (reconcile.Result, error) {
	status := definition.GetPhase()

	switch {
	case status == controllers.New && definition.Suspended():
		return s.stopStream(ctx, definition, controllers.Suspended)

	case status == controllers.New && !definition.Suspended():
		return s.startBackfill(ctx, definition, controllers.Pending)

	case status == controllers.Pending && backfillRequest == nil:
		return s.reconcileStream(ctx, definition, controllers.Running)

	case status == controllers.Pending && backfillRequest != nil:
		return s.reconcileBackfill(ctx, definition, backfillRequest, controllers.Backfilling)

	case status == controllers.Running && definition.Suspended():
		return s.stopStream(ctx, definition, controllers.Suspended)

	case status == controllers.Running && backfillRequest != nil:
		return s.reconcileBackfill(ctx, definition, backfillRequest, controllers.Running)

	case status == controllers.Backfilling && job.IsCompleted():
		return s.completeBackfill(ctx, job.ToV1Job(), definition, backfillRequest, controllers.Pending)

	case job.IsFailed():
		return s.stopStream(ctx, definition, controllers.Failed)
	}

	return reconcile.Result{}, fmt.Errorf("failed to reconcile Stream FSM for %s/%s. Current state: %s",
		definition.NamespacedName().Namespace,
		definition.NamespacedName().Name,
		definition.StateString(),
	)
}

func (s *StreamReconciler) stopStream(ctx context.Context, definition controllers.StreamDefinition, nextStatus controllers.Phase) (reconcile.Result, error) {
	job := &batchv1.Job{}
	job.SetName(definition.GetJobName())
	job.SetNamespace(definition.GetJobNamespace())
	err := s.client.Delete(ctx, job)
	if client.IgnoreNotFound(err) != nil {
		return reconcile.Result{}, err
	}

	return s.updateStreamStatus(ctx, definition, nil, nextStatus)
}

func (s *StreamReconciler) startBackfill(ctx context.Context, definition controllers.StreamDefinition, nextStatus controllers.Phase) (reconcile.Result, error) {
	_, err := s.stopStream(ctx, definition, nextStatus)
	if err != nil {
		return reconcile.Result{}, err
	}

	job, err := s.jobBuilder.BuildJob(ctx, definition, &v1.BackfillRequest{})
	if err != nil {
		return reconcile.Result{}, err
	}

	err = s.client.Create(ctx, &job)
	if err != nil {
		return reconcile.Result{}, err
	}

	return s.updateStreamStatus(ctx, definition, &v1.BackfillRequest{}, nextStatus)
}

func (s *StreamReconciler) reconcileStream(ctx context.Context, definition controllers.StreamDefinition, nextStatus controllers.Phase) (reconcile.Result, error) {
	return s.reconcileJob(ctx, definition, nil, nextStatus)
}

func (s *StreamReconciler) reconcileBackfill(ctx context.Context, definition controllers.StreamDefinition, backfillRequest *v1.BackfillRequest, nextStatus controllers.Phase) (reconcile.Result, error) {
	return s.reconcileJob(ctx, definition, backfillRequest, nextStatus)
}

func (s *StreamReconciler) completeBackfill(ctx context.Context, job *batchv1.Job, definition controllers.StreamDefinition, request *v1.BackfillRequest, nextStatus controllers.Phase) (reconcile.Result, error) {
	err := s.client.Delete(ctx, job)
	if client.IgnoreNotFound(err) != nil {
		return reconcile.Result{}, err
	}

	request.Status.Completed = true
	err = s.client.Status().Update(ctx, request)
	if err != nil {
		return reconcile.Result{}, err
	}

	return s.updateStreamStatus(ctx, definition, nil, nextStatus)
}

func (s *StreamReconciler) reconcileJob(ctx context.Context, definition controllers.StreamDefinition, backfillRequest *v1.BackfillRequest, nextStatus controllers.Phase) (reconcile.Result, error) {
	logger := s.getLogger(ctx, definition.NamespacedName())
	currentConfig := definition.CurrentConfiguration()
	previousConfig := definition.LastObservedConfiguration()

	if currentConfig == previousConfig {
		logger.V(0).Info("no configuration changes detected")
		return reconcile.Result{}, nil
	}
	_, err := s.stopStream(ctx, definition, nextStatus)
	if err != nil {
		return reconcile.Result{}, err
	}

	job, err := s.jobBuilder.BuildJob(ctx, definition, backfillRequest)
	err = s.client.Create(ctx, &job)
	if err != nil {
		return reconcile.Result{}, err
	}
	return s.updateStreamStatus(ctx, definition, nil, nextStatus)
}

func (s *StreamReconciler) getJob(ctx context.Context, request reconcile.Request, logger klog.Logger) (controllers.StreamingJob, error) {
	streamJob := &batchv1.Job{}
	err := s.client.Get(ctx, request.NamespacedName, streamJob)

	if errors.IsNotFound(err) {
		return nil, nil
	}

	if client.IgnoreNotFound(err) != nil {
		logger.V(1).Error(err, "unable to fetch Stream Job")
		return nil, err
	}

	return controllers.FromV1Job(streamJob), nil
}

func (s *StreamReconciler) getBackfillRequestForStream(ctx context.Context, definition *unstructured.Unstructured) (*v1.BackfillRequest, error) {
	backfillRequestList := &v1.BackfillRequestList{}
	err := s.client.List(ctx, backfillRequestList, client.InNamespace(definition.GetNamespace()))
	if client.IgnoreNotFound(err) != nil {
		return nil, err
	}

	for _, bfr := range backfillRequestList.Items {
		if bfr.Spec.StreamId == definition.GetName() {
			return &bfr, nil
		}
	}

	return nil, nil
}

func (s *StreamReconciler) getStreamDefinition(ctx context.Context, request reconcile.Request) (*unstructured.Unstructured, error) {
	logger := s.getLogger(ctx, request.NamespacedName)

	streamDefinition := &unstructured.Unstructured{}
	streamDefinition.SetGroupVersionKind(s.gvk)
	err := s.client.Get(ctx, request.NamespacedName, streamDefinition)
	if client.IgnoreNotFound(err) != nil {
		logger.V(1).Error(err, "unable to fetch Stream resource")
		return nil, err
	}
	return streamDefinition, nil
}

func (s *StreamReconciler) getLogger(ctx context.Context, request types.NamespacedName) klog.Logger {
	return klog.FromContext(ctx).
		WithName("StreamReconciler").
		WithValues("namespace", request.Namespace, "name", request.Name)
}

func (s *StreamReconciler) updateStreamStatus(ctx context.Context, definition controllers.StreamDefinition, backfillRequest *v1.BackfillRequest, nextStatus controllers.Phase) (reconcile.Result, error) {
	logger := s.getLogger(ctx, definition.NamespacedName())
	if definition.GetPhase() == nextStatus {
		logger.V(2).Info("Stream phase is already set to", definition.GetPhase())
		return reconcile.Result{}, nil
	}
	logger.V(1).Info("updating Stream status", "from", definition.GetPhase(), "to", nextStatus)
	definition.SetStatus(nextStatus)
	statusUpdate := definition.ToUnstructured().DeepCopy()
	err := s.client.Status().Update(ctx, statusUpdate)
	if err != nil {
		logger.V(1).Error(err, "unable to update Stream status")
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil

}
