package controllers

import (
	"context"
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	controller "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ reconcile.Reconciler = (*StreamReconciler)(nil)

type StreamReconciler struct {
	gvk        schema.GroupVersionKind
	client     client.Client
	jobBuilder JobBuilder
}

func NewStreamReconciler(gvk schema.GroupVersionKind, jobBuilder JobBuilder) *StreamReconciler {
	return &StreamReconciler{
		gvk:        gvk,
		jobBuilder: jobBuilder,
	}
}

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

	return s.moveFsm(ctx, FromUnstructured(streamDefinition), job, backfillRequest)
}

func (s *StreamReconciler) SetupWithManager(mgr controller.Manager) error {
	resource := &unstructured.Unstructured{}
	resource.SetGroupVersionKind(s.gvk)

	return controller.NewControllerManagedBy(mgr).
		For(resource).
		Owns(&batchv1.Job{}).
		Watches(&v1.BackfillRequest{}, handler.EnqueueRequestsFromMapFunc(s.mapBfrToReconcile)).
		Complete(s)
}

func (s *StreamReconciler) mapBfrToReconcile(ctx context.Context, obj client.Object) []controller.Request {
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

	return []controller.Request{{NamespacedName: client.ObjectKey{
		Namespace: obj.GetNamespace(),
		Name:      bfr.Spec.StreamId,
	}}}
}

func (s *StreamReconciler) moveFsm(ctx context.Context, definition StreamDefinition, job StreamingJob, backfillRequest *v1.BackfillRequest) (reconcile.Result, error) {
	status := definition.GetStatus()

	switch {
	case status == New && definition.Suspended():
		return s.stopStream(ctx, definition, Suspended)

	case status == New && !definition.Suspended():
		return s.startBackfill(ctx, definition, Pending)

	case status == Pending && backfillRequest == nil:
		return s.reconcileStream(ctx, definition, Running)

	case status == Pending && backfillRequest != nil:
		return s.reconcileBackfill(ctx, definition, backfillRequest, Backfilling)

	case status == Running && definition.Suspended():
		return s.stopStream(ctx, definition, Suspended)

	case status == Running && backfillRequest != nil:
		return s.reconcileBackfill(ctx, definition, backfillRequest, Running)

	case status == Backfilling && job.IsCompleted():
		return s.completeBackfill(ctx, definition, backfillRequest, Pending)

	case job.IsFailed():
		return s.stopStream(ctx, definition, Failed)
	}

	return reconcile.Result{}, nil
}

func (s *StreamReconciler) stopStream(ctx context.Context, definition StreamDefinition, nextStatus Status) (reconcile.Result, error) {
	job := &batchv1.Job{}
	job.SetName(definition.GetJobName())
	job.SetNamespace(definition.GetJobNamespace())
	err := s.client.Delete(ctx, job)
	if client.IgnoreNotFound(err) != nil {
		return reconcile.Result{}, err
	}

	return s.updateStreamStatus(ctx, definition, nil, nextStatus)
}

func (s *StreamReconciler) startBackfill(ctx context.Context, definition StreamDefinition, nextStatus Status) (reconcile.Result, error) {
	_, err := s.stopStream(ctx, definition, nextStatus)
	if err != nil {
		return reconcile.Result{}, err
	}

	job, err := s.jobBuilder.BuildJob(definition, &v1.BackfillRequest{})
	if err != nil {
		return reconcile.Result{}, err
	}

	err = s.client.Create(ctx, &job)
	if err != nil {
		return reconcile.Result{}, err
	}

	return s.updateStreamStatus(ctx, definition, &v1.BackfillRequest{}, nextStatus)
}

func (s *StreamReconciler) reconcileStream(ctx context.Context, definition StreamDefinition, nextStatus Status) (reconcile.Result, error) {
	return s.reconcileJob(ctx, definition, nil, nextStatus)
}

func (s *StreamReconciler) reconcileBackfill(ctx context.Context, definition StreamDefinition, backfillRequest *v1.BackfillRequest, nextStatus Status) (reconcile.Result, error) {
	return s.reconcileJob(ctx, definition, backfillRequest, nextStatus)
}

func (s *StreamReconciler) completeBackfill(ctx context.Context, definition StreamDefinition, request *v1.BackfillRequest, nextStatus Status) (reconcile.Result, error) {
	job := &batchv1.Job{}
	job.SetName(definition.GetJobName())
	job.SetNamespace(definition.GetJobNamespace())
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

func (s *StreamReconciler) reconcileJob(ctx context.Context, definition StreamDefinition, backfillRequest *v1.BackfillRequest, nextStatus Status) (reconcile.Result, error) {
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

	job, err := s.jobBuilder.BuildJob(definition, nil)
	err = s.client.Create(ctx, &job)
	if err != nil {
		return reconcile.Result{}, err
	}
	return s.updateStreamStatus(ctx, definition, nil, nextStatus)
}

func (s *StreamReconciler) getJob(ctx context.Context, request reconcile.Request, logger klog.Logger) (StreamingJob, error) {
	streamJob := &batchv1.Job{}
	err := s.client.Get(ctx, request.NamespacedName, streamJob)

	if errors.IsNotFound(err) {
		return nil, nil
	}

	if client.IgnoreNotFound(err) != nil {
		logger.V(1).Error(err, "unable to fetch Stream Job")
		return nil, err
	}

	return FromV1Job(streamJob), nil
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

func (s *StreamReconciler) updateStreamStatus(ctx context.Context, definition StreamDefinition, backfillRequest *v1.BackfillRequest, nextStatus Status) (reconcile.Result, error) {
	logger := s.getLogger(ctx, definition.NamespacedName())
	logger.V(1).Info("updating Stream status", "from", definition.GetStatus(), "to", nextStatus)
	definition.SetStatus(nextStatus)
	statusUpdate := definition.ToUnstructured().DeepCopy()
	err := s.client.Status().Update(ctx, statusUpdate)
	if err != nil {
		logger.V(1).Error(err, "unable to update Stream status")
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil

}
