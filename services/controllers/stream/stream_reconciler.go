package stream

import (
	"context"
	"fmt"
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
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

var _ reconcile.Reconciler = (*streamReconciler)(nil)

type streamReconciler struct {
	gvk        schema.GroupVersionKind
	client     client.Client
	jobBuilder JobBuilder
}

// NewStreamReconciler creates a new StreamReconciler instance.
func NewStreamReconciler(gvk schema.GroupVersionKind, jobBuilder JobBuilder) reconcile.Reconciler {
	return &streamReconciler{
		gvk:        gvk,
		jobBuilder: jobBuilder,
	}
}

// Reconcile implements the reconciliation loop for Stream resources.
func (s *streamReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := s.getLogger(ctx, request.NamespacedName)
	logger.V(2).Info("reconciling Stream resource")

	maybeSd, err := s.getStreamDefinition(ctx, request)
	if client.IgnoreNotFound(err) != nil {
		return reconcile.Result{}, err
	}

	if errors.IsNotFound(err) {
		logger.V(1).Info("Stream resource not found, might have been deleted")
		return reconcile.Result{}, nil
	}

	streamDefinition, err := fromUnstructured(maybeSd)
	if err != nil {
		logger.V(0).Error(err, "failed to parse Stream definition")
		return reconcile.Result{}, err
	}

	backfillRequest, err := s.getBackfillRequestForStream(ctx, streamDefinition)
	if client.IgnoreNotFound(err) != nil {
		return reconcile.Result{}, err
	}

	job, err := s.getJob(ctx, request, logger)
	if client.IgnoreNotFound(err) != nil {
		return reconcile.Result{}, err
	}

	return s.moveFsm(ctx, streamDefinition, job, backfillRequest)
}

// SetupUnmanaged sets up the controller with the Manager.
func (s *streamReconciler) SetupUnmanaged(mgr runtime.Manager) (controller.Controller, error) {
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

func (s *streamReconciler) mapBfrToReconcile(ctx context.Context, obj client.Object) []runtime.Request {
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

func (s *streamReconciler) moveFsm(ctx context.Context, definition Definition, job StreamingJob, backfillRequest *v1.BackfillRequest) (reconcile.Result, error) {
	phase := definition.GetPhase()

	switch {
	case phase == New && definition.Suspended():
		return s.stopStream(ctx, definition, Suspended)

	case phase == New && !definition.Suspended():
		return s.startBackfill(ctx, definition, Pending)

	case phase == Pending && backfillRequest == nil:
		return s.reconcileStream(ctx, definition, Running)

	case phase == Pending && backfillRequest != nil:
		return s.reconcileBackfill(ctx, definition, backfillRequest, Backfilling)

	case phase == Running && definition.Suspended():
		return s.stopStream(ctx, definition, Suspended)

	case phase == Running && backfillRequest != nil:
		return s.reconcileBackfill(ctx, definition, backfillRequest, Running)

	case phase == Backfilling && job.IsCompleted():
		return s.completeBackfill(ctx, job.ToV1Job(), definition, backfillRequest, Pending)

	case job.IsFailed():
		return s.stopStream(ctx, definition, Failed)
	}

	return reconcile.Result{}, fmt.Errorf("failed to reconcile Stream FSM for %s/%s. Current state: %s",
		definition.NamespacedName().Namespace,
		definition.NamespacedName().Name,
		definition.StateString(),
	)
}

func (s *streamReconciler) stopStream(ctx context.Context, definition Definition, nextStatus Phase) (reconcile.Result, error) {
	job := &batchv1.Job{}
	job.SetName(definition.NamespacedName().Name)
	job.SetNamespace(definition.NamespacedName().Namespace)
	err := s.client.Delete(ctx, job)
	if client.IgnoreNotFound(err) != nil {
		return reconcile.Result{}, err
	}

	return s.updateStreamStatus(ctx, definition, nil, nextStatus)
}

func (s *streamReconciler) startBackfill(ctx context.Context, definition Definition, nextStatus Phase) (reconcile.Result, error) {
	_, err := s.stopStream(ctx, definition, nextStatus)
	if err != nil {
		return reconcile.Result{}, err
	}

	job, err := s.jobBuilder.BuildJob(ctx, definition, &v1.BackfillRequest{})
	if err != nil {
		return reconcile.Result{}, err
	}

	err = s.client.Create(ctx, job)
	if err != nil {
		return reconcile.Result{}, err
	}

	return s.updateStreamStatus(ctx, definition, &v1.BackfillRequest{}, nextStatus)
}

func (s *streamReconciler) reconcileStream(ctx context.Context, definition Definition, nextStatus Phase) (reconcile.Result, error) {
	return s.reconcileJob(ctx, definition, nil, nextStatus)
}

func (s *streamReconciler) reconcileBackfill(ctx context.Context, definition Definition, backfillRequest *v1.BackfillRequest, nextStatus Phase) (reconcile.Result, error) {
	return s.reconcileJob(ctx, definition, backfillRequest, nextStatus)
}

func (s *streamReconciler) completeBackfill(ctx context.Context, job *batchv1.Job, definition Definition, request *v1.BackfillRequest, nextStatus Phase) (reconcile.Result, error) {
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

func (s *streamReconciler) reconcileJob(ctx context.Context, definition Definition, backfillRequest *v1.BackfillRequest, nextStatus Phase) (reconcile.Result, error) {
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
	err = s.client.Create(ctx, job)
	if err != nil {
		return reconcile.Result{}, err
	}
	return s.updateStreamStatus(ctx, definition, nil, nextStatus)
}

func (s *streamReconciler) getJob(ctx context.Context, request reconcile.Request, logger klog.Logger) (StreamingJob, error) {
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

func (s *streamReconciler) getBackfillRequestForStream(ctx context.Context, definition Definition) (*v1.BackfillRequest, error) {
	backfillRequestList := &v1.BackfillRequestList{}
	err := s.client.List(ctx, backfillRequestList, client.InNamespace(definition.NamespacedName().Namespace))
	if client.IgnoreNotFound(err) != nil {
		return nil, err
	}

	for _, bfr := range backfillRequestList.Items {
		if bfr.Spec.StreamId == definition.NamespacedName().Name {
			return &bfr, nil
		}
	}

	return nil, nil
}

func (s *streamReconciler) getStreamDefinition(ctx context.Context, request reconcile.Request) (*unstructured.Unstructured, error) {
	logger := s.getLogger(ctx, request.NamespacedName)

	streamDefinition := &unstructured.Unstructured{}
	streamDefinition.SetGroupVersionKind(s.gvk)
	err := s.client.Get(ctx, request.NamespacedName, streamDefinition)
	if err != nil {
		logger.V(1).Error(err, "unable to fetch Stream resource")
		return nil, err
	}
	return streamDefinition, nil
}

func (s *streamReconciler) getLogger(ctx context.Context, request types.NamespacedName) klog.Logger {
	return klog.FromContext(ctx).
		WithName("StreamReconciler").
		WithValues("namespace", request.Namespace, "name", request.Name)
}

func (s *streamReconciler) updateStreamStatus(ctx context.Context, definition Definition, backfillRequest *v1.BackfillRequest, nextStatus Phase) (reconcile.Result, error) {
	logger := s.getLogger(ctx, definition.NamespacedName())
	if definition.GetPhase() == nextStatus {
		logger.V(2).Info("Stream phase is already set to", definition.GetPhase())
		return reconcile.Result{}, nil
	}
	logger.V(1).Info("updating Stream status", "from", definition.GetPhase(), "to", nextStatus)
	err := definition.SetStatus(nextStatus)
	if err != nil {
		logger.V(0).Error(err, "unable to set Stream status")
		return reconcile.Result{}, err
	}
	statusUpdate := definition.ToUnstructured().DeepCopy()
	err = s.client.Status().Update(ctx, statusUpdate)
	if err != nil {
		logger.V(1).Error(err, "unable to update Stream status")
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil

}
