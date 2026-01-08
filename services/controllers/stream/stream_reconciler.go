package stream

import (
	"context"
	"fmt"
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
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
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var (
	_ reconcile.Reconciler            = (*streamReconciler)(nil)
	_ controllers.UnmanagedReconciler = (*streamReconciler)(nil)
)

type streamReconciler struct {
	gvk        schema.GroupVersionKind
	client     client.Client
	jobBuilder JobBuilder
	className  string
}

func (s *streamReconciler) SetupUnmanaged(cache cache.Cache, scheme *runtime.Scheme, mapper meta.RESTMapper) (controller.Controller, error) {
	resource := &unstructured.Unstructured{}
	resource.SetGroupVersionKind(s.gvk)

	newController, err := controller.NewUnmanaged("stream-controller", controller.Options{
		Reconciler: s,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to start unmanaged stream controller: %w", err)
	}

	// Watch for changes to primary resource Stream
	newSource := source.Kind(cache, resource, &handler.TypedEnqueueRequestForObject[*unstructured.Unstructured]{})

	err = newController.Watch(newSource)
	if err != nil {
		return nil, fmt.Errorf("failed to watch stream resource: %w", err)
	}

	// Watch for changes to secondary resource Jobs and requeue the owner Stream
	h := handler.TypedEnqueueRequestForOwner[*batchv1.Job](
		scheme,
		mapper,
		&batchv1.Job{},
		handler.OnlyControllerOwner(),
	)

	jobSource := source.Kind(cache, &batchv1.Job{}, h, nil)
	err = newController.Watch(jobSource)

	return newController, nil
}

// NewStreamReconciler creates a new StreamReconciler instance.
func NewStreamReconciler(client client.Client, gvk schema.GroupVersionKind, jobBuilder JobBuilder, className string) reconcile.Reconciler {
	return &streamReconciler{
		gvk:        gvk,
		jobBuilder: jobBuilder,
		client:     client,
		className:  className,
	}
}

// Reconcile implements the reconciliation loop for Stream resources.
func (s *streamReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := s.getLogger(ctx, request.NamespacedName)
	logger.V(2).Info("Reconciling the Stream resource")

	maybeSd := unstructured.Unstructured{}
	maybeSd.SetGroupVersionKind(s.gvk)
	err := s.client.Get(ctx, request.NamespacedName, &maybeSd)

	if errors.IsNotFound(err) { // coverage-ignore
		logger.V(1).Info("stream resource not found, might have been deleted")
		return reconcile.Result{}, nil
	}

	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		logger.V(1).Error(err, "unable to fetch Stream resource")
		return reconcile.Result{}, err
	}

	streamDefinition, err := fromUnstructured(&maybeSd)
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "failed to parse Stream definition")
		return reconcile.Result{}, err
	}

	backfillRequest, err := s.getBackfillRequest(ctx, streamDefinition)
	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		logger.V(1).Error(err, "unable to fetch BackfillRequest for the Stream, cannot proceed")
		return reconcile.Result{}, err
	}

	job := &batchv1.Job{}
	err = s.client.Get(ctx, request.NamespacedName, job)

	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		logger.V(1).Error(err, "unable to fetch Stream Job")
		return reconcile.Result{}, err
	}

	var streamingJob *StreamingJob

	if errors.IsNotFound(err) {
		streamingJob = nil
		logger.V(2).Info("streaming does not exist")
	} else {
		streamingJob = (*StreamingJob)(job)
		logger.V(2).Info("streaming job found")
	}

	return s.moveFsm(ctx, streamDefinition, streamingJob, backfillRequest)
}

func (s *streamReconciler) moveFsm(ctx context.Context, definition Definition, job *StreamingJob, backfillRequest *v1.BackfillRequest) (reconcile.Result, error) {
	phase := definition.GetPhase()

	switch {
	case job != nil && job.IsFailed():
		return s.stopStream(ctx, definition, Failed)
	case phase == Failed:
		return s.stopStream(ctx, definition, Failed)

	case phase == New && definition.Suspended():
		return s.stopStream(ctx, definition, Suspended)

	case phase == New && !definition.Suspended():
		return s.startBackfill(ctx, definition, Pending)

	case phase == Pending && backfillRequest == nil:
		return s.reconcileJob(ctx, definition, nil, Running)

	case phase == Pending && backfillRequest != nil:
		return s.reconcileJob(ctx, definition, backfillRequest, Backfilling)

	case phase == Running && definition.Suspended():
		return s.stopStream(ctx, definition, Suspended)

	case phase == Running && backfillRequest != nil:
		return s.stopStream(ctx, definition, Pending)

	case phase == Suspended && backfillRequest != nil:
		return s.updateStreamPhase(ctx, definition, backfillRequest, Pending)

	case phase == Suspended && backfillRequest == nil:
		return s.stopStream(ctx, definition, Suspended)

	case phase == Backfilling && job == nil:
		return s.reconcileJob(ctx, definition, backfillRequest, Backfilling)

	case phase == Backfilling && job.IsCompleted():
		return s.completeBackfill(ctx, job.ToV1Job(), definition, backfillRequest, Pending)

	case phase == Backfilling && !job.IsCompleted():
		return s.updateStreamPhase(ctx, definition, backfillRequest, Backfilling)

	}

	return reconcile.Result{}, fmt.Errorf("failed to reconcile Stream FSM for %s/%s. Current state: %s",
		definition.NamespacedName().Namespace,
		definition.NamespacedName().Name,
		definition.StateString(),
	)
}

func (s *streamReconciler) stopStream(ctx context.Context, definition Definition, nextPhase Phase) (reconcile.Result, error) {
	job := &batchv1.Job{}
	job.SetName(definition.NamespacedName().Name)
	job.SetNamespace(definition.NamespacedName().Namespace)
	err := s.client.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground))
	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		return reconcile.Result{}, err
	}

	return s.updateStreamPhase(ctx, definition, nil, nextPhase)
}

func (s *streamReconciler) startBackfill(ctx context.Context, definition Definition, nextPhase Phase) (reconcile.Result, error) {

	logger := s.getLogger(ctx, definition.NamespacedName())
	logger.V(2).Info("starting backfill by creating a backfill request")

	backfillRequest := &v1.BackfillRequest{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-initial-backfill-", definition.NamespacedName().Name),
			Namespace:    definition.NamespacedName().Namespace,
		},
		Spec: v1.BackfillRequestSpec{
			StreamId:    definition.NamespacedName().Name,
			StreamClass: s.className,
		},
	}

	err := s.client.Create(ctx, backfillRequest)
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "failed to create backfill request")
		return reconcile.Result{}, err
	}

	return s.updateStreamPhase(ctx, definition, backfillRequest, nextPhase)
}

func (s *streamReconciler) reconcileJob(ctx context.Context, definition Definition, backfillRequest *v1.BackfillRequest, nextPhase Phase) (reconcile.Result, error) {
	logger := s.getLogger(ctx, definition.NamespacedName())
	v1job := batchv1.Job{}
	err := s.client.Get(ctx, definition.NamespacedName(), &v1job)

	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		return reconcile.Result{}, err
	}

	templateType := services.StreamingJobTemplate
	if backfillRequest != nil {
		templateType = services.BackfillJobTemplate
	}
	configurator := definition.ToConfiguratorProvider().JobConfigurator().AddNext(backfillRequest.JobConfigurator())

	if errors.IsNotFound(err) {
		err := s.startNewJob(ctx, templateType, configurator)
		if err != nil { // coverage-ignore
			return reconcile.Result{}, err
		}
		return s.updateStreamPhase(ctx, definition, backfillRequest, nextPhase)
	}

	equals, err := s.compareConfigurations(ctx, v1job, definition)
	if err != nil { // coverage-ignore
		return reconcile.Result{}, err
	}

	if equals {
		logger.V(1).Info("Backfill job already exists with matching configuration, skipping creation")
		return s.updateStreamPhase(ctx, definition, &v1.BackfillRequest{}, nextPhase)
	}

	err = s.client.Delete(ctx, &v1job, client.PropagationPolicy(metav1.DeletePropagationBackground))
	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		return reconcile.Result{}, err
	}

	err = s.startNewJob(ctx, templateType, configurator)
	if err != nil { // coverage-ignore
		return reconcile.Result{}, err
	}
	return s.updateStreamPhase(ctx, definition, backfillRequest, nextPhase)
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

func (s *streamReconciler) completeBackfill(ctx context.Context, job *batchv1.Job, definition Definition, request *v1.BackfillRequest, nextStatus Phase) (reconcile.Result, error) {
	if job != nil {
		err := s.client.Delete(ctx, job)
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

	return s.updateStreamPhase(ctx, definition, nil, nextStatus)
}

func (s *streamReconciler) startNewJob(ctx context.Context, templateType services.JobTemplateType, configurator services.JobConfigurator) error {
	job, err := s.jobBuilder.BuildJob(ctx, templateType, configurator)
	if err != nil { // coverage-ignore
		return err
	}

	err = s.client.Create(ctx, job)
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
		if bfr.Spec.StreamId == definition.NamespacedName().Name {
			return &bfr, nil
		}
	}

	return nil, nil
}

func (s *streamReconciler) getLogger(ctx context.Context, request types.NamespacedName) klog.Logger {
	return klog.FromContext(ctx).
		WithName("StreamReconciler").
		WithValues("namespace", request.Namespace, "name", request.Name, "kind", s.gvk.Kind)
}

func (s *streamReconciler) updateStreamPhase(ctx context.Context, definition Definition, _ *v1.BackfillRequest, nextStatus Phase) (reconcile.Result, error) {
	logger := s.getLogger(ctx, definition.NamespacedName())
	if definition.GetPhase() == nextStatus { // coverage-ignore
		logger.V(2).Info("Stream phase is already set to", definition.GetPhase())
		return reconcile.Result{}, nil
	}
	logger.V(1).Info("updating Stream status", "from", definition.GetPhase(), "to", nextStatus)
	err := definition.SetPhase(nextStatus)
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to set Stream status")
		return reconcile.Result{}, err
	}
	statusUpdate := definition.ToUnstructured().DeepCopy()
	err = s.client.Status().Update(ctx, statusUpdate)
	if err != nil { // coverage-ignore
		logger.V(1).Error(err, "unable to update Stream status")
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil

}
