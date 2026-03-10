package stream

import (
	"context"
	"fmt"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
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
	gvk                            schema.GroupVersionKind
	client                         client.Client
	jobBuilder                     JobBuilder
	streamClass                    *v1.StreamClass
	eventRecorder                  record.EventRecorder
	definitionParser               DefinitionParser
	backendResourceManagers        map[Backend]BackendResourceManager
	backfillBackendResourceManager *BackfillBackendResourceManager
}

func (s *streamReconciler) SetupUnmanaged(cache cache.Cache, scheme *runtime.Scheme, mapper meta.RESTMapper) (controller.Controller, error) { // coverage-ignore (setup is not tested in unit tests)
	controllerName := s.streamClass.Name + "-controller"
	newController, err := controller.NewUnmanaged(controllerName, controller.Options{Reconciler: s})

	if err != nil {
		return nil, fmt.Errorf("failed to start unmanaged stream controller: %w", err)
	}

	for backend, manager := range s.backendResourceManagers {
		err = manager.SetupWithController(cache, scheme, mapper, newController, s.gvk)
		if err != nil {
			return nil, fmt.Errorf("failed to start backend resource watcher for backend %s: %w", backend, err)
		}
	}
	resource := &unstructured.Unstructured{}
	resource.SetGroupVersionKind(s.gvk)
	newSource := source.Kind(cache, resource, &handler.TypedEnqueueRequestForObject[*unstructured.Unstructured]{})

	err = newController.Watch(newSource)
	if err != nil {
		return nil, fmt.Errorf("failed to watch stream resource: %w", err)
	}

	err = s.backfillBackendResourceManager.SetupWithController(cache, scheme, mapper, newController, s.gvk)
	if err != nil {
		return nil, fmt.Errorf("failed to watch backfills: %w", err)
	}

	return newController, nil
}

// NewStreamReconciler creates a new StreamReconciler instance.
func NewStreamReconciler(client client.Client, gvk schema.GroupVersionKind, jobBuilder JobBuilder, streamClass *v1.StreamClass, eventRecorder record.EventRecorder, definitionParser DefinitionParser, managers map[Backend]BackendResourceManager, backfillResourceManager *BackfillBackendResourceManager) controllers.UnmanagedReconciler {
	return &streamReconciler{
		gvk:                            gvk,
		jobBuilder:                     jobBuilder,
		client:                         client,
		streamClass:                    streamClass,
		eventRecorder:                  eventRecorder,
		definitionParser:               definitionParser,
		backendResourceManagers:        managers,
		backfillBackendResourceManager: backfillResourceManager,
	}
}

// Reconcile implements the reconciliation loop for Stream resources.
func (s *streamReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := s.getLogger(ctx, request.NamespacedName)
	logger.V(0).Info("Reconciling the Stream resource")

	streamDefinition, err := GetStreamForClass(ctx, s.client, s.streamClass, request.NamespacedName, s.definitionParser)

	if errors.IsNotFound(err) { // coverage-ignore
		logger.V(0).Info("stream resource not found, might have been deleted")
		return reconcile.Result{}, nil
	}

	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to fetch Stream resource")
		return reconcile.Result{}, err
	}

	backfillRequest, err := s.backfillBackendResourceManager.GetBackfillRequest(ctx, streamDefinition)
	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to fetch BackfillRequest for the Stream, cannot proceed")
		return reconcile.Result{}, err
	}

	streamingJob, err := s.backendResourceManagers[BatchJob].Get(ctx, request.NamespacedName)
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to fetch streaming job for the Stream, cannot proceed")
		return reconcile.Result{}, err
	}

	return s.moveFsm(ctx, streamDefinition, streamingJob, backfillRequest)
}

func (s *streamReconciler) moveFsm(ctx context.Context, definition Definition, job *StreamingJob, backfillRequest *v1.BackfillRequest) (reconcile.Result, error) {
	phase := definition.GetPhase()

	switch {
	case phase == Backfilling && job != nil && job.IsFailed():
		return s.backfillBackendResourceManager.Remove(ctx, definition, Failed, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Warning",
				"StreamingJobFailed",
				"The backfill job %s has failed", job.Name)
		})

	case job != nil && job.IsFailed():
		return s.backendResourceManagers[definition.GetBackend()].Remove(ctx, definition, Failed, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Warning",
				"StreamingJobFailed",
				"The streaming job %s has failed", job.Name)
		})

	case phase == Failed && definition.Suspended() && backfillRequest != nil:
		return s.backfillBackendResourceManager.Remove(ctx, definition, Suspended, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"StreamSuspended",
				"The stream was suspended")
		})

	case phase == Failed && definition.Suspended() && backfillRequest == nil:
		return s.backendResourceManagers[definition.GetBackend()].Remove(ctx, definition, Suspended, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"StreamSuspended",
				"The stream was suspended")
		})

	case phase == Failed && !definition.Suspended() && backfillRequest != nil:
		return s.backendResourceManagers[definition.GetBackend()].Apply(ctx, definition, backfillRequest, Backfilling, s.streamClass, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"BackfillRequested",
				"Backfill was requested for the new stream definition: %s", definition.NamespacedName().Name)
		})

	case phase == Failed:
		return s.backendResourceManagers[definition.GetBackend()].Remove(ctx, definition, Failed, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Warning",
				"StreamingJobFailed",
				"The stream %s has failed", definition.NamespacedName().Name)
		})

	case phase == New && definition.Suspended():
		return s.backendResourceManagers[definition.GetBackend()].Remove(ctx, definition, Suspended, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"StreamSuspended",
				"The new stream %s was added in the suspended state, nothing to do", definition.NamespacedName().Name)
		})

	case phase == New && !definition.Suspended():
		return s.backfillBackendResourceManager.Apply(ctx, definition, s.newBackfillRequest(definition), Pending, s.streamClass, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"StreamCreated",
				"Backfill was requested for the new stream definition: %s", definition.NamespacedName().Name)
		})

	case phase == Pending && backfillRequest == nil:
		return s.backendResourceManagers[definition.GetBackend()].Apply(ctx, definition, nil, Running, s.streamClass, nil)

	case phase == Pending && backfillRequest != nil:
		return s.backendResourceManagers[definition.GetBackend()].Apply(ctx, definition, backfillRequest, Backfilling, s.streamClass, nil)

	case phase == Running && definition.Suspended():
		return s.backendResourceManagers[definition.GetBackend()].Remove(ctx, definition, Suspended, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"StreamSuspended",
				"The streaming job for stream %s has been suspended", definition.NamespacedName().Name)
		})

	case phase == Running && backfillRequest != nil:
		return s.backendResourceManagers[definition.GetBackend()].Remove(ctx, definition, Pending, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"BackfillRequested",
				"A backfill requested for stream %s, stopping the streaming job to start backfilling", definition.NamespacedName().Name)
		})

	case phase == Running && backfillRequest == nil:
		return s.backendResourceManagers[definition.GetBackend()].Apply(ctx, definition, backfillRequest, Running, s.streamClass, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"StreamingContinued",
				"The streaming job for stream %s is continuing", definition.NamespacedName().Name)
		})

	case phase == Suspended && backfillRequest != nil:
		return s.backendResourceManagers[definition.GetBackend()].NoOp(ctx, definition, backfillRequest, Pending, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"BackfillRequested",
				"A backfill requested for suspended stream %s", definition.NamespacedName().Name)
		})

	case phase == Suspended && backfillRequest == nil && definition.Suspended():
		return s.backendResourceManagers[definition.GetBackend()].Remove(ctx, definition, Suspended, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"StreamSuspended",
				"The stream %s remains suspended", definition.NamespacedName().Name)
		})

	case phase == Suspended && !definition.Suspended():
		return s.backendResourceManagers[definition.GetBackend()].NoOp(ctx, definition, backfillRequest, Pending, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"StreamResumed",
				"The stream %s has been resumed", definition.NamespacedName().Name)
		})

	case phase == Backfilling && definition.Suspended():
		return s.backfillBackendResourceManager.Remove(ctx, definition, Suspended, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"StreamSuspended",
				"The backfilling for stream %s has been suspended", definition.NamespacedName().Name)
		})

	case phase == Backfilling && backfillRequest == nil:
		return s.backfillBackendResourceManager.Remove(ctx, definition, Pending, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"BackfillNotRequested",
				"The backfill request for stream %s not found, probably deleted", definition.NamespacedName().Name)
		})

	case phase == Backfilling && job == nil:
		return s.backendResourceManagers[definition.GetBackend()].Apply(ctx, definition, backfillRequest, Backfilling, s.streamClass, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"BackfillStarted",
				"Backfill job for stream %s has been started", definition.NamespacedName().Name)
		})

	case phase == Backfilling && job.IsCompleted():
		return s.backfillBackendResourceManager.Remove(ctx, definition, Pending, func() {
			s.eventRecorder.Eventf(definition.ToUnstructured(),
				"Normal",
				"BackfillCompleted",
				"Backfill for stream %s has been completed", definition.NamespacedName().Name)
		})

	case phase == Backfilling && !job.IsCompleted():
		return s.backendResourceManagers[definition.GetBackend()].NoOp(ctx, definition, backfillRequest, Backfilling, func() {
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

func (s *streamReconciler) newBackfillRequest(definition Definition) *v1.BackfillRequest {
	return &v1.BackfillRequest{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-initial-backfill-", definition.NamespacedName().Name),
			Namespace:    definition.NamespacedName().Namespace,
		},
		Spec: v1.BackfillRequestSpec{
			StreamId:    definition.NamespacedName().Name,
			StreamClass: s.streamClass.Name,
		},
	}
}

func (s *streamReconciler) getLogger(_ context.Context, request types.NamespacedName) klog.Logger {
	return klog.Background().
		WithName("StreamReconciler").
		WithValues("namespace", request.Namespace, "streamId", request.Name, "streamKind", s.gvk.Kind)
}
