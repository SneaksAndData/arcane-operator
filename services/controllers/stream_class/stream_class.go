package stream_class

import (
	"context"
	"errors"
	"fmt"
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	runtime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sync"
)

var _ reconcile.Reconciler = (*StreamClassReconciler)(nil)

type StreamClassReconciler struct {
	client                  client.Client
	rwLock                  sync.RWMutex
	streamControllers       map[types.NamespacedName]*StreamControllerHandle
	streamControllerFactory UnmanagedControllerFactory
	reporter                StreamClassMetricsReporter
	eventRecorder           record.EventRecorder
}

func NewStreamClassReconciler(client client.Client, streamControllerFactory UnmanagedControllerFactory, reporter StreamClassMetricsReporter) *StreamClassReconciler {
	return &StreamClassReconciler{
		client:                  client,
		streamControllers:       make(map[types.NamespacedName]*StreamControllerHandle),
		streamControllerFactory: streamControllerFactory,
		reporter:                reporter,
	}
}

func (s *StreamClassReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := s.getLogger(ctx, request.NamespacedName)
	logger.Info("Reconciling StreamClass")

	sc := &v1.StreamClass{}
	err := s.client.Get(ctx, request.NamespacedName, sc)
	deleted := apierrors.IsNotFound(err)
	if client.IgnoreNotFound(err) != nil {
		logger.V(0).Error(err, "unable to get stream class")
	}

	return s.moveFsm(ctx, sc, deleted, request.NamespacedName)
}

func (s *StreamClassReconciler) SetupWithManager(mgr runtime.Manager) error {
	return runtime.NewControllerManagedBy(mgr).For(&v1.StreamClass{}).Complete(s)
}

func (s *StreamClassReconciler) getLogger(ctx context.Context, request types.NamespacedName) klog.Logger {
	return klog.FromContext(ctx).
		WithName("StreamClassReconciler").
		WithValues("name", request.Name)
}

func (s *StreamClassReconciler) moveFsm(ctx context.Context, sc *v1.StreamClass, deleted bool, name types.NamespacedName) (reconcile.Result, error) {
	logger := s.getLogger(ctx, name)
	switch {
	case deleted:
		return s.tryStopStreamController(ctx, name, nil)

	case sc.Status.Phase == "":
		return s.updatePhase(ctx, sc, name, v1.PhasePending, func() {
			s.eventRecorder.Event(sc,
				corev1.EventTypeNormal,
				"StreamClassCreated",
				"New StreamClass has been created and is pending processing")
		})
	case sc.Status.Phase == v1.PhasePending:
		return s.tryStartStreamController(ctx, sc, name, v1.PhaseReady, func() {
			s.eventRecorder.Event(sc,
				corev1.EventTypeNormal,
				"StreamClassReady",
				"StreamClass is ready and stream controller has been started")
		})
	case sc.Status.Phase == v1.PhaseFailed:
		logger.V(0).Info("Found StreamClass in Failed state, attempting to recover")
		return s.tryStartStreamController(ctx, sc, name, v1.PhaseReady, func() {
			s.eventRecorder.Event(sc,
				corev1.EventTypeWarning,
				"StreamClassRecovered",
				"StreamClass was found in Failed state and recovery was attempted")
		})

	case sc.Status.Phase == v1.PhaseReady:
		return s.tryStartStreamController(ctx, sc, name, v1.PhaseReady, func() {
			s.eventRecorder.Event(sc,
				corev1.EventTypeNormal,
				"StreamClassReconciled",
				"StreamClass is reconciled and stream controller is running")
		})
	}

	return reconcile.Result{}, fmt.Errorf("failed to reconcile StreamClass FSM for %s. Current state: %s",
		name.Name,
		sc.StateString(),
	)
}

func (s *StreamClassReconciler) tryStartStreamController(ctx context.Context, sc *v1.StreamClass, name types.NamespacedName, nextPhase v1.Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()

	logger := s.getLogger(ctx, name)

	_, ok := s.streamControllers[name]
	if ok {
		logger.V(0).Info("Stream controller is already running")
		return s.updatePhase(ctx, sc, name, nextPhase, eventFunc)
	}

	controller, err := s.streamControllerFactory.CreateStreamController(ctx, sc.TargetResourceGvk(), sc)

	if err != nil {
		logger.V(0).Error(err, "unable to create stream reconciler")
		return s.updatePhase(ctx, sc, name, v1.PhaseFailed, eventFunc)
	}

	controllerContext, cancelFunc := context.WithCancel(ctx)
	go func() {
		err := controller.Start(controllerContext)
		if errors.Is(err, context.Canceled) {
			logger.V(1).Info("stream controller is stopped")
			return
		}
		if err != nil {
			logger := s.getLogger(ctx, name)
			logger.V(0).Error(err, "stream controller exited with error")

			_, err = s.updatePhase(ctx, sc, name, v1.PhaseFailed, func() {
				s.eventRecorder.Event(sc,
					corev1.EventTypeWarning,
					"StreamControllerError",
					"Stream controller exited with error, StreamClass moved to Failed state")
			})
			if err != nil {
				logger := s.getLogger(ctx, name)
				logger.V(0).Error(err, "unable to update StreamClass phase to Failed after stream controller exited with error")
			}
		}
	}()

	logger.V(1).Info("Stream controller is started")
	s.streamControllers[name] = &StreamControllerHandle{
		cancelFunc: cancelFunc,
		gvk:        sc.TargetResourceGvk(),
	}
	s.reporter.AddStreamClass(sc.TargetResourceGvk().Kind, "stream_class", sc.MetricsTags())

	return s.updatePhase(ctx, sc, name, nextPhase, eventFunc)
}

func (s *StreamClassReconciler) tryStopStreamController(ctx context.Context, name types.NamespacedName, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()

	logger := s.getLogger(ctx, name)
	_, ok := s.streamControllers[name]
	if !ok {
		logger.V(2).Info("Stream controller is not running")
		return reconcile.Result{}, nil
	}
	s.streamControllers[name].cancelFunc()
	logger.V(2).Info("Stream controller is stopped")
	s.reporter.RemoveStreamClass(s.streamControllers[name].gvk.Kind)

	delete(s.streamControllers, name)
	return s.updatePhase(ctx, nil, name, v1.PhaseStopped, eventFunc)
}

func (s *StreamClassReconciler) updatePhase(ctx context.Context, sc *v1.StreamClass, name types.NamespacedName, nextPhase v1.Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	logger := s.getLogger(ctx, name)
	if sc == nil {
		logger.V(0).Info("Stream class is deleted, skipping phase update")
		return reconcile.Result{}, nil
	}

	if sc.Status.Phase == nextPhase {
		logger.V(2).Info("StreamClass phase is already set to", sc.Status.Phase)
		return reconcile.Result{}, nil
	}

	logger.V(1).Info("Updating StreamClass phase", "from", sc.Status.Phase, "to", nextPhase)
	sc.Status.Phase = nextPhase
	err := s.client.Status().Update(ctx, sc)
	if client.IgnoreNotFound(err) != nil {
		logger.V(1).Error(err, "unable to update Stream Class status")
		return reconcile.Result{}, err
	}

	if eventFunc != nil {
		eventFunc()
	}
	return reconcile.Result{}, nil
}
