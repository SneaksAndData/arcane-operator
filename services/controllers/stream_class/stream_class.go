package stream_class

import (
	"context"
	"errors"
	"fmt"
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	runtime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sync"
	"time"
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

func NewStreamClassReconciler(client client.Client, streamControllerFactory UnmanagedControllerFactory, reporter StreamClassMetricsReporter, eventRecorder record.EventRecorder) *StreamClassReconciler {
	return &StreamClassReconciler{
		client:                  client,
		streamControllers:       make(map[types.NamespacedName]*StreamControllerHandle),
		streamControllerFactory: streamControllerFactory,
		reporter:                reporter,
		eventRecorder:           eventRecorder,
	}
}

func (s *StreamClassReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := s.getLogger(ctx, request.NamespacedName)
	logger.Info("Reconciling StreamClass")

	sc := &v1.StreamClass{}
	err := s.client.Get(ctx, request.NamespacedName, sc)
	deleted := apierrors.IsNotFound(err)
	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to get stream class")
	}

	return s.moveFsm(ctx, sc, deleted, request.NamespacedName)
}

func (s *StreamClassReconciler) SetupWithManager(mgr runtime.Manager) error { // coverage-ignore (should be tested in e2e)
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
	case sc.Status.Phase == v1.PhaseFailed && sc.Status.ReconcileAfter == nil:
		logger.V(0).Info("StreamClass is in Failed state with no scheduled retry, not attempting to recover")
		return reconcile.Result{}, nil

	case sc.Status.Phase == v1.PhaseFailed && sc.Status.ReconcileAfter != nil:
		logger.V(0).Info("Found StreamClass in Failed state, attempting to recover")
		t := time.Until(*sc.Status.ReconcileAfter)
		if t <= 0 {
			logger.V(0).Info("ReconcileAfter time has passed, attempting to restart stream controller")
			return s.tryStartStreamController(ctx, sc, name, v1.PhaseReady, func() {
				s.eventRecorder.Event(sc,
					corev1.EventTypeNormal,
					"StreamClassRecovered",
					"StreamClass has been recovered from Failed state and stream controller has been restarted")
			})
		}
		logger.V(0).Info("StreamClass is in Failed state, waiting until ReconcileAfter time to attempt recovery",
			"reconcileAfter",
			sc.Status.ReconcileAfter)
		return reconcile.Result{RequeueAfter: t}, nil

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
			logger.V(0).Info("stream controller is stopped")
			return
		}
		if err != nil {
			logger := s.getLogger(ctx, name)
			logger.V(0).Error(err, "stream controller exited with error")

			if apierrors.IsForbidden(err) {
				err = s.setForRetry(ctx, sc, name, logger)
				return
			}

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

	logger.V(0).Info("Stream controller is started")
	s.streamControllers[name] = &StreamControllerHandle{
		cancelFunc: cancelFunc,
		gvk:        sc.TargetResourceGvk(),
	}
	s.reporter.AddStreamClass(sc.TargetResourceGvk().Kind, "stream_class", sc.MetricsTags())

	return s.updatePhase(ctx, sc, name, nextPhase, eventFunc)
}

func (s *StreamClassReconciler) setForRetry(ctx context.Context, sc *v1.StreamClass, name types.NamespacedName, logger klog.Logger) error {
	now := metav1.Now().Add(time.Second * 10) // TODO: make the retry delay configurable and implement exponential backoff
	sc.Status.ReconcileAfter = &now
	_, err := s.updatePhase(ctx, sc, name, v1.PhaseFailed, func() {
		s.eventRecorder.Eventf(sc,
			corev1.EventTypeWarning,
			"StreamControllerError",
			"Stream controller exited with error, next reconcile attempt at %s, StreamClass moved to Failed state", sc.Status.ReconcileAfter.Format(time.RFC3339))
	})
	if err != nil {
		logger.V(0).Error(err, "unable to update StreamClass phase to Failed after stream controller exited with error")
	}
	return err
}

func (s *StreamClassReconciler) tryStopStreamController(ctx context.Context, name types.NamespacedName, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()

	logger := s.getLogger(ctx, name)
	_, ok := s.streamControllers[name]
	if !ok {
		logger.V(0).Info("Stream controller is not running")
		return reconcile.Result{}, nil
	}
	s.streamControllers[name].cancelFunc()
	logger.V(0).Info("Stream controller is stopped")
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
		logger.V(0).Info("StreamClass phase is already set to", sc.Status.Phase)
		return reconcile.Result{}, nil
	}

	logger.V(0).Info("Updating StreamClass phase", "from", sc.Status.Phase, "to", nextPhase)
	sc.Status.Phase = nextPhase
	err := s.client.Status().Update(ctx, sc)

	if eventFunc != nil {
		eventFunc()
	}

	if client.IgnoreNotFound(err) != nil {
		logger.V(0).Error(err, "unable to update Stream Class status")
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}
