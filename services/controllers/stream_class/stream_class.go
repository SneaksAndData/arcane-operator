package stream_class

import (
	"context"
	"fmt"
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	runtime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ reconcile.Reconciler = (*StreamClassReconciler)(nil)

type StreamClassReconciler struct {
	client                  client.Client
	streamControllers       map[types.NamespacedName]*StreamControllerHandle
	streamControllerFactory UnmanagedControllerFactory
}

func NewStreamClassReconciler(client client.Client, streamControllerFactory UnmanagedControllerFactory) *StreamClassReconciler {
	return &StreamClassReconciler{
		client:                  client,
		streamControllers:       make(map[types.NamespacedName]*StreamControllerHandle),
		streamControllerFactory: streamControllerFactory,
	}
}

func (s *StreamClassReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	logger := s.getLogger(ctx, request.NamespacedName)
	logger.Info("Reconciling StreamClass")

	sc := &v1.StreamClass{}
	err := s.client.Get(ctx, request.NamespacedName, sc)
	deleted := errors.IsNotFound(err)
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
	switch {
	case !deleted && sc.Status.Phase == "":
		return s.updatePhase(ctx, sc, name, v1.PhasePending)
	case !deleted && (sc.Status.Phase == v1.PhasePending || sc.Status.Phase == v1.PhaseReady):
		return s.tryStartStreamController(ctx, sc, name, v1.PhaseReady)
	case deleted:
		return s.tryStopStreamController(ctx, name)
	}

	return reconcile.Result{}, fmt.Errorf("failed to reconcile StreamClass FSM for %s. Current state: %s",
		name.Name,
		sc.StateString(),
	)
}

func (s *StreamClassReconciler) tryStartStreamController(ctx context.Context, sc *v1.StreamClass, name types.NamespacedName, nextPhase v1.Phase) (reconcile.Result, error) {
	logger := s.getLogger(ctx, name)

	_, ok := s.streamControllers[name]
	if ok {
		logger.V(2).Info("Stream controller is already running")
		return s.updatePhase(ctx, sc, name, nextPhase)
	}

	controller, err := s.streamControllerFactory.CreateStreamController(ctx, sc.TargetResourceGvk())

	if err != nil {
		logger.V(0).Error(err, "unable to create stream reconciler")
		return s.updatePhase(ctx, sc, name, v1.PhaseFailed)
	}

	controllerContext, cancelFunc := context.WithCancel(context.Background())
	go func() {
		err := controller.Start(controllerContext)
		if err != nil {
			logger := s.getLogger(ctx, name)
			logger.V(0).Error(err, "stream controller exited with error")

			_, err = s.updatePhase(ctx, sc, name, v1.PhaseFailed)
			if err != nil {
				logger := s.getLogger(ctx, name)
				logger.V(0).Error(err, "unable to update StreamClass phase to Failed after stream controller exited with error")
			}
		}
	}()

	s.streamControllers[name] = &StreamControllerHandle{cancelFunc: cancelFunc}
	return s.updatePhase(ctx, sc, name, nextPhase)
}

func (s *StreamClassReconciler) tryStopStreamController(ctx context.Context, name types.NamespacedName) (reconcile.Result, error) {
	logger := s.getLogger(ctx, name)
	_, ok := s.streamControllers[name]
	if !ok {
		logger.V(2).Info("Stream controller is not running")
		return reconcile.Result{}, nil
	}
	s.streamControllers[name].cancelFunc()
	logger.V(2).Info("Stream controller is stopped")
	return s.updatePhase(ctx, nil, name, v1.PhaseStopped)
}

func (s *StreamClassReconciler) updatePhase(ctx context.Context, sc *v1.StreamClass, name types.NamespacedName, nextPhase v1.Phase) (reconcile.Result, error) {
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
	return reconcile.Result{}, nil
}
