package controllers

import (
	"context"
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ reconcile.Reconciler = NewStreamClassReconciler()

type StreamClassReconciler struct {
}

func NewStreamClassReconciler() *StreamClassReconciler {
	return &StreamClassReconciler{}
}

func (s StreamClassReconciler) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	//TODO implement me
	panic("implement me")
}

func (s StreamClassReconciler) SetupWithManager(mgr controllerruntime.Manager) error {
	return controllerruntime.NewControllerManagedBy(mgr).For(&v1.StreamClass{}).Complete(s)

}
