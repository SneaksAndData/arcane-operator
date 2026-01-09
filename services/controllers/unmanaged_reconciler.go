package controllers

import (
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type UnmanagedReconciler interface {
	reconcile.Reconciler
	SetupUnmanaged(cache cache.Cache, scheme *runtime.Scheme, mapper meta.RESTMapper) (controller.Controller, error)
}
