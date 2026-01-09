package controllers

import (
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// UnmanagedReconciler is an extension of the standard Reconciler interface
// that includes a method for setting up unmanaged controllers.
// Unmanaged controllers are not automatically started by the manager.
// See: https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/controller#NewUnmanaged
// and https://github.com/kubernetes-sigs/controller-runtime/issues/730
// for more details.
type UnmanagedReconciler interface {
	reconcile.Reconciler

	// SetupUnmanaged sets up the unmanaged controller with the provided cache, scheme, and REST mapper.
	SetupUnmanaged(cache cache.Cache, scheme *runtime.Scheme, mapper meta.RESTMapper) (controller.Controller, error)
}
