package controllers

import (
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/controller"
)

type UnmanagedReconciler interface {
	SetupUnmanaged(cache cache.Cache, scheme *runtime.Scheme, mapper meta.RESTMapper) (controller.Controller, error)
}
