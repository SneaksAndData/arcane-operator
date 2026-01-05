package controllers

import (
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/controller"
)

type StreamReconciler interface {
	SetupUnmanaged(cache cache.Cache) (controller.Controller, error)
}
