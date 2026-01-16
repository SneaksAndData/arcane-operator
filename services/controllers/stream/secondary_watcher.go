package stream

import (
	"fmt"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// TypedSecondaryWatcher watches secondary resources and enqueues reconcile requests for the primary resource.
type TypedSecondaryWatcher[object client.Object] struct {
	cache   cache.Cache
	handler handler.TypedEventHandler[object, reconcile.Request]
	filter  predicate.TypedPredicate[object]
}

// SetupWithController sets up the watcher with the given controller
func (w *TypedSecondaryWatcher[object]) SetupWithController(controller controller.Controller, obj object) error {
	err := controller.Watch(source.Kind(w.cache, obj, w.handler, w.filter))
	if err != nil {
		return fmt.Errorf("failed to watch backfills: %w", err)
	}
	return nil
}
