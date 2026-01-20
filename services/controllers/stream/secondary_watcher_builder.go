package stream

import (
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// TypedSecondaryWatcherBuilder builds a TypedSecondaryWatcher instance
type TypedSecondaryWatcherBuilder[object client.Object] struct {
	cache   cache.Cache
	handler handler.TypedEventHandler[object, reconcile.Request]
	filter  predicate.TypedPredicate[object]
}

// NewTypedSecondaryWatcherBuilder creates a new builder for TypedSecondaryWatcher
func NewTypedSecondaryWatcherBuilder[object client.Object]() *TypedSecondaryWatcherBuilder[object] { // coverage-ignore (trivial)
	return &TypedSecondaryWatcherBuilder[object]{}
}

// WithCache sets the cache for the watcher
func (b *TypedSecondaryWatcherBuilder[object]) WithCache(cache cache.Cache) *TypedSecondaryWatcherBuilder[object] { // coverage-ignore (trivial)
	b.cache = cache
	return b
}

// WithHandler sets the event handler for the watcher
func (b *TypedSecondaryWatcherBuilder[object]) WithHandler(handler handler.TypedEventHandler[object, reconcile.Request]) *TypedSecondaryWatcherBuilder[object] { // coverage-ignore (trivial)
	b.handler = handler
	return b
}

// WithFilter sets the predicate filter for the watcher
func (b *TypedSecondaryWatcherBuilder[object]) WithFilter(filter predicate.TypedPredicate[object]) *TypedSecondaryWatcherBuilder[object] { // coverage-ignore (trivial)
	b.filter = filter
	return b
}

// Build creates the TypedSecondaryWatcher instance
func (b *TypedSecondaryWatcherBuilder[object]) Build() *TypedSecondaryWatcher[object] { // coverage-ignore (trivial)
	return &TypedSecondaryWatcher[object]{
		cache:   b.cache,
		handler: b.handler,
		filter:  b.filter,
	}
}
