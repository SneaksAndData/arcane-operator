package backend

import "sigs.k8s.io/controller-runtime/pkg/event"

type SecondaryResourcePredicate[object any] struct{}

func (b *SecondaryResourcePredicate[object]) Create(_ event.TypedCreateEvent[object]) bool {
	return true
}

func (b *SecondaryResourcePredicate[object]) Delete(_ event.TypedDeleteEvent[object]) bool {
	return true
}

func (b *SecondaryResourcePredicate[object]) Generic(_ event.TypedGenericEvent[object]) bool {
	return false
}

func (b *SecondaryResourcePredicate[object]) Update(_ event.TypedUpdateEvent[object]) bool {
	return true
}
