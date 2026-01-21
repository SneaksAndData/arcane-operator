package stream

import (
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LifetimeService defines methods required for interaction with the user interface (conditions, status, events, etc.)
type LifetimeService interface {

	// ComputeConditions computes the conditions for the given stream definition and backfill request.
	ComputeConditions(definition Definition, bfr *v1.BackfillRequest) []metav1.Condition

	// RecordLifetimeEvent records an event for the given stream definition and backfill request.
	RecordLifetimeEvent(definition Definition, bfr *v1.BackfillRequest)
}
