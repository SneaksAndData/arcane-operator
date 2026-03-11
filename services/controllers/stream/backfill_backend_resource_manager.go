package stream

import (
	"context"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
)

type BackfillBackendResourceManager interface {
	BackendResourceManager

	// GetBackfillRequest returns the current backfill request associated with the given stream definition, if any.
	GetBackfillRequest(ctx context.Context, definition Definition) (*v1.BackfillRequest, error)
}
