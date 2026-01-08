package stream_class

import (
	"context"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type StreamReconcilerFactory interface {
	CreateStreamReconciler(ctx context.Context, gvk schema.GroupVersionKind) (controllers.UnmanagedReconciler, error)
}
