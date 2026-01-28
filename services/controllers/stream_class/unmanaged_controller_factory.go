package stream_class

import (
	"context"
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/controller"
)

// UnmanagedControllerFactory is an interface for creating unmanaged controllers for different GVKs.
// This allows for dynamic creation of controllers based on the stream class specifications.
type UnmanagedControllerFactory interface {

	// CreateStreamController creates an unmanaged controller for the given GroupVersionKind (GVK).
	CreateStreamController(ctx context.Context, gvk schema.GroupVersionKind, streamClass *v1.StreamClass) (controller.Controller, error)
}
