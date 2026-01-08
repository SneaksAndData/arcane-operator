package stream_class

import (
	"context"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/controller"
)

type UnmanagedControllerFactory interface {
	CreateStreamController(ctx context.Context, gvk schema.GroupVersionKind) (controller.Controller, error)
}
