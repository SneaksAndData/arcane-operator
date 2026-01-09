package stream

import (
	"context"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream_class"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

var _ stream_class.UnmanagedControllerFactory = (*streamControllerFactory)(nil)

type streamControllerFactory struct {
	client     client.Client
	jobBuilder JobBuilder
	className  string
	manager    manager.Manager
}

func (s streamControllerFactory) CreateStreamController(_ context.Context, gvk schema.GroupVersionKind) (controller.Controller, error) {
	streamReconciler := NewStreamReconciler(s.client, gvk, s.jobBuilder, s.className)
	unmanaged, err := streamReconciler.SetupUnmanaged(s.manager.GetCache(), s.manager.GetScheme(), s.manager.GetRESTMapper())
	return unmanaged, err
}

// NewStreamControllerFactory creates a new instance of StreamControllerFactory
func NewStreamControllerFactory(client client.Client, jobBuilder JobBuilder, className string, manager manager.Manager) stream_class.UnmanagedControllerFactory {
	return &streamControllerFactory{
		client:     client,
		jobBuilder: jobBuilder,
		className:  className,
		manager:    manager,
	}
}
