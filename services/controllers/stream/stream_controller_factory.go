package stream

import (
	"context"
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream_class"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

var _ stream_class.UnmanagedControllerFactory = (*streamControllerFactory)(nil)

type streamControllerFactory struct {
	client        client.Client
	jobBuilder    JobBuilder
	manager       manager.Manager
	eventRecorder record.EventRecorder
}

func (s streamControllerFactory) CreateStreamController(_ context.Context, gvk schema.GroupVersionKind, streamClass *v1.StreamClass) (controller.Controller, error) { // coverage-ignore (trivial)
	streamReconciler := NewStreamReconciler(s.client, gvk, s.jobBuilder, streamClass, s.eventRecorder)
	unmanaged, err := streamReconciler.SetupUnmanaged(s.manager.GetCache(), s.manager.GetScheme(), s.manager.GetRESTMapper())
	return unmanaged, err
}

// NewStreamControllerFactory creates a new instance of StreamControllerFactory
func NewStreamControllerFactory(client client.Client, jobBuilder JobBuilder, manager manager.Manager, eventRecorder record.EventRecorder) stream_class.UnmanagedControllerFactory { // coverage-ignore (trivial)
	return &streamControllerFactory{
		client:        client,
		jobBuilder:    jobBuilder,
		manager:       manager,
		eventRecorder: eventRecorder,
	}
}
