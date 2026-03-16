package services

import (
	"context"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend/cron_job"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend/job"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream_class"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

var _ stream_class.UnmanagedControllerFactory = (*streamControllerFactory)(nil)

type streamControllerFactory struct {
	client           client.Client
	jobBuilder       stream.JobBuilder
	manager          manager.Manager
	eventRecorder    record.EventRecorder
	definitionParser stream.DefinitionParser
}

func (s streamControllerFactory) CreateStreamController(_ context.Context, gvk schema.GroupVersionKind, streamClass *v1.StreamClass) (controller.Controller, error) { // coverage-ignore (trivial)
	statusManager := stream.NewDefaultStatusManager(s.client, gvk, streamClass, s.definitionParser)
	backfillBackend := job.NewBackfillBackendResourceManager(streamClass, s.client, statusManager, s.eventRecorder)
	backends := map[stream.Backend]stream.BackendResourceManager{
		stream.BatchJob: job.NewJobBackend(s.client, s.jobBuilder, s.eventRecorder, statusManager),
		stream.CronJob:  cron_job.NewCronJobBackend(s.client, s.jobBuilder, s.eventRecorder, statusManager),
	}
	streamReconciler := stream.NewStreamReconciler(s.client, gvk, s.jobBuilder, streamClass, s.eventRecorder, s.definitionParser, backends, backfillBackend)
	unmanaged, err := streamReconciler.SetupUnmanaged(s.manager.GetCache(), s.manager.GetScheme(), s.manager.GetRESTMapper())
	return unmanaged, err
}

// NewStreamControllerFactory creates a new instance of StreamControllerFactory
func NewStreamControllerFactory(client client.Client, jobBuilder stream.JobBuilder, manager manager.Manager, eventRecorder record.EventRecorder, definitionParser stream.DefinitionParser) stream_class.UnmanagedControllerFactory { // coverage-ignore (trivial)
	return &streamControllerFactory{
		client:           client,
		jobBuilder:       jobBuilder,
		manager:          manager,
		eventRecorder:    eventRecorder,
		definitionParser: definitionParser,
	}
}
