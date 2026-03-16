package backend

import (
	"context"
	"fmt"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/job"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type BaseResourceManager struct {
	Client        client.Client
	JobBuilder    stream.JobBuilder
	EventRecorder record.EventRecorder
}

func (j *BaseResourceManager) Remove(ctx context.Context, object client.Object, updatePhase func() (reconcile.Result, error)) (reconcile.Result, error) {
	err := j.Client.Delete(ctx, object, client.PropagationPolicy(metav1.DeletePropagationForeground))
	if client.IgnoreNotFound(err) != nil {
		return reconcile.Result{}, err
	}

	if updatePhase != nil {
		return updatePhase()
	}
	return reconcile.Result{}, nil
}

func (j *BaseResourceManager) BuildJob(ctx context.Context, definition stream.Definition, request *v1.BackfillRequest, streamClass *v1.StreamClass) (*batchv1.Job, error) {
	logger := klog.FromContext(ctx)

	templateReference := definition.GetJobTemplate(request)

	streamConfiguration, err := definition.CurrentConfiguration(request)
	if err != nil { // coverage-ignore
		return nil, fmt.Errorf("failed to compute stream configuration: %w", err)
	}

	definitionConfigurator, err := definition.JobConfigurator()
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "failed to get job configurator from stream definition")
		return nil, err
	}

	secretsConfigurator, err := stream.NewStreamMetadataService(streamClass, definition).JobConfigurator()
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "failed to get metadata configurator")
		return nil, err
	}

	backfillRequestConfigurator, err := request.JobConfigurator()
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "failed to get backfill request configurator")
		return nil, err
	}

	combinedConfigurator := job.NewConfiguratorChainBuilder().
		WithConfigurator(definitionConfigurator).
		WithConfigurator(backfillRequestConfigurator).
		WithConfigurator(job.NewConfigurationChecksumConfigurator(streamConfiguration)).
		WithConfigurator(secretsConfigurator)

	newJob, err := j.JobBuilder.BuildJob(ctx, templateReference, combinedConfigurator)
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "failed to build job")
		j.EventRecorder.Eventf(definition.ToUnstructured(),
			corev1.EventTypeWarning,
			"FailedCreateJob",
			"failed to create job: %v", err)
		return nil, err
	}
	return newJob, nil
}
