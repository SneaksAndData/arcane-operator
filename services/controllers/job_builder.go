package controllers

import (
	"context"
	"fmt"
	streamingv1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	clientset "github.com/SneaksAndData/arcane-operator/pkg/generated/clientset/versioned"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type JobBuilder interface {
	BuildJob(ctx context.Context, definition StreamDefinition, backfillRequest *streamingv1.BackfillRequest) (batchv1.Job, error)
}

type SimpleJobBuilder struct {
	client clientset.Interface
}

func (s *SimpleJobBuilder) BuildJob(ctx context.Context, definition StreamDefinition, backfillRequest *streamingv1.BackfillRequest) (*batchv1.Job, error) {
	var name types.NamespacedName
	var err error
	if backfillRequest != nil {
		name, err = definition.GetBackfillJobName()
		if err != nil {
			return nil, fmt.Errorf("backfill request could not be found in backfill job definition: %w", err)
		}
	} else {
		name, err = definition.GetStreamingJobName()
		if err != nil {
			return nil, fmt.Errorf("streaming job definition could not be found in backfill job definition: %w", err)
		}
	}

	jobTemplate, err := s.client.StreamingV1().StreamingJobTemplates(name.Namespace).Get(ctx, name.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("error getting job template definition: %w", err)
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      definition.NamespacedName().Name,
			Namespace: definition.NamespacedName().Namespace,
		},
		Spec: jobTemplate.Spec,
	}

	err = definition.JobConfigurator().ConfigureJob(job)
	if err != nil {
		return nil, fmt.Errorf("error enriching job spec: %w", err)
	}

	if backfillRequest != nil {
		err = backfillRequest.ConfigureJob(job)
		if err != nil {
			return nil, fmt.Errorf("error configuring backfill job: %w", err)
		}
	}

	ownerReference, err := definition.ToOwnerReference()
	if err != nil {
		return nil, err
	}
	job.SetOwnerReferences([]metav1.OwnerReference{
		ownerReference,
	})

	return job, nil
}
