package job_builder

import (
	"context"
	"fmt"
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/job"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ stream.JobBuilder = &DefaultJobBuilder{}

// DefaultJobBuilder is the default implementation of the JobBuilder interface.
type DefaultJobBuilder struct {
	client client.Client
}

func (d DefaultJobBuilder) BuildJob(ctx context.Context, templateName types.NamespacedName, configurator job.Configurator) (*batchv1.Job, error) {
	template := v1.StreamingJobTemplate{}
	err := d.client.Get(ctx, templateName, &template)
	if err != nil {
		return nil, fmt.Errorf("failed to get template (%s): %w", templateName.String(), err)
	}

	extractedJob := &template.Spec
	err = configurator.ConfigureJob(extractedJob)
	if err != nil {
		return nil, fmt.Errorf("failed to configure template (%s): %w", templateName.String(), err)
	}

	return extractedJob, nil
}
