package backend

import (
	"context"

	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ResourceConverter func(object client.Object) (stream.BackendResource, error)

type ResourceReader struct {
	client client.Client
}

func (c *ResourceReader) Get(ctx context.Context, name client.ObjectKey, object client.Object, fromObject ResourceConverter) (stream.BackendResource, error) {
	logger := c.getLogger(ctx, name)
	err := c.client.Get(ctx, name, object)

	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to fetch Stream Cron Job")
		return nil, err
	}

	if errors.IsNotFound(err) {
		logger.V(0).Info("streaming does not exist")
		return nil, nil
	}

	return fromObject(object)
}

func (j *ResourceReader) CompareConfigurations(ctx context.Context, object client.Object, definition stream.Definition, fromObject ResourceConverter) (bool, error) {
	logger := j.getLogger(ctx, definition.NamespacedName())
	resource, err := j.Get(ctx, definition.NamespacedName(), object, fromObject)
	if err != nil {
		logger.V(0).Error(err, "Failed to get resource for stream")
		return false, err
	}

	configuration, err := resource.CurrentConfiguration()
	if err != nil {
		logger.V(0).Error(err, "Failed to extract configuration from job")
		return false, err
	}

	// This is a new stream, so we start backfill even if the backfill request is not present.
	definitionConfiguration, err := definition.CurrentConfiguration(nil)
	if err != nil {
		logger.V(0).Error(err, "Failed to extract configuration from stream definition")
		return false, err
	}
	return configuration == definitionConfiguration, nil
}

func (c *ResourceReader) getLogger(ctx context.Context, request types.NamespacedName) klog.Logger {
	return klog.FromContext(ctx).
		WithName("ResourceReader").
		WithValues("namespace", request.Namespace, "streamId", request.Name)
}
