package backend

import (
	"context"

	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ResourceConverter func(object client.Object) (stream.BackendResource, error)

type ResourceReader struct {
	Client client.Client
}

func (c *ResourceReader) Get(ctx context.Context, name client.ObjectKey, object client.Object, fromObject ResourceConverter) (stream.BackendResource, error) {
	logger := klog.FromContext(ctx)
	err := c.Client.Get(ctx, name, object)

	if client.IgnoreNotFound(err) != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to fetch streaming backend resource")
		return nil, err
	}

	if errors.IsNotFound(err) {
		logger.V(0).Info("streaming backend resource does not exist")
		return nil, nil
	}

	return fromObject(object)
}

func (j *ResourceReader) CompareConfigurations(ctx context.Context, object client.Object, definition stream.Definition, fromObject ResourceConverter) (bool, error) {
	logger := klog.FromContext(ctx)

	resource, err := j.Get(ctx, definition.NamespacedName(), object, fromObject)
	if err != nil { // coverage-ignore
		return false, err
	}

	configuration, err := resource.CurrentConfiguration()
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "failed to extract configuration from streaming backend resource")
		return false, err
	}

	definitionConfiguration, err := definition.CurrentConfiguration(nil)
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "failed to extract configuration from stream definition")
		return false, err
	}
	return configuration == definitionConfiguration, nil
}
