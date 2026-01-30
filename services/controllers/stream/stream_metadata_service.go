package stream

import (
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/job"
)

var _ job.ConfiguratorProvider = &streamClassMetadataService{}

type streamClassMetadataService struct {
	streamClass      *v1.StreamClass
	streamDefinition Definition
}

func (s streamClassMetadataService) JobConfigurator() (job.Configurator, error) {
	builder := job.NewConfiguratorChainBuilder()

	for _, referenceFieldName := range s.streamClass.Spec.SecretRefs {
		builder = builder.WithConfigurator(job.NewSecretReferenceConfigurator(referenceFieldName, s.streamDefinition))
	}

	return builder.Build(), nil
}

func NewStreamMetadataService(streamClass *v1.StreamClass, streamDefinition Definition) job.ConfiguratorProvider {
	return &streamClassMetadataService{
		streamClass:      streamClass,
		streamDefinition: streamDefinition,
	}
}
