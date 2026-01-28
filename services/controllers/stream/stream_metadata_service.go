package stream

import (
	"fmt"
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

	for _, secretName := range s.streamClass.Spec.SecretRefs {
		name, err := s.streamDefinition.GetReferenceForSecret(secretName)
		if err != nil {
			return nil, fmt.Errorf("error getting secret reference: %w", err)
		}
		builder = builder.WithConfigurator(job.NewSecretReferenceConfigurator(name))
	}

	return builder.Build(), nil
}

func NewStreamMetadataService(streamClass *v1.StreamClass, streamDefinition Definition) job.ConfiguratorProvider {
	return &streamClassMetadataService{
		streamClass:      streamClass,
		streamDefinition: streamDefinition,
	}
}
