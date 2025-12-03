package configurations

// StreamingJobTemplateConfiguration holds configuration for StreamingJobTemplate CRD.
type StreamingJobTemplateConfiguration struct {

	// MaxBufferCapacity is the max buffer capacity for StreamClasses events stream.
	MaxBufferCapacity int

	// ApiSettings holds the API settings for the StreamingJobTemplate CRD.
	ApiSettings ApiSettings
}
