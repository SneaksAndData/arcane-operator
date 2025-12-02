package configurations

// StreamingJobTemplateConfiguration holds configuration for StreamingJobTemplate CRD.
type StreamingJobTemplateConfiguration struct {

	// ApiGroup is the API group of the StreamDefinition CRD.
	ApiGroup string

	// Version is the version of the CRD.
	Version string

	// Plural is the plural of the CRD.
	Plural string
}
