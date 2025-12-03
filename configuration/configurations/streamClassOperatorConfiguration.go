package configurations

// StreamClassOperatorConfiguration holds configuration for StreamClassOperatorService.
type StreamClassOperatorConfiguration struct {

	// MaxBufferCapacity is the max buffer capacity for StreamClasses events stream.
	MaxBufferCapacity int

	// NameSpace is the namespace where the StreamClass CRDs are located.
	NameSpace string

	// ApiSettings holds the API settings for the StreamClass CRD.
	ApiSettings ApiSettings
}
