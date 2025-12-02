package configurations

// StreamClassOperatorConfiguration holds configuration for StreamClassOperatorService.
type StreamClassOperatorConfiguration struct {

	// MaxBufferCapacity is the max buffer capacity for StreamClasses events stream.
	MaxBufferCapacity int

	// ApiGroup is the API group of the StreamClass CRD.
	ApiGroup string

	// Version is the version of the StreamClass CRD.
	Version string

	// Plural is the plural name of the StreamClass CRD.
	Plural string

	// NameSpace is the namespace where the StreamClass CRDs are located.
	NameSpace string
}
