package configurations

type ApiSettings struct {
	// ApiGroup is the API group of the StreamClass CRD.
	ApiGroup string

	// Version is the version of the StreamClass CRD.
	Version string

	// Plural is the plural name of the StreamClass CRD.
	Plural string
}
