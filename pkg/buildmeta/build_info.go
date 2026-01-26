package buildmeta

var (
	AppVersion  string
	BuildNumber string
)

// Use ldflags to set build metadata for apps using Nexus Core
// go build -ldflags "-X github.com/SneaksAndData/arcane-operator/pkg/buildmeta.AppVersion=0.0.0 -X github.com/SneaksAndData/arcane-operator/pkg/buildmeta.BuildNumber=dev1"
