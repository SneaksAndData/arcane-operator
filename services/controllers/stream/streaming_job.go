package stream

import (
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type BackendResource interface {
	Name() string
	CurrentConfiguration() (string, error)
	IsCompleted() bool
	IsFailed() bool
	ToObject() client.Object
	IsBackfill() bool
}
