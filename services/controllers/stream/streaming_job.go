package stream

import (
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type BackendResource interface {
	Name() string
	UID() types.UID
	CurrentConfiguration() (string, error)
	IsCompleted() bool
	IsFailed() bool
	ToObject() client.Object
	IsBackfill() bool
}
