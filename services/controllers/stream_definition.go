package controllers

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

type Phase string

const (
	New         Phase = ""
	Pending     Phase = "Pending"
	Running     Phase = "Running"
	Backfilling Phase = "Backfilling"
	Suspended   Phase = "Completed"
	Failed      Phase = "Failed"
)

type StreamDefinition interface {
	GetPhase() Phase
	Suspended() bool
	GetJobName() string
	GetJobNamespace() string
	CurrentConfiguration() string
	LastObservedConfiguration() string
	NamespacedName() types.NamespacedName
	ToUnstructured() *unstructured.Unstructured
	SetStatus(status Phase)
	StateString() string
}

func FromUnstructured(obj *unstructured.Unstructured) StreamDefinition {
	panic("implement me")
}
