package controllers

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

type Status string

const (
	New         Status = ""
	Pending     Status = "Pending"
	Running     Status = "Running"
	Backfilling Status = "Backfilling"
	Suspended   Status = "Completed"
	Failed      Status = "Failed"
)

type StreamDefinition interface {
	GetStatus() Status
	Suspended() bool
	GetJobName() string
	GetJobNamespace() string
	CurrentConfiguration() string
	LastObservedConfiguration() string
	NamespacedName() types.NamespacedName
	ToUnstructured() *unstructured.Unstructured
	SetStatus(status Status)
}

func FromUnstructured(obj *unstructured.Unstructured) StreamDefinition {
	panic("implement me")
}
