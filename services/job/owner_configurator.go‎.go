package job

import (
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ Configurator = &ownerConfigurator{}

type ownerConfigurator struct {
	metav1.OwnerReference
}

func (o ownerConfigurator) ConfigureJob(job *batchv1.Job) error {
	if job.OwnerReferences == nil {
		job.OwnerReferences = []metav1.OwnerReference{}
	}

	job.OwnerReferences = append(job.OwnerReferences, o.OwnerReference)
	return nil
}

func NewOwnerConfigurator(ownerRef metav1.OwnerReference) Configurator {
	return &ownerConfigurator{
		OwnerReference: ownerRef,
	}
}
