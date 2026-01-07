package services

import (
	"encoding/json"
	"fmt"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"strings"
)

var _ JobConfigurator = &EnvironmentBuilder{}

type EnvironmentBuilder struct {
	environmentKey string
	definition     string
	next           JobConfigurator
}

func (f EnvironmentBuilder) AddNext(configurator JobConfigurator) JobConfigurator {
	f.next = configurator
	return f
}

func (f EnvironmentBuilder) ConfigureJob(job *batchv1.Job) error {
	for k := range job.Spec.Template.Spec.Containers {
		envVar := corev1.EnvVar{
			Name:  fmt.Sprintf("STREAMCONTEXT__%s", strings.ToUpper(f.environmentKey)),
			Value: f.definition,
		}
		job.Spec.Template.Spec.Containers[k].Env = append(job.Spec.Template.Spec.Containers[k].Env, envVar)
	}

	if f.next != nil {
		return f.next.ConfigureJob(job)
	}
	return nil
}

func NewFromStreamDefinition(baseObject any, environmentKey string) *EnvironmentBuilder {
	b, err := json.Marshal(baseObject)
	def := ""
	if err == nil {
		def = string(b)
	}
	return &EnvironmentBuilder{
		environmentKey: environmentKey,
		definition:     def,
	}
}
