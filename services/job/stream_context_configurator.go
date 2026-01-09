package job

import (
	"encoding/json"
	"fmt"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"strings"
)

var _ Configurator = &StreamContextConfigurator{}

type StreamContextConfigurator struct {
	environmentKey string
	definition     string
	next           Configurator
}

func (f StreamContextConfigurator) AddNext(configurator Configurator) Configurator {
	f.next = configurator
	return f
}

func (f StreamContextConfigurator) ConfigureJob(job *batchv1.Job) error {
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

func NewEnvironmentConfigurator(baseObject any, environmentKey string) *StreamContextConfigurator {
	b, err := json.Marshal(baseObject)
	def := ""
	if err == nil {
		def = string(b)
	}
	return &StreamContextConfigurator{
		environmentKey: environmentKey,
		definition:     def,
	}
}
