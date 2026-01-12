package job

import (
	"encoding/json"
	"fmt"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"strings"
)

var _ Configurator = &streamContextConfigurator{}

// streamContextConfigurator sets a STREAMCONTEXT__<KEY> environment variable in the job's containers.
type streamContextConfigurator struct {
	environmentKey string
	definition     string
}

func (f streamContextConfigurator) ConfigureJob(job *batchv1.Job) error {
	for k := range job.Spec.Template.Spec.Containers {
		envVar := corev1.EnvVar{
			Name:  fmt.Sprintf("STREAMCONTEXT__%s", strings.ToUpper(f.environmentKey)),
			Value: f.definition,
		}
		job.Spec.Template.Spec.Containers[k].Env = append(job.Spec.Template.Spec.Containers[k].Env, envVar)
	}

	return nil
}

func NewEnvironmentConfigurator(baseObject any, environmentKey string) Configurator {
	b, err := json.Marshal(baseObject)
	def := ""
	if err == nil {
		def = string(b)
	}
	return &streamContextConfigurator{
		environmentKey: environmentKey,
		definition:     def,
	}
}
