package job

import (
	"fmt"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

var _ Configurator = &metadataConfigurator{}

// metadataConfigurator adds stream metadata as environment variables to the job's containers.
// It adds STREAMCONTEXT__STREAM_ID and STREAMCONTEXT__STREAM_KIND environment variables.
// It also adds corresponding labels to the job metadata.
type metadataConfigurator struct {
	streamId   string
	streamKind string
}

func (f metadataConfigurator) ConfigureJob(job *batchv1.Job) error {
	if f.streamId == "" {
		return fmt.Errorf("streamId cannot be empty")
	}
	if f.streamKind == "" {
		return fmt.Errorf("streamKind cannot be empty")
	}

	err := f.addEnvironmentVariable(job, "STREAMCONTEXT__STREAM_ID", f.streamId)
	if err != nil {
		return err
	}

	err = f.addEnvironmentVariable(job, "STREAMCONTEXT__STREAM_KIND", f.streamKind)
	if err != nil {
		return err
	}

	if job.Labels == nil {
		job.Labels = make(map[string]string)
	}
	job.Labels["arcane/stream-id"] = f.streamId
	job.Labels["arcane/stream-kind"] = f.streamKind

	return nil
}

func (f metadataConfigurator) addEnvironmentVariable(job *batchv1.Job, name string, value string) error {
	envVar := corev1.EnvVar{Name: name, Value: value}
	for k := range job.Spec.Template.Spec.Containers {
		if job.Spec.Template.Spec.Containers[k].Env == nil || len(job.Spec.Template.Spec.Containers[k].Env) == 0 {
			job.Spec.Template.Spec.Containers[k].Env = []corev1.EnvVar{
				envVar,
			}
			continue
		}
		for v := range job.Spec.Template.Spec.Containers[k].Env {
			if job.Spec.Template.Spec.Containers[k].Env[v].Name == name {
				return fmt.Errorf("environment variable %s already present", name)
			}
			job.Spec.Template.Spec.Containers[k].Env = append(job.Spec.Template.Spec.Containers[k].Env, envVar)
		}
	}
	return nil
}

func NewMetadataConfigurator(streamId string, streamKind string) Configurator {
	return &metadataConfigurator{
		streamId:   streamId,
		streamKind: streamKind,
	}
}
