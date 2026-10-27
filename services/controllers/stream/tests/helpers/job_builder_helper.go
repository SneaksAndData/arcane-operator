package helpers

import (
	"context"

	"github.com/SneaksAndData/arcane-operator/services/job"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ConfigureAndReturnJob returns a function suitable for use with gomock's
// DoAndReturn on JobBuilder.BuildJob. It builds a bare Job with a single
// container and applies the given configurator to it, allowing assertions
// against the resulting environment variables (e.g. STREAMCONTEXT__RUNMODE)
// without depending on a real StreamingJobTemplate resource.
func ConfigureAndReturnJob(name types.NamespacedName) func(ctx context.Context, templateName types.NamespacedName, configurator job.Configurator) (*batchv1.Job, error) {
	return func(_ context.Context, _ types.NamespacedName, configurator job.Configurator) (*batchv1.Job, error) {
		newJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: name.Name, Namespace: name.Namespace},
			Spec: batchv1.JobSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "test-container"}},
					},
				},
			},
		}

		if err := configurator.ConfigureJob(newJob); err != nil {
			return nil, err
		}

		return newJob, nil
	}
}
