package job

import (
	"testing"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

func Test_SecretReferenceConfigurator_Add_SecretRef_To_Empty_Job(t *testing.T) {
	job := &batchv1.Job{
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "test-container",
							Image: "test-image:latest",
						},
					},
				},
			},
		},
	}

	configurator := NewSecretReferenceConfigurator(&corev1.LocalObjectReference{Name: "my-secret"})
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.NotNil(t, job.Spec.Template.Spec.Containers[0].EnvFrom)
	require.Len(t, job.Spec.Template.Spec.Containers[0].EnvFrom, 1)
	require.NotNil(t, job.Spec.Template.Spec.Containers[0].EnvFrom[0].SecretRef)
	require.Equal(t, &corev1.LocalObjectReference{Name: "my-secret"}, job.Spec.Template.Spec.Containers[0].EnvFrom[0].SecretRef.Name)
}

func Test_SecretReferenceConfigurator_Append_To_Existing_EnvFrom(t *testing.T) {
	job := &batchv1.Job{
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "test-container",
							Image: "test-image:latest",
							EnvFrom: []corev1.EnvFromSource{
								{
									ConfigMapRef: &corev1.ConfigMapEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: "existing-configmap",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	configurator := NewSecretReferenceConfigurator(&corev1.LocalObjectReference{Name: "new-secret"})
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.Spec.Template.Spec.Containers[0].EnvFrom, 2)
	require.NotNil(t, job.Spec.Template.Spec.Containers[0].EnvFrom[0].ConfigMapRef)
	require.Equal(t, "existing-configmap", job.Spec.Template.Spec.Containers[0].EnvFrom[0].ConfigMapRef.Name)
	require.NotNil(t, job.Spec.Template.Spec.Containers[0].EnvFrom[1].SecretRef)
	require.Equal(t, "new-secret", job.Spec.Template.Spec.Containers[0].EnvFrom[1].SecretRef.Name)
}

func Test_SecretReferenceConfigurator_No_Containers(t *testing.T) {
	job := &batchv1.Job{
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{},
				},
			},
		},
	}

	configurator := NewSecretReferenceConfigurator(&corev1.LocalObjectReference{Name: "my-secret"})
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Empty(t, job.Spec.Template.Spec.Containers)
}

func Test_SecretReferenceConfigurator_Nil_Containers(t *testing.T) {
	job := &batchv1.Job{
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: nil,
				},
			},
		},
	}

	configurator := NewSecretReferenceConfigurator(&corev1.LocalObjectReference{Name: "my-secret"})
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Nil(t, job.Spec.Template.Spec.Containers)
}

func Test_SecretReferenceConfigurator_Multiple_Secrets(t *testing.T) {
	job := &batchv1.Job{
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "test-container",
							Image: "test-image:latest",
						},
					},
				},
			},
		},
	}

	configurator1 := NewSecretReferenceConfigurator(&corev1.LocalObjectReference{Name: "first-secret"})
	err := configurator1.ConfigureJob(job)
	require.NoError(t, err)

	configurator2 := NewSecretReferenceConfigurator(&corev1.LocalObjectReference{Name: "second-secret"})
	err = configurator2.ConfigureJob(job)
	require.NoError(t, err)

	require.Len(t, job.Spec.Template.Spec.Containers[0].EnvFrom, 2)
	require.Equal(t, "first-secret", job.Spec.Template.Spec.Containers[0].EnvFrom[0].SecretRef.Name)
	require.Equal(t, "second-secret", job.Spec.Template.Spec.Containers[0].EnvFrom[1].SecretRef.Name)
}

func Test_SecretReferenceConfigurator_Empty_Secret_Name(t *testing.T) {
	job := &batchv1.Job{
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "test-container",
							Image: "test-image:latest",
						},
					},
				},
			},
		},
	}

	configurator := NewSecretReferenceConfigurator(&corev1.LocalObjectReference{Name: ""})
	err := configurator.ConfigureJob(job)
	require.EqualError(t, err, "secretReferenceConfigurator reference name is empty")
}

func Test_SecretReferenceConfigurator_Affects_All_Containers(t *testing.T) {
	job := &batchv1.Job{
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "first-container",
							Image: "first-image:latest",
						},
						{
							Name:  "second-container",
							Image: "second-image:latest",
						},
						{
							Name:  "third-container",
							Image: "third-image:latest",
						},
					},
				},
			},
		},
	}

	configurator := NewSecretReferenceConfigurator(&corev1.LocalObjectReference{Name: "my-secret"})
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	// Verify all containers have the secret reference
	require.Len(t, job.Spec.Template.Spec.Containers[0].EnvFrom, 1)
	require.Equal(t, &corev1.LocalObjectReference{Name: "my-secret"}, job.Spec.Template.Spec.Containers[0].EnvFrom[0].SecretRef.Name)

	require.Len(t, job.Spec.Template.Spec.Containers[1].EnvFrom, 1)
	require.Equal(t, &corev1.LocalObjectReference{Name: "my-secret"}, job.Spec.Template.Spec.Containers[1].EnvFrom[0].SecretRef.Name)

	require.Len(t, job.Spec.Template.Spec.Containers[2].EnvFrom, 1)
	require.Equal(t, &corev1.LocalObjectReference{Name: "my-secret"}, job.Spec.Template.Spec.Containers[2].EnvFrom[0].SecretRef.Name)
}

func Test_SecretReferenceConfigurator_With_Existing_SecretRef(t *testing.T) {
	job := &batchv1.Job{
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "test-container",
							Image: "test-image:latest",
							EnvFrom: []corev1.EnvFromSource{
								{
									SecretRef: &corev1.SecretEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: "existing-secret",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	configurator := NewSecretReferenceConfigurator(&corev1.LocalObjectReference{Name: "new-secret"})
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	require.Len(t, job.Spec.Template.Spec.Containers[0].EnvFrom, 2)
	require.Equal(t, "existing-secret", job.Spec.Template.Spec.Containers[0].EnvFrom[0].SecretRef.Name)
	require.Equal(t, "new-secret", job.Spec.Template.Spec.Containers[0].EnvFrom[1].SecretRef.Name)
}
