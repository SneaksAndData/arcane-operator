package tests

import (
	"testing"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/tests/mocks/job_mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Test_StreamMetadataService_JobConfigurator_NoSecretRefs(t *testing.T) {
	// Arrange
	streamClass := &v1.StreamClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-stream-class",
		},
		Spec: v1.StreamClassSpec{
			APIGroupRef: "streaming.sneaksanddata.com",
			APIVersion:  "v1",
			KindRef:     "MockStreamDefinition",
			PluralName:  "mockstreamdefinitions",
			SecretRefs:  []string{}, // No secret refs
		},
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockProvider := job_mock.NewMockSecretReferenceProvider(mockCtrl)
	service := stream.NewStreamMetadataService(streamClass, mockProvider)

	// Act
	configurator, err := service.JobConfigurator()

	// Assert
	require.NoError(t, err)
	require.NotNil(t, configurator)

	// Apply configurator to a job
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

	err = configurator.ConfigureJob(job)
	require.NoError(t, err)

	// Should not have any EnvFrom since no secret refs
	require.Nil(t, job.Spec.Template.Spec.Containers[0].EnvFrom)
}

func Test_StreamMetadataService_JobConfigurator_SingleSecretRef(t *testing.T) {
	// Arrange
	streamClass := &v1.StreamClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-stream-class",
		},
		Spec: v1.StreamClassSpec{
			APIGroupRef: "streaming.sneaksanddata.com",
			APIVersion:  "v1",
			KindRef:     "MockStreamDefinition",
			PluralName:  "mockstreamdefinitions",
			SecretRefs:  []string{"secretRef"},
		},
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockProvider := job_mock.NewMockSecretReferenceProvider(mockCtrl)
	mockProvider.EXPECT().
		GetReferenceForSecret("secretRef").
		Return(&corev1.LocalObjectReference{Name: "databaseCredentials"}, nil).
		Times(1)
	service := stream.NewStreamMetadataService(streamClass, mockProvider)

	// Act
	configurator, err := service.JobConfigurator()

	// Assert
	require.NoError(t, err)
	require.NotNil(t, configurator)

	// Apply configurator to a job
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

	err = configurator.ConfigureJob(job)
	require.NoError(t, err)

	// Should have one EnvFrom with the secret reference
	require.NotNil(t, job.Spec.Template.Spec.Containers[0].EnvFrom)
	require.Len(t, job.Spec.Template.Spec.Containers[0].EnvFrom, 1)
	require.Equal(t, "databaseCredentials", job.Spec.Template.Spec.Containers[0].EnvFrom[0].SecretRef.Name)
}

func Test_StreamMetadataService_JobConfigurator_NilSecretRefs(t *testing.T) {
	// Arrange
	streamClass := &v1.StreamClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-stream-class",
		},
		Spec: v1.StreamClassSpec{
			APIGroupRef: "streaming.sneaksanddata.com",
			APIVersion:  "v1",
			KindRef:     "MockStreamDefinition",
			PluralName:  "mockstreamdefinitions",
			SecretRefs:  nil, // Nil secret refs
		},
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockProvider := job_mock.NewMockSecretReferenceProvider(mockCtrl)
	service := stream.NewStreamMetadataService(streamClass, mockProvider)

	// Act
	configurator, err := service.JobConfigurator()

	// Assert
	require.NoError(t, err)
	require.NotNil(t, configurator)

	// Apply configurator to a job
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

	err = configurator.ConfigureJob(job)
	require.NoError(t, err)

	// Should not have any EnvFrom since no secret refs
	require.Nil(t, job.Spec.Template.Spec.Containers[0].EnvFrom)
}

func Test_StreamMetadataService_JobConfigurator_MultipleContainers(t *testing.T) {
	// Arrange
	streamClass := &v1.StreamClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-stream-class",
		},
		Spec: v1.StreamClassSpec{
			APIGroupRef: "streaming.sneaksanddata.com",
			APIVersion:  "v1",
			KindRef:     "MockStreamDefinition",
			PluralName:  "mockstreamdefinitions",
			SecretRefs:  []string{"secretRef"},
		},
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockProvider := job_mock.NewMockSecretReferenceProvider(mockCtrl)
	mockProvider.EXPECT().
		GetReferenceForSecret("secretRef").
		Return(&corev1.LocalObjectReference{Name: "databaseCredentials"}, nil).
		Times(1)
	service := stream.NewStreamMetadataService(streamClass, mockProvider)

	// Act
	configurator, err := service.JobConfigurator()

	// Assert
	require.NoError(t, err)
	require.NotNil(t, configurator)

	// Apply configurator to a job with multiple containers
	job := &batchv1.Job{
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "main-container",
							Image: "main-image:latest",
						},
						{
							Name:  "sidecar-container",
							Image: "sidecar-image:latest",
						},
					},
				},
			},
		},
	}

	err = configurator.ConfigureJob(job)
	require.NoError(t, err)

	// Should have secret reference in all containers
	require.NotNil(t, job.Spec.Template.Spec.Containers[0].EnvFrom)
	require.Len(t, job.Spec.Template.Spec.Containers[0].EnvFrom, 1)
	require.Equal(t, "databaseCredentials", job.Spec.Template.Spec.Containers[0].EnvFrom[0].SecretRef.Name)

	require.NotNil(t, job.Spec.Template.Spec.Containers[1].EnvFrom)
	require.Len(t, job.Spec.Template.Spec.Containers[1].EnvFrom, 1)
	require.Equal(t, "databaseCredentials", job.Spec.Template.Spec.Containers[1].EnvFrom[0].SecretRef.Name)
}

func Test_StreamMetadataService_JobConfigurator_PreservesExistingEnvFrom(t *testing.T) {
	// Arrange
	streamClass := &v1.StreamClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-stream-class",
		},
		Spec: v1.StreamClassSpec{
			APIGroupRef: "streaming.sneaksanddata.com",
			APIVersion:  "v1",
			KindRef:     "MockStreamDefinition",
			PluralName:  "mockstreamdefinitions",
			SecretRefs:  []string{"secretRef"},
		},
	}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockProvider := job_mock.NewMockSecretReferenceProvider(mockCtrl)
	mockProvider.EXPECT().
		GetReferenceForSecret("secretRef").
		Return(&corev1.LocalObjectReference{Name: "my-secret"}, nil).
		Times(1)
	service := stream.NewStreamMetadataService(streamClass, mockProvider)

	// Act
	configurator, err := service.JobConfigurator()

	// Assert
	require.NoError(t, err)
	require.NotNil(t, configurator)

	// Apply configurator to a job with existing EnvFrom
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

	err = configurator.ConfigureJob(job)
	require.NoError(t, err)

	// Should preserve existing configmap and add secret reference
	require.NotNil(t, job.Spec.Template.Spec.Containers[0].EnvFrom)
	require.Len(t, job.Spec.Template.Spec.Containers[0].EnvFrom, 2)
	require.NotNil(t, job.Spec.Template.Spec.Containers[0].EnvFrom[0].ConfigMapRef)
	require.Equal(t, "existing-configmap", job.Spec.Template.Spec.Containers[0].EnvFrom[0].ConfigMapRef.Name)
	require.NotNil(t, job.Spec.Template.Spec.Containers[0].EnvFrom[1].SecretRef)
	require.Equal(t, "my-secret", job.Spec.Template.Spec.Containers[0].EnvFrom[1].SecretRef.Name)
}
