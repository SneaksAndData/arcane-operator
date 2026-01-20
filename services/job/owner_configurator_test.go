package job

import (
	"github.com/stretchr/testify/require"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Test_OwnerConfigurator_Set_OwnerReference(t *testing.T) {
	job := &batchv1.Job{}

	ownerRef := metav1.OwnerReference{
		APIVersion: "v1",
		Kind:       "StreamDefinition",
		Name:       "test-stream",
		UID:        "12345-67890",
	}

	configurator := NewOwnerConfigurator(ownerRef)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.OwnerReferences, 1)
	require.Equal(t, "v1", job.OwnerReferences[0].APIVersion)
	require.Equal(t, "StreamDefinition", job.OwnerReferences[0].Kind)
	require.Equal(t, "test-stream", job.OwnerReferences[0].Name)
	require.Equal(t, "12345-67890", string(job.OwnerReferences[0].UID))
}

func Test_OwnerConfigurator_Nil_OwnerReferences(t *testing.T) {
	job := &batchv1.Job{}
	job.OwnerReferences = nil

	ownerRef := metav1.OwnerReference{
		APIVersion: "v1",
		Kind:       "Pod",
		Name:       "test-pod",
		UID:        "abc-123",
	}

	configurator := NewOwnerConfigurator(ownerRef)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.NotNil(t, job.OwnerReferences)
	require.Len(t, job.OwnerReferences, 1)
	require.Equal(t, "test-pod", job.OwnerReferences[0].Name)
}

func Test_OwnerConfigurator_Append_To_Existing_OwnerReferences(t *testing.T) {
	job := &batchv1.Job{}
	job.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion: "v1",
			Kind:       "Deployment",
			Name:       "existing-deployment",
			UID:        "existing-uid",
		},
	}

	ownerRef := metav1.OwnerReference{
		APIVersion: "v1beta1",
		Kind:       "StreamDefinition",
		Name:       "new-stream",
		UID:        "new-uid",
	}

	configurator := NewOwnerConfigurator(ownerRef)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.OwnerReferences, 2)
	require.Equal(t, "existing-deployment", job.OwnerReferences[0].Name)
	require.Equal(t, "new-stream", job.OwnerReferences[1].Name)
}

func Test_OwnerConfigurator_Multiple_Calls(t *testing.T) {
	job := &batchv1.Job{}

	ownerRef1 := metav1.OwnerReference{
		APIVersion: "v1",
		Kind:       "Stream1",
		Name:       "stream-1",
		UID:        "uid-1",
	}

	ownerRef2 := metav1.OwnerReference{
		APIVersion: "v1",
		Kind:       "Stream2",
		Name:       "stream-2",
		UID:        "uid-2",
	}

	configurator1 := NewOwnerConfigurator(ownerRef1)
	err := configurator1.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.OwnerReferences, 1)

	configurator2 := NewOwnerConfigurator(ownerRef2)
	err = configurator2.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.OwnerReferences, 2)
	require.Equal(t, "stream-1", job.OwnerReferences[0].Name)
	require.Equal(t, "stream-2", job.OwnerReferences[1].Name)
}

func Test_OwnerConfigurator_With_Controller_True(t *testing.T) {
	job := &batchv1.Job{}

	controller := true
	ownerRef := metav1.OwnerReference{
		APIVersion: "v1",
		Kind:       "StreamDefinition",
		Name:       "controller-stream",
		UID:        "controller-uid",
		Controller: &controller,
	}

	configurator := NewOwnerConfigurator(ownerRef)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.OwnerReferences, 1)
	require.NotNil(t, job.OwnerReferences[0].Controller)
	require.True(t, *job.OwnerReferences[0].Controller)
}

func Test_OwnerConfigurator_With_BlockOwnerDeletion_True(t *testing.T) {
	job := &batchv1.Job{}

	blockOwnerDeletion := true
	ownerRef := metav1.OwnerReference{
		APIVersion:         "v1",
		Kind:               "StreamDefinition",
		Name:               "block-stream",
		UID:                "block-uid",
		BlockOwnerDeletion: &blockOwnerDeletion,
	}

	configurator := NewOwnerConfigurator(ownerRef)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.OwnerReferences, 1)
	require.NotNil(t, job.OwnerReferences[0].BlockOwnerDeletion)
	require.True(t, *job.OwnerReferences[0].BlockOwnerDeletion)
}

func Test_OwnerConfigurator_With_Both_Controller_And_BlockOwnerDeletion(t *testing.T) {
	job := &batchv1.Job{}

	controller := true
	blockOwnerDeletion := true
	ownerRef := metav1.OwnerReference{
		APIVersion:         "streaming.sneaksanddata.com/v1",
		Kind:               "StreamDefinition",
		Name:               "full-stream",
		UID:                "full-uid",
		Controller:         &controller,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}

	configurator := NewOwnerConfigurator(ownerRef)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.OwnerReferences, 1)
	require.NotNil(t, job.OwnerReferences[0].Controller)
	require.True(t, *job.OwnerReferences[0].Controller)
	require.NotNil(t, job.OwnerReferences[0].BlockOwnerDeletion)
	require.True(t, *job.OwnerReferences[0].BlockOwnerDeletion)
	require.Equal(t, "streaming.sneaksanddata.com/v1", job.OwnerReferences[0].APIVersion)
}

func Test_OwnerConfigurator_Does_Not_Affect_Other_Properties(t *testing.T) {
	job := &batchv1.Job{}
	job.Name = "test-job"
	job.Namespace = "test-namespace"
	job.Labels = map[string]string{
		"app": "test-app",
	}

	ownerRef := metav1.OwnerReference{
		APIVersion: "v1",
		Kind:       "Stream",
		Name:       "test-stream",
		UID:        "test-uid",
	}

	configurator := NewOwnerConfigurator(ownerRef)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Equal(t, "test-job", job.Name)
	require.Equal(t, "test-namespace", job.Namespace)
	require.Equal(t, "test-app", job.Labels["app"])
}

func Test_OwnerConfigurator_Empty_OwnerReference(t *testing.T) {
	job := &batchv1.Job{}

	ownerRef := metav1.OwnerReference{}

	configurator := NewOwnerConfigurator(ownerRef)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.OwnerReferences, 1)
	require.Equal(t, "", job.OwnerReferences[0].APIVersion)
	require.Equal(t, "", job.OwnerReferences[0].Kind)
	require.Equal(t, "", job.OwnerReferences[0].Name)
}

func Test_OwnerConfigurator_Realistic_StreamDefinition_Owner(t *testing.T) {
	job := &batchv1.Job{}

	controller := true
	blockOwnerDeletion := true
	ownerRef := metav1.OwnerReference{
		APIVersion:         "streaming.sneaksanddata.com/v1beta1",
		Kind:               "StreamDefinition",
		Name:               "production-stream-abc",
		UID:                "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
		Controller:         &controller,
		BlockOwnerDeletion: &blockOwnerDeletion,
	}

	configurator := NewOwnerConfigurator(ownerRef)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Len(t, job.OwnerReferences, 1)
	require.Equal(t, "streaming.sneaksanddata.com/v1beta1", job.OwnerReferences[0].APIVersion)
	require.Equal(t, "StreamDefinition", job.OwnerReferences[0].Kind)
	require.Equal(t, "production-stream-abc", job.OwnerReferences[0].Name)
	require.Equal(t, "a1b2c3d4-e5f6-7890-abcd-ef1234567890", string(job.OwnerReferences[0].UID))
}
