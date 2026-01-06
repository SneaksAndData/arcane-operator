package stream

import (
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	testv1 "github.com/SneaksAndData/arcane-operator/pkg/test/apis_test/streaming/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

func Test_GetPhase(t *testing.T) {
	// Arrange
	fakeClient := setupFakeClient(nil)
	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	// Act
	wrapper, err := fromUnstructured(&unstructuredObj)

	// Assert
	require.NoError(t, err)
	require.Equal(t, Phase("Running"), wrapper.GetPhase())
}

func Test_Suspended(t *testing.T) {
	// Arrange
	fakeClient := setupFakeClient(func(sd *testv1.MockStreamDefinition) {
		sd.Spec.Suspended = true
	})

	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	// Act
	wrapper, err := fromUnstructured(&unstructuredObj)

	// Assert
	require.NoError(t, err)
	require.Equal(t, true, wrapper.Suspended())
}

func Test_CurrentConfiguration(t *testing.T) {
	// Arrange
	var streamDefinition *testv1.MockStreamDefinition
	fakeClient := setupFakeClient(func(sd *testv1.MockStreamDefinition) {
		sd.Spec.Suspended = true
		streamDefinition = sd
	})

	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	// Act
	wrapper, err := fromUnstructured(&unstructuredObj)
	require.NotNil(t, wrapper)
	require.NoError(t, err)
	currentConfig, err := wrapper.CurrentConfiguration()
	require.NoError(t, err)

	streamDefinition.Spec.Destination = "destinationC"
	err = fakeClient.Update(t.Context(), streamDefinition)
	require.NoError(t, err)

	unstructuredObj, err = getUnstructured(t, fakeClient)
	require.NoError(t, err)

	wrapper, err = fromUnstructured(&unstructuredObj)
	require.NoError(t, err)
	require.NotNil(t, wrapper)

	updatedConfig, err := wrapper.CurrentConfiguration()
	require.NoError(t, err)

	// Assert
	require.NoError(t, err)
	require.NotEqual(t, currentConfig, updatedConfig)
}

func Test_LastAppliedConfiguration(t *testing.T) {
	// Arrange
	fakeClient := setupFakeClient(nil)

	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	// Act
	wrapper, err := fromUnstructured(&unstructuredObj)
	require.NotNil(t, wrapper)
	require.NoError(t, err)

	currentConfig, err := wrapper.CurrentConfiguration()
	require.NoError(t, err)

	// Precondition: LastAppliedConfiguration is empty before recompute since it hasn't been set yet
	lastAppliedConfig := wrapper.LastAppliedConfiguration()
	require.Empty(t, lastAppliedConfig)

	err = wrapper.RecomputeConfiguration()
	require.NoError(t, err)

	err = fakeClient.Update(t.Context(), wrapper.ToUnstructured())
	require.NoError(t, err)

	unstructuredObj, err = getUnstructured(t, fakeClient)
	require.NoError(t, err)

	lastAppliedConfig = wrapper.LastAppliedConfiguration()

	// Assert
	require.NoError(t, err)
	require.Equal(t, currentConfig, lastAppliedConfig)
}

func Test_NamespacedName(t *testing.T) {
	// Arrange
	fakeClient := setupFakeClient(nil)

	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	// Act
	wrapper, err := fromUnstructured(&unstructuredObj)
	require.NoError(t, err)
	require.NotNil(t, wrapper)

	// Assert
	require.Equal(t, types.NamespacedName{Name: "sd1", Namespace: ""}, wrapper.NamespacedName())
}

func Test_SetPhase(t *testing.T) {
	// Arrange
	fakeClient := setupFakeClient(nil)

	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	wrapper, err := fromUnstructured(&unstructuredObj)
	require.NotNil(t, wrapper)
	require.NoError(t, err)

	// Act

	err = wrapper.SetPhase(Backfilling)
	require.NoError(t, err)
	us := wrapper.ToUnstructured()
	err = fakeClient.Status().Update(t.Context(), us)
	require.NoError(t, err)

	unstructuredObj, err = getUnstructured(t, fakeClient)
	require.NoError(t, err)

	wrapper, err = fromUnstructured(&unstructuredObj)
	require.NotNil(t, wrapper)
	require.NoError(t, err)

	// Assert
	require.Equal(t, Phase("Backfilling"), wrapper.GetPhase())
}

func Test_GetStreamingJobName(t *testing.T) {
	// Arrange
	fakeClient := setupFakeClient(nil)

	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	// Act
	wrapper, err := fromUnstructured(&unstructuredObj)
	require.NotNil(t, wrapper)
	require.NoError(t, err)

	// Assert
	require.Equal(t, types.NamespacedName{Name: "jobTemplate1", Namespace: "default"}, wrapper.GetStreamingJobName())
}

func Test_GetBackfillJobName(t *testing.T) {
	// Arrange
	fakeClient := setupFakeClient(nil)

	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	// Act
	wrapper, err := fromUnstructured(&unstructuredObj)
	require.NotNil(t, wrapper)
	require.NoError(t, err)

	// Assert
	require.Equal(t, types.NamespacedName{Name: "backfillJobTemplate1", Namespace: "default"}, wrapper.GetBackfillJobName())
}

func Test_ToOwnerReference(t *testing.T) {
	// Arrange
	fakeClient := setupFakeClient(nil)

	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	// Act
	wrapper, err := fromUnstructured(&unstructuredObj)
	require.NotNil(t, wrapper)
	require.NoError(t, err)

	// Assert
	ownerReference := wrapper.ToOwnerReference()
	require.Equal(t, "MockStreamDefinition", ownerReference.Kind)
	require.Equal(t, "streaming.sneaksanddata.com/v1", ownerReference.APIVersion)
	require.Equal(t, "sd1", ownerReference.Name)
	require.True(t, *ownerReference.Controller)

}

func setupFakeClient(updateStreamDefinition func(sd *testv1.MockStreamDefinition)) client.WithWatch {
	sd := testv1.MockStreamDefinition{
		TypeMeta:   metav1.TypeMeta{APIVersion: "streaming.sneaksanddata.com/v1", Kind: "MockStreamDefinition"},
		ObjectMeta: metav1.ObjectMeta{Name: "sd1"},
		Spec: testv1.MockStreamDefinitionSpec{
			Source:      "sourceA",
			Destination: "destinationB",
			Suspended:   false,
			JobTemplateRef: corev1.ObjectReference{
				Name:      "jobTemplate1",
				Namespace: "default",
			},
			BackfillJobTemplateRef: corev1.ObjectReference{
				Name:      "backfillJobTemplate1",
				Namespace: "default",
			},
		},
		Status: testv1.MockStreamDefinitionStatus{
			Phase: "Running",
		},
	}
	if updateStreamDefinition != nil {
		updateStreamDefinition(&sd)
	}
	scheme := runtime.NewScheme()
	_ = testv1.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	k8sClient := crfake.NewClientBuilder().WithStatusSubresource(&testv1.MockStreamDefinition{}).WithScheme(scheme).WithObjects(&sd).Build()
	return k8sClient
}

func getUnstructured(t *testing.T, fakeClient client.WithWatch) (unstructured.Unstructured, error) {
	var unstructuredObj unstructured.Unstructured
	unstructuredObj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "streaming.sneaksanddata.com",
		Version: "v1",
		Kind:    "MockStreamDefinition",
	})
	err := fakeClient.Get(t.Context(), types.NamespacedName{Name: "sd1"}, &unstructuredObj)
	require.NoError(t, err)
	return unstructuredObj, err
}
