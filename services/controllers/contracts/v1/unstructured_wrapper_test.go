package v1

import (
	"testing"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	testv2 "github.com/SneaksAndData/arcane-operator/pkg/test/apis_test/streaming/v2"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func Test_GetPhase(t *testing.T) {
	// Arrange
	fakeClient := setupFakeClient(func(sd *testv2.MockStreamDefinition) {
		sd.Status.Phase = "Running"
	})
	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	// Act
	wrapper := NewExecutionSettings(&unstructuredObj)
	err = wrapper.Validate()

	// Assert
	require.NoError(t, err)
	require.Equal(t, stream.Phase("Running"), wrapper.GetPhase())
}

func Test_Suspended(t *testing.T) {
	// Arrange
	fakeClient := setupFakeClient(func(sd *testv2.MockStreamDefinition) {
		sd.Spec.ExecutionSettings.Suspended = false
	})

	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	// Act
	wrapper := NewExecutionSettings(&unstructuredObj)
	err = wrapper.Validate()
	require.NoError(t, err)

	err = wrapper.SetSuspended(true)
	require.NoError(t, err)

	us := wrapper.ToUnstructured()
	err = fakeClient.Update(t.Context(), us)
	require.NoError(t, err)
	unstructuredObj, err = getUnstructured(t, fakeClient)
	require.NoError(t, err)
	wrapper = NewExecutionSettings(&unstructuredObj)
	err = wrapper.Validate()

	// Assert
	require.NoError(t, err)
	require.Equal(t, true, wrapper.Suspended())
}

func Test_CurrentConfiguration(t *testing.T) {
	// Arrange
	var streamDefinition *testv2.MockStreamDefinition
	fakeClient := setupFakeClient(func(sd *testv2.MockStreamDefinition) {
		sd.Spec.ExecutionSettings.Suspended = true
		streamDefinition = sd
	})

	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	// Act
	wrapper := NewExecutionSettings(&unstructuredObj)
	err = wrapper.Validate()
	require.NoError(t, err)
	require.NotNil(t, wrapper)

	currentConfig, err := wrapper.CurrentConfiguration(nil)
	require.NoError(t, err)

	streamDefinition.Spec.Destination = "destinationC"
	err = fakeClient.Update(t.Context(), streamDefinition)
	require.NoError(t, err)

	unstructuredObj, err = getUnstructured(t, fakeClient)
	require.NoError(t, err)

	wrapper = NewExecutionSettings(&unstructuredObj)
	err = wrapper.Validate()
	require.NotNil(t, wrapper)
	require.NoError(t, err)

	updatedConfig, err := wrapper.CurrentConfiguration(nil)
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
	wrapper := NewExecutionSettings(&unstructuredObj)
	require.NotNil(t, wrapper)

	err = wrapper.Validate()
	require.NoError(t, err)

	currentConfig, err := wrapper.CurrentConfiguration(nil)
	require.NoError(t, err)

	// Precondition: LastAppliedConfiguration is empty before recompute since it hasn't been set yet
	lastAppliedConfig := wrapper.LastAppliedConfiguration()
	require.Empty(t, lastAppliedConfig)

	err = wrapper.RecomputeConfiguration(nil)
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
	wrapper := NewExecutionSettings(&unstructuredObj)
	require.NotNil(t, wrapper)

	err = wrapper.Validate()
	require.NoError(t, err)

	// Assert
	require.Equal(t, types.NamespacedName{Name: "sd1", Namespace: ""}, wrapper.NamespacedName())
}

func Test_SetPhase(t *testing.T) {
	// Arrange
	fakeClient := setupFakeClient(nil)

	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	wrapper := NewExecutionSettings(&unstructuredObj)
	require.NotNil(t, wrapper)

	err = wrapper.Validate()
	require.NoError(t, err)

	// Act

	err = wrapper.SetPhase(stream.Backfilling)
	require.NoError(t, err)
	us := wrapper.ToUnstructured()
	err = fakeClient.Status().Update(t.Context(), us)
	require.NoError(t, err)

	unstructuredObj, err = getUnstructured(t, fakeClient)
	require.NoError(t, err)

	wrapper = NewExecutionSettings(&unstructuredObj)
	require.NotNil(t, wrapper)

	err = wrapper.Validate()
	require.NoError(t, err)

	// Assert
	require.Equal(t, stream.Phase("Backfilling"), wrapper.GetPhase())
}

func Test_GetStreamingJobName(t *testing.T) {
	// Arrange
	fakeClient := setupFakeClient(func(sd *testv2.MockStreamDefinition) {
		sd.Spec.ExecutionSettings = testv2.ExecutionSettings{
			APIVersion: "v1",
			Suspended:  false,
			BackfillJobTemplateRef: corev1.ObjectReference{
				Name:      "backfillJobTemplate1",
				Namespace: "default",
			},
			StreamingBackend: testv2.StreamingBackend{
				Realtime: &testv2.RealtimeBackend{
					JobTemplateRef: corev1.ObjectReference{
						Name:      "jobTemplate1",
						Namespace: "default",
					},
				},
			},
		}
	})

	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	// Act
	wrapper := NewExecutionSettings(&unstructuredObj)
	require.NotNil(t, wrapper)

	err = wrapper.Validate()
	require.NoError(t, err)

	// Assert
	name := types.NamespacedName{Name: "jobTemplate1", Namespace: "default"}
	require.Equal(t, name, wrapper.GetJobTemplate(nil))
}

func Test_GetBackfillJobName(t *testing.T) {
	// Arrange
	fakeClient := setupFakeClient(func(sd *testv2.MockStreamDefinition) {
		sd.Spec.ExecutionSettings.BackfillJobTemplateRef = corev1.ObjectReference{
			Name:      "backfillJobTemplate1",
			Namespace: "default",
		}
	})

	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	// Act
	wrapper := NewExecutionSettings(&unstructuredObj)
	require.NotNil(t, wrapper)

	err = wrapper.Validate()
	require.NoError(t, err)

	// Assert
	name := types.NamespacedName{Name: "backfillJobTemplate1", Namespace: "default"}
	require.Equal(t, name, wrapper.GetJobTemplate(&v1.BackfillRequest{}))
}

func Test_ToOwnerReference(t *testing.T) {
	// Arrange
	fakeClient := setupFakeClient(nil)

	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	// Act
	wrapper := NewExecutionSettings(&unstructuredObj)
	require.NotNil(t, wrapper)

	err = wrapper.Validate()
	require.NoError(t, err)

	// Assert
	ownerReference := wrapper.ToOwnerReference()
	require.Equal(t, "MockStreamDefinition", ownerReference.Kind)
	require.Equal(t, "streaming.sneaksanddata.com/v2", ownerReference.APIVersion)
	require.Equal(t, "sd1", ownerReference.Name)
	require.True(t, *ownerReference.Controller)

}

func Test_StateString(t *testing.T) {
	// Arrange
	fakeClient := setupFakeClient(func(sd *testv2.MockStreamDefinition) {
		sd.Status.Phase = "Running"
	})

	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	// Act
	wrapper := NewExecutionSettings(&unstructuredObj)
	require.NotNil(t, wrapper)

	err = wrapper.Validate()
	require.NoError(t, err)

	// Assert
	stateString := wrapper.StateString()
	require.Contains(t, stateString, "phase=Running")
}

func Test_SetSuspended(t *testing.T) {
	// Arrange
	fakeClient := setupFakeClient(func(sd *testv2.MockStreamDefinition) {
		sd.Spec.ExecutionSettings.Suspended = false
	})

	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	// Act
	wrapper := NewExecutionSettings(&unstructuredObj)
	require.NotNil(t, wrapper)

	err = wrapper.Validate()
	require.NoError(t, err)

	err = wrapper.SetSuspended(true)
	require.NoError(t, err)

	// Assert
	suspended := wrapper.Suspended()
	require.True(t, suspended)
}

func TestUnstructuredWrapper_GetBackend_Default(t *testing.T) {
	fakeClient := setupFakeClient(nil)
	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	wrapper := NewExecutionSettings(&unstructuredObj)
	err = wrapper.Validate()
	require.NotNil(t, wrapper)
	require.NoError(t, err)

	// Act
	backend := wrapper.GetBackend()

	// Assert
	require.Equal(t, stream.BatchJob, backend)
}

func TestUnstructuredWrapper_GetBackend_BatchJob(t *testing.T) {
	fakeClient := setupFakeClient(func(sd *testv2.MockStreamDefinition) {
		sd.Spec.ExecutionSettings = testv2.ExecutionSettings{
			APIVersion: "v1",
			Suspended:  false,
			BackfillJobTemplateRef: corev1.ObjectReference{
				Name:      "backfillJobTemplate1",
				Namespace: "default",
			},
			StreamingBackend: testv2.StreamingBackend{
				Realtime: &testv2.RealtimeBackend{
					JobTemplateRef: corev1.ObjectReference{
						Name:      "jobTemplate1",
						Namespace: "default",
					},
				},
			},
		}
	})
	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	wrapper := NewExecutionSettings(&unstructuredObj)
	err = wrapper.Validate()
	require.NotNil(t, wrapper)
	require.NoError(t, err)

	// Act
	backend := wrapper.GetBackend()

	// Assert
	require.Equal(t, stream.BatchJob, backend)
}

func TestUnstructuredWrapper_GetBackend_CronJob(t *testing.T) {
	fakeClient := setupFakeClient(func(sd *testv2.MockStreamDefinition) {
		sd.Spec.ExecutionSettings = testv2.ExecutionSettings{
			APIVersion: "v1",
			Suspended:  false,
			BackfillJobTemplateRef: corev1.ObjectReference{
				Name:      "backfillJobTemplate1",
				Namespace: "default",
			},
			StreamingBackend: testv2.StreamingBackend{
				Batch: &testv2.BatchBackend{
					JobTemplateRef: corev1.ObjectReference{
						Name:      "jobTemplate1",
						Namespace: "default",
					},
				},
			},
		}
	})
	unstructuredObj, err := getUnstructured(t, fakeClient)
	require.NoError(t, err)

	wrapper := NewExecutionSettings(&unstructuredObj)
	err = wrapper.Validate()
	require.NotNil(t, wrapper)
	require.NoError(t, err)

	// Act
	backend := wrapper.GetBackend()

	// Assert
	require.Equal(t, stream.CronJob, backend)
}

func setupFakeClient(updateStreamDefinition func(sd *testv2.MockStreamDefinition)) client.WithWatch {
	sd := testv2.MockStreamDefinition{
		TypeMeta:   metav1.TypeMeta{APIVersion: "streaming.sneaksanddata.com/v1", Kind: "MockStreamDefinition"},
		ObjectMeta: metav1.ObjectMeta{Name: "sd1"},
		Spec: testv2.MockStreamDefinitionSpec{
			Source:      "sourceA",
			Destination: "destinationB",
		},
		Status: testv2.MockStreamDefinitionStatus{
			Phase: "Running",
		},
	}
	if updateStreamDefinition != nil {
		updateStreamDefinition(&sd)
	}
	scheme := runtime.NewScheme()
	_ = testv2.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	k8sClient := crfake.NewClientBuilder().WithStatusSubresource(&testv2.MockStreamDefinition{}).WithScheme(scheme).WithObjects(&sd).Build()
	return k8sClient
}

func getUnstructured(t *testing.T, fakeClient client.WithWatch) (unstructured.Unstructured, error) {
	var unstructuredObj unstructured.Unstructured
	unstructuredObj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "streaming.sneaksanddata.com",
		Version: "v2",
		Kind:    "MockStreamDefinition",
	})
	err := fakeClient.Get(t.Context(), types.NamespacedName{Name: "sd1"}, &unstructuredObj)
	require.NoError(t, err)
	return unstructuredObj, err
}
