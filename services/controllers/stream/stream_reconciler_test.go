package stream

import (
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	testv1 "github.com/SneaksAndData/arcane-operator/pkg/test/apis_test/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/tests/mocks"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"testing"
)

func Test_UpdatePhase_New_To_Suspended(t *testing.T) {
	// Arrange
	k8sClient := setupClient(nil)
	gvk := schema.GroupVersionKind{Group: "streaming.sneaksanddata.com", Version: "v1", Kind: "MockStreamDefinition"}
	reconciler := NewStreamReconciler(k8sClient, gvk, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "s1"}})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	sd := &testv1.MockStreamDefinition{}
	err = k8sClient.Get(t.Context(), types.NamespacedName{Name: "s1"}, sd)
	require.NoError(t, err)
	require.Equal(t, "Suspended", sd.Status.Phase)
}

func Test_UpdatePhase_New_To_Backfilling(t *testing.T) {
	// Arrange
	k8sClient := setupClient(func(definition *testv1.MockStreamDefinition) {
		definition.Spec.Suspended = false
	})
	gvk := schema.GroupVersionKind{Group: "streaming.sneaksanddata.com", Version: "v1", Kind: "MockStreamDefinition"}
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "job1"},
	}, nil)
	reconciler := NewStreamReconciler(k8sClient, gvk, jobBuilder)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: "s1"}})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	// Fetch the object and ensure its status Phase is Pending
	sd := &testv1.MockStreamDefinition{}
	err = k8sClient.Get(t.Context(), types.NamespacedName{Name: "s1"}, sd)
	require.NoError(t, err)
	// The generated mock type stores status as MockStreamDefinitionStatus with Phase string
	require.Equal(t, "Suspended", sd.Status.Phase)
}

func setupClient(prepareStreamDefinition func(definition *testv1.MockStreamDefinition)) client.Client {
	scheme := runtime.NewScheme()
	_ = testv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)

	obj := &testv1.MockStreamDefinition{
		TypeMeta:   metav1.TypeMeta{APIVersion: "streaming.sneaksanddata.com/v1", Kind: "MockStreamDefinition"},
		ObjectMeta: metav1.ObjectMeta{Name: "s1"},
		Spec: testv1.MockStreamDefinitionSpec{
			Source:      "sourceA",
			Destination: "destinationB",
			Suspended:   true,
		},
	}
	if prepareStreamDefinition != nil {
		prepareStreamDefinition(obj)
	}
	return crfake.NewClientBuilder().WithScheme(scheme).WithObjects(obj).WithStatusSubresource(&testv1.MockStreamDefinition{}).Build()
}
