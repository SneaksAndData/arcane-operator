package stream_class

import (
	"context"
	"fmt"
	"github.com/SneaksAndData/arcane-operator/tests/mocks"
	"github.com/google/uuid"
	"go.uber.org/mock/gomock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"testing"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func Test_UpdatePhase_ToPending(t *testing.T) {
	// Arrange
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	k8sClient, name := setupFakeClient(t, &v1.StreamClass{ObjectMeta: metav1.ObjectMeta{}})
	streamReconcilerFactory := mocks.NewMockUnmanagedControllerFactory(mockCtrl)
	reconciler := NewStreamClassReconciler(k8sClient, streamReconcilerFactory)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	expectPhase(t, k8sClient, name, v1.PhasePending)
}

func Test_UpdatePhase_ToRunning(t *testing.T) {
	// Arrange
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	k8sClient, name := setupFakeClient(t, &v1.StreamClass{
		ObjectMeta: metav1.ObjectMeta{},
		Status: v1.StreamClassStatus{
			Phase: v1.PhasePending,
		},
	})

	streamController := mocks.NewMockController[reconcile.Request](mockCtrl)
	streamController.EXPECT().Start(gomock.Any())

	streamReconcilerFactory := mocks.NewMockUnmanagedControllerFactory(mockCtrl)
	streamReconcilerFactory.EXPECT().CreateStreamController(gomock.Any(), gomock.Any(), gomock.Any()).Return(streamController, nil)

	reconciler := NewStreamClassReconciler(k8sClient, streamReconcilerFactory)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	expectPhase(t, k8sClient, name, v1.PhaseReady)
}

func Test_UpdatePhase_ToRunning_Idempotence(t *testing.T) {
	// Arrange
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	k8sClient, name := setupFakeClient(t, &v1.StreamClass{
		ObjectMeta: metav1.ObjectMeta{},
		Status: v1.StreamClassStatus{
			Phase: v1.PhasePending,
		},
	})

	streamController := mocks.NewMockController[reconcile.Request](mockCtrl)
	started := make(chan bool, 1)
	streamController.EXPECT().Start(gomock.Any()).Do(func(ctx context.Context, obj client.Object) {
		started <- true
	})

	streamReconcilerFactory := mocks.NewMockUnmanagedControllerFactory(mockCtrl)
	streamReconcilerFactory.EXPECT().CreateStreamController(gomock.Any(), gomock.Any(), gomock.Any()).Return(streamController, nil)

	reconciler := NewStreamClassReconciler(k8sClient, streamReconcilerFactory)

	// Act
	for i := 0; i < 5; i++ {
		result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
		require.NoError(t, err)
		require.Equal(t, result, reconcile.Result{})
	}

	// Wait for the stream controller to start at least once
	<-started

	// Assert
	expectPhase(t, k8sClient, name, v1.PhaseReady)
}

func Test_UpdatePhase_Ready_ToStopped(t *testing.T) {
	// Arrange
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	k8sClient, name := setupFakeClient(t, &v1.StreamClass{
		ObjectMeta: metav1.ObjectMeta{},
		Status: v1.StreamClassStatus{
			Phase: v1.PhaseReady,
		},
	})

	streamController := mocks.NewMockController[reconcile.Request](mockCtrl)
	streamController.EXPECT().Start(gomock.Any()).AnyTimes()

	streamReconcilerFactory := mocks.NewMockUnmanagedControllerFactory(mockCtrl)
	streamReconcilerFactory.EXPECT().CreateStreamController(gomock.Any(), gomock.Any(), gomock.Any()).Return(streamController, nil)

	reconciler := NewStreamClassReconciler(k8sClient, streamReconcilerFactory)

	// Start the stream controller first
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Stream class should be transitioned to Ready state again
	sc2 := &v1.StreamClass{}
	err = k8sClient.Get(t.Context(), types.NamespacedName{Name: name}, sc2)
	require.NoError(t, err)
	require.Equal(t, v1.PhaseReady, sc2.Status.Phase)

	err = k8sClient.Delete(t.Context(), sc2)
	require.NoError(t, err)

	// Act
	result, err = reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})

	// Assert
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})
}

func Test_UpdatePhase_Pending_ToStopped(t *testing.T) {
	// Arrange
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	k8sClient, name := setupFakeClient(t, &v1.StreamClass{
		ObjectMeta: metav1.ObjectMeta{},
	})

	streamReconcilerFactory := mocks.NewMockUnmanagedControllerFactory(mockCtrl)

	reconciler := NewStreamClassReconciler(k8sClient, streamReconcilerFactory)

	// Transit the stream class to Pending state first
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Stream class should be transitioned to Pending state
	sc2 := &v1.StreamClass{}
	err = k8sClient.Get(t.Context(), types.NamespacedName{Name: name}, sc2)
	require.NoError(t, err)
	require.Equal(t, v1.PhasePending, sc2.Status.Phase)

	err = k8sClient.Delete(t.Context(), sc2)
	require.NoError(t, err)

	// Act
	result, err = reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})

	// Assert
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})
}

func Test_UpdatePhase_Pending_ToFailed(t *testing.T) {
	// Arrange
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	k8sClient, name := setupFakeClient(t, &v1.StreamClass{
		ObjectMeta: metav1.ObjectMeta{},
		Status: v1.StreamClassStatus{
			Phase: v1.PhasePending,
		},
	})

	streamReconcilerFactory := mocks.NewMockUnmanagedControllerFactory(mockCtrl)
	streamReconcilerFactory.EXPECT().CreateStreamController(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("some error"))

	reconciler := NewStreamClassReconciler(k8sClient, streamReconcilerFactory)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	expectPhase(t, k8sClient, name, v1.PhaseFailed)
}

func Test_UpdatePhase_Ready_ToFailed(t *testing.T) {
	t.Skip("Flaky")

	// Arrange
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	k8sClient, name := setupFakeClient(t, &v1.StreamClass{
		ObjectMeta: metav1.ObjectMeta{},
		Status: v1.StreamClassStatus{
			Phase: v1.PhaseReady,
		},
	})

	completed := make(chan struct{})
	defer close(completed)
	streamController := mocks.NewMockController[reconcile.Request](mockCtrl)
	streamController.EXPECT().Start(gomock.Any()).Do(func(arg any) {
		completed <- struct{}{}
	}).Return(fmt.Errorf("some error"))

	streamReconcilerFactory := mocks.NewMockUnmanagedControllerFactory(mockCtrl)
	streamReconcilerFactory.EXPECT().CreateStreamController(gomock.Any(), gomock.Any(), gomock.Any()).Return(streamController, nil)

	cacheProvider := mocks.NewMockCacheProvider(mockCtrl)
	cacheProvider.EXPECT().GetCache().Return(nil).Times(1)

	reconciler := NewStreamClassReconciler(k8sClient, streamReconcilerFactory)

	// Start the stream controller first
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Name: name}})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Wait for the stream controller to start
	<-completed

	// Assert
	expectPhase(t, k8sClient, name, v1.PhaseFailed)
}

func expectPhase(t *testing.T, k8sClient client.WithWatch, name string, phase v1.Phase) {
	sc2 := &v1.StreamClass{}
	err := k8sClient.Get(t.Context(), types.NamespacedName{Name: name}, sc2)
	require.NoError(t, err)
	require.Equal(t, phase, sc2.Status.Phase)
}

func setupFakeClient(t *testing.T, sc *v1.StreamClass) (client.WithWatch, string) {
	name, err := uuid.NewUUID()
	require.NoError(t, err)
	scheme := runtime.NewScheme()
	_ = v1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	sc.Name = name.String()
	k8sClient := crfake.NewClientBuilder().WithStatusSubresource(&v1.StreamClass{}).WithScheme(scheme).WithObjects(sc).Build()
	return k8sClient, name.String()
}
