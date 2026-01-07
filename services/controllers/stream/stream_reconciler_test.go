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
	k8sClient := setupClient(nil, nil)
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

func Test_UpdatePhase_New_To_Pending(t *testing.T) {
	// Arrange
	k8sClient := setupClient(func(definition *testv1.MockStreamDefinition) { definition.Spec.Suspended = false }, nil)
	gvk := schema.GroupVersionKind{Group: "streaming.sneaksanddata.com", Version: "v1", Kind: "MockStreamDefinition"}
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "job1"}}
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil)
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
	require.Equal(t, "Pending", sd.Status.Phase)
}

func Test_UpdatePhase_Pending_To_Running_no_job(t *testing.T) {
	// Arrange
	k8sClient := setupClient(func(definition *testv1.MockStreamDefinition) { definition.Status.Phase = "Pending" }, nil)
	gvk := schema.GroupVersionKind{Group: "streaming.sneaksanddata.com", Version: "v1", Kind: "MockStreamDefinition"}
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "job1"}}
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil)
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
	require.Equal(t, "Running", sd.Status.Phase)

	newJob := &batchv1.Job{}
	err = k8sClient.Get(t.Context(), types.NamespacedName{Name: "job1"}, newJob)
	require.NoError(t, err)
}

func Test_UpdatePhase_Pending_To_Running_recreate_job(t *testing.T) {
	// Arrange
	namespace := "default"
	name := "stream1"
	k8sClient := setupClient(
		func(definition *testv1.MockStreamDefinition) {
			definition.Namespace = namespace
			definition.Name = name
			definition.Status.Phase = "Pending"
		},

		func(client2 *crfake.ClientBuilder) {
			client2.WithObjects(&batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      name,
					Annotations: map[string]string{
						"configuration-hash": "old-hash",
					},
				},
			})
		},
	)
	gvk := schema.GroupVersionKind{Group: "streaming.sneaksanddata.com", Version: "v1", Kind: "MockStreamDefinition"}
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	mockJob := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Annotations: map[string]string{
				"configuration-hash": "new-hash",
			},
		},
	}

	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil)
	reconciler := NewStreamReconciler(k8sClient, gvk, jobBuilder)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	sd := &testv1.MockStreamDefinition{}
	err = k8sClient.Get(t.Context(), types.NamespacedName{Namespace: namespace, Name: name}, sd)
	require.NoError(t, err)
	require.Equal(t, "Running", sd.Status.Phase)

	newJob := &batchv1.Job{}
	err = k8sClient.Get(t.Context(), types.NamespacedName{Namespace: namespace, Name: name}, newJob)
	require.NoError(t, err)

	jobConfiguration, err := StreamingJob(*newJob).CurrentConfiguration()
	require.NoError(t, err)

	require.Equal(t, jobConfiguration, "new-hash")
}

func Test_UpdatePhase_Pending_To_Running_not_recreate_job(t *testing.T) {
	// Arrange
	namespace := "default"
	name := "stream1"
	definitionHash := "add681440f7ac110a987718f47978201"
	k8sClient := setupClient(
		func(definition *testv1.MockStreamDefinition) {
			definition.Namespace = namespace
			definition.Name = name
			definition.Status.Phase = "Pending"
		},

		func(client2 *crfake.ClientBuilder) {
			client2.WithObjects(&batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      name,
					Annotations: map[string]string{
						"configuration-hash": definitionHash,
					},
				},
			})
		},
	)
	gvk := schema.GroupVersionKind{Group: "streaming.sneaksanddata.com", Version: "v1", Kind: "MockStreamDefinition"}
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	reconciler := NewStreamReconciler(k8sClient, gvk, jobBuilder)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	sd := &testv1.MockStreamDefinition{}
	err = k8sClient.Get(t.Context(), types.NamespacedName{Namespace: namespace, Name: name}, sd)
	require.NoError(t, err)
	require.Equal(t, "Running", sd.Status.Phase)

	newJob := &batchv1.Job{}
	err = k8sClient.Get(t.Context(), types.NamespacedName{Namespace: namespace, Name: name}, newJob)
	require.NoError(t, err)

	jobConfiguration, err := StreamingJob(*newJob).CurrentConfiguration()
	require.NoError(t, err)

	require.Equal(t, jobConfiguration, definitionHash)
}

func Test_UpdatePhase_Pending_To_Backfilling_no_job(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := setupClient(
		func(definition *testv1.MockStreamDefinition) {
			definition.Namespace = name.Namespace
			definition.Name = name.Name
			definition.Status.Phase = "Pending"
		},
		func(client2 *crfake.ClientBuilder) {
			client2.WithObjects(&v1.BackfillRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "backfill1", Namespace: name.Namespace},
				Spec: v1.BackfillRequestSpec{
					StreamClass: "MockStreamDefinition",
					StreamId:    name.Name,
				},
			})
		},
	)
	gvk := schema.GroupVersionKind{Group: "streaming.sneaksanddata.com", Version: "v1", Kind: "MockStreamDefinition"}
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name.Name, Namespace: name.Namespace}}
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil)
	reconciler := NewStreamReconciler(k8sClient, gvk, jobBuilder)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	sd := &testv1.MockStreamDefinition{}
	err = k8sClient.Get(t.Context(), name, sd)
	require.NoError(t, err)
	require.Equal(t, "Backfilling", sd.Status.Phase)

	newJob := &batchv1.Job{}
	err = k8sClient.Get(t.Context(), name, newJob)
	require.NoError(t, err)
}

func Test_UpdatePhase_Pending_To_Backfilling_recreate_job(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := setupClient(
		func(definition *testv1.MockStreamDefinition) {
			definition.Namespace = name.Namespace
			definition.Name = name.Name
			definition.Status.Phase = "Pending"
		},
		func(client2 *crfake.ClientBuilder) {
			client2.WithObjects(&v1.BackfillRequest{
				ObjectMeta: metav1.ObjectMeta{Name: "backfill1", Namespace: name.Namespace},
				Spec: v1.BackfillRequestSpec{
					StreamClass: "MockStreamDefinition",
					StreamId:    name.Name,
				},
			})
			client2.WithObjects(&batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: name.Namespace,
					Name:      name.Name,
					Annotations: map[string]string{
						"configuration-hash": "old-hash",
					},
				},
			})
		},
	)
	gvk := schema.GroupVersionKind{Group: "streaming.sneaksanddata.com", Version: "v1", Kind: "MockStreamDefinition"}
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	mockJob := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: name.Namespace,
			Name:      name.Name,
			Annotations: map[string]string{
				"configuration-hash": "new-hash",
			},
		},
	}
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil)
	reconciler := NewStreamReconciler(k8sClient, gvk, jobBuilder)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	sd := &testv1.MockStreamDefinition{}
	err = k8sClient.Get(t.Context(), name, sd)
	require.NoError(t, err)
	require.Equal(t, "Backfilling", sd.Status.Phase)

	newJob := &batchv1.Job{}
	err = k8sClient.Get(t.Context(), name, newJob)
	require.NoError(t, err)

	jobConfiguration, err := StreamingJob(*newJob).CurrentConfiguration()
	require.NoError(t, err)

	require.Equal(t, jobConfiguration, "new-hash")
}

func setupClient(definition func(definition *testv1.MockStreamDefinition), addResources func(client2 *crfake.ClientBuilder)) client.Client {
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
	if definition != nil {
		definition(obj)
	}

	clientBuilder := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(obj).WithStatusSubresource(&testv1.MockStreamDefinition{})
	if addResources != nil {
		addResources(clientBuilder)
	}
	return clientBuilder.Build()
}
