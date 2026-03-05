package stream

import (
	"strings"
	"testing"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	testv1 "github.com/SneaksAndData/arcane-operator/pkg/test/apis_test/streaming/v1"
	v2 "github.com/SneaksAndData/arcane-operator/pkg/test/generated/applyconfiguration/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/tests/mocks"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var objectName types.NamespacedName = types.NamespacedName{Name: "stream1", Namespace: "default"}

func Test_UpdatePhase_New_To_Suspended(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName, nil, nil)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Suspended)
}

func Test_UpdatePhase_New_To_Pending(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName, WithSuspendedSpec(false), nil)
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Pending)
}

func Test_UpdatePhase_Pending_To_Running_no_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName, WithPhase(Pending), nil)
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	// Fetch the object and ensure its status Phase is Pending
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Running)
	AssertJobExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Pending_To_Running_recreate_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName, Combined(WithPhase(Pending), WithNamedStreamDefinition(objectName)), WithOutdatedJob(objectName))

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockJob := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: objectName.Namespace,
			Name:      objectName.Name,
			Annotations: map[string]string{
				"configuration-hash": "new-hash",
			},
		},
	}

	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Running)
	AssertJobExists(t, k8sClient, objectName)
	AssertJobConfiguration(t, k8sClient, objectName, "new-hash")
}

func Test_UpdatePhase_Pending_To_Running_not_recreate_job(t *testing.T) {
	// Arrange
	definitionHash := "0xdeadbeef" // computed manually for the test definition

	k8sClient := SetupClient(objectName, Combined(WithPhase(Pending), WithNamedStreamDefinition(objectName)), WithConsistentJob(objectName, definitionHash))

	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Running)
	AssertJobExists(t, k8sClient, objectName)
	AssertJobConfiguration(t, k8sClient, objectName, definitionHash)
}

func Test_UpdatePhase_Pending_To_Backfilling_no_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName, Combined(WithPhase(Pending), WithNamedStreamDefinition(objectName)), WithBackfillRequest(objectName))
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Backfilling)
	AssertJobExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Pending_To_Backfilling_recreate_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithPhase(Pending), WithNamedStreamDefinition(objectName)),
		CombinedB(WithBackfillRequest(objectName), WithOutdatedJob(objectName)),
	)

	mockJob := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: objectName.Namespace,
			Name:      objectName.Name,
			Annotations: map[string]string{
				"configuration-hash": "new-hash",
			},
		},
	}
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Backfilling)
	AssertJobExists(t, k8sClient, objectName)
	AssertJobConfiguration(t, k8sClient, objectName, "new-hash")
}

func Test_UpdatePhase_Running_To_Suspended_no_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName, Combined(WithPhase(Running), WithSuspendedSpec(true)), nil)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Suspended)
}

func Test_UpdatePhase_Running_To_Suspended_stop_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), Combined(WithPhase(Running), WithSuspendedSpec(true))),
		CombinedB(WithBackfillRequest(objectName), WithOutdatedJob(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Suspended)
	AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Running_To_Suspended_to_Pending(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), Combined(WithPhase(Suspended), WithSuspendedSpec(false))),
		nil,
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Pending)
}

func Test_UpdatePhase_Running_To_Suspended_to_Pending_With_BFR(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), Combined(WithPhase(Suspended), WithSuspendedSpec(true))),
		CombinedB(WithBackfillRequest(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Pending)
}

func Test_UpdatePhase_Running_with_BackfillRequest_no_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(Running), WithSuspendedSpec(false)),
		CombinedB(WithBackfillRequest(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Pending)
}

func Test_UpdatePhase_Suspended_with_BackfillRequest(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(Suspended), WithSuspendedSpec(false)),
		CombinedB(WithBackfillRequest(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Pending)
}

func Test_UpdatePhase_Suspended_without_BackfillRequest_without_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(Suspended)),
		nil,
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Suspended)
	AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Suspended_without_BackfillRequest_with_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(Suspended)),
		CombinedB(WithOutdatedJob(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Suspended)
	AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Pending_with_job_running(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(Backfilling), WithSuspendedSpec(false)),
		CombinedB(WithOutdatedJob(objectName), WithBackfillRequest(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Backfilling)
	AssertJobExists(t, k8sClient, objectName)
	AssertBackfillRequestNotCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Pending_with_job_completed(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(Backfilling), WithSuspendedSpec(false)),
		CombinedB(WithBackfillRequest(objectName), WithCompletedJob(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Pending)
	AssertJobNotExists(t, k8sClient, objectName)
	AssertBackfillRequestCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Backfilling_with_no_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(Backfilling), WithSuspendedSpec(false)),
		CombinedB(WithBackfillRequest(objectName)),
	)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockJob := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: objectName.Namespace,
			Name:      objectName.Name,
			Annotations: map[string]string{
				"configuration-hash": "new-hash",
			},
		},
	}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Backfilling)
	AssertJobExists(t, k8sClient, objectName)
	AssertBackfillRequestNotCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Suspended(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(Backfilling), WithSuspendedSpec(true)),
		CombinedB(WithBackfillRequest(objectName)),
	)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockJob := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: objectName.Namespace,
			Name:      objectName.Name,
			Annotations: map[string]string{
				"configuration-hash": "new-hash",
			},
		},
	}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Suspended)
	AssertJobNotExists(t, k8sClient, objectName)
	AssertBackfillRequestCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_Job_Failed(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(Backfilling), WithSuspendedSpec(false)),
		CombinedB(WithBackfillRequest(objectName), WithFailedJob(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Failed)
	AssertJobNotExists(t, k8sClient, objectName)
	AssertBackfillRequestCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Running(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(Backfilling), WithSuspendedSpec(false)),
		WithOutdatedJob(objectName),
	)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: objectName.Namespace, Name: objectName.Name}}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Pending)
	AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Failed_to_Failed(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(name), WithPhase(Failed)),
		CombinedB(WithFailedJob(name)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Failed)
	AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Failed_to_Failed_without_job(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := SetupClient(objectName, Combined(WithNamedStreamDefinition(name), WithPhase(Failed), WithSuspendedSpec(false)), nil)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Failed)
}

func Test_UpdatePhase_Failed_to_Suspended_without_job(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := SetupClient(objectName, Combined(WithNamedStreamDefinition(name), WithPhase(Failed)), nil)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Suspended)
}

func Test_UpdatePhase_Failed_to_Suspended_with_BackfillRequest(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := SetupClient(objectName, Combined(WithNamedStreamDefinition(name), WithPhase(Failed)), WithBackfillRequest(objectName))
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Suspended)
	AssertBackfillRequestCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Failed_to_Backfilling(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(name), WithPhase(Failed), WithSuspendedSpec(false)),
		WithBackfillRequest(objectName),
	)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, Backfilling)
	AssertBackfillRequestNotCompleted(t, k8sClient, objectName)
}

func createReconciler(k8sClient client.Client, mockJob *batchv1.Job, mockCtrl *gomock.Controller) reconcile.Reconciler {
	var jobBuilder *mocks.MockJobBuilder
	if mockJob != nil {
		jobBuilder = mocks.NewMockJobBuilder(mockCtrl)
		jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockJob, nil).AnyTimes()
	}
	recorder := record.NewFakeRecorder(10)
	gvk := schema.GroupVersionKind{Group: "streaming.sneaksanddata.com", Version: "v1", Kind: "MockStreamDefinition"}
	mock := v2.MockStreamDefinition("name", "namespace")
	sc := v1.StreamClass{
		ObjectMeta: metav1.ObjectMeta{Name: "stream-class"},
		Spec: v1.StreamClassSpec{
			APIGroupRef: strings.Split(*mock.GetAPIVersion(), "/")[0],
			APIVersion:  strings.Split(*mock.GetAPIVersion(), "/")[1],
			KindRef:     *mock.Kind,
			PluralName:  "mockstreamdefinitions",
		},
	}
	definitionParser := func(u *unstructured.Unstructured) (Definition, error) {
		var mock testv1.MockStreamDefinition
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &mock); err != nil {
			return nil, err
		}
		return NewMockDefinitionWrapper(&mock)
	}
	statusManager := NewDefaultStatusManager(k8sClient, gvk, &sc, definitionParser)
	backendResourceManager := NewJobBackend(k8sClient, jobBuilder, recorder, statusManager)
	backfillBackendResourceManager := NewBackfillBackendResourceManager(&sc, k8sClient, statusManager)
	return NewStreamReconciler(k8sClient, gvk, jobBuilder, &sc, recorder, definitionParser, backendResourceManager, backfillBackendResourceManager)
}
