package tests

import (
	"strings"
	"testing"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	testv2 "github.com/SneaksAndData/arcane-operator/pkg/test/apis_test/streaming/v2"
	v2 "github.com/SneaksAndData/arcane-operator/pkg/test/generated/applyconfiguration/streaming/v2"
	"github.com/SneaksAndData/arcane-operator/services/controllers/contracts"
	stream2 "github.com/SneaksAndData/arcane-operator/services/controllers/stream"
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
	k8sClient := stream2.SetupClient(objectName, nil, nil)
	reconciler := createReconciler(k8sClient, nil, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Suspended)
}

func Test_UpdatePhase_New_To_Pending(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName, stream2.WithSuspendedSpec(false), nil)
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	reconciler := createReconciler(k8sClient, nil, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Pending)
}

func Test_UpdatePhase_Pending_To_Running_no_job(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName, stream2.WithPhase(stream2.Pending), nil)
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	// Fetch the object and ensure its status Phase is Pending
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Running)
	stream2.AssertJobExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Pending_To_Running_recreate_job(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName, stream2.Combined(stream2.WithPhase(stream2.Pending), stream2.WithNamedStreamDefinition(objectName)), stream2.WithOutdatedJob(objectName))

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

	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Running)
	stream2.AssertJobExists(t, k8sClient, objectName)
	stream2.AssertJobConfiguration(t, k8sClient, objectName, "new-hash")
}

func Test_UpdatePhase_Pending_To_Running_not_recreate_job(t *testing.T) {
	// Arrange
	definitionHash := "0xdeadbeef" // computed manually for the test definition

	k8sClient := stream2.SetupClient(objectName, stream2.Combined(stream2.WithPhase(stream2.Pending), stream2.WithNamedStreamDefinition(objectName)), stream2.WithConsistentJob(objectName, definitionHash))

	reconciler := createReconciler(k8sClient, nil, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Running)
	stream2.AssertJobExists(t, k8sClient, objectName)
	stream2.AssertJobConfiguration(t, k8sClient, objectName, definitionHash)
}

func Test_UpdatePhase_Pending_To_Backfilling_no_job(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName, stream2.Combined(stream2.WithPhase(stream2.Pending), stream2.WithNamedStreamDefinition(objectName)), stream2.WithBackfillRequest(objectName))
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Backfilling)
	stream2.AssertJobExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Pending_To_Backfilling_recreate_job(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName,
		stream2.Combined(stream2.WithPhase(stream2.Pending), stream2.WithNamedStreamDefinition(objectName)),
		stream2.CombinedB(stream2.WithBackfillRequest(objectName), stream2.WithOutdatedJob(objectName)),
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

	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Backfilling)
	stream2.AssertJobExists(t, k8sClient, objectName)
	stream2.AssertJobConfiguration(t, k8sClient, objectName, "new-hash")
}

func Test_UpdatePhase_Running_To_Suspended_no_job(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName, stream2.Combined(stream2.WithPhase(stream2.Running), stream2.WithSuspendedSpec(true)), nil)
	reconciler := createReconciler(k8sClient, nil, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Suspended)
}

func Test_UpdatePhase_Running_To_Suspended_stop_job(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName,
		stream2.Combined(stream2.WithNamedStreamDefinition(objectName), stream2.Combined(stream2.WithPhase(stream2.Running), stream2.WithSuspendedSpec(true))),
		stream2.CombinedB(stream2.WithBackfillRequest(objectName), stream2.WithOutdatedJob(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Suspended)
	stream2.AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Running_to_Pending_with_schedule(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName,
		stream2.Combined(
			stream2.WithNamedStreamDefinition(objectName),
			stream2.WithApiVersion("v1"),
			stream2.WithPhase(stream2.Running),
			stream2.WithSuspendedSpec(false),
			stream2.WithSchedule("* * * * *"),
		),
		stream2.WithOutdatedJob(objectName),
	)

	reconciler := createReconciler(k8sClient, nil, nil, contracts.FromUnstructured)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Pending)
	stream2.AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Suspended_to_Pending(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName,
		stream2.Combined(stream2.WithNamedStreamDefinition(objectName), stream2.Combined(stream2.WithPhase(stream2.Suspended), stream2.WithSuspendedSpec(false))),
		nil,
	)
	reconciler := createReconciler(k8sClient, nil, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Pending)
}

func Test_UpdatePhase_Suspended_to_Pending_With_BFR(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName,
		stream2.Combined(stream2.WithNamedStreamDefinition(objectName), stream2.Combined(stream2.WithPhase(stream2.Suspended), stream2.WithSuspendedSpec(true))),
		stream2.CombinedB(stream2.WithBackfillRequest(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Pending)
}

func Test_UpdatePhase_Running_with_BackfillRequest_no_job(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName,
		stream2.Combined(stream2.WithNamedStreamDefinition(objectName), stream2.WithPhase(stream2.Running), stream2.WithSuspendedSpec(false)),
		stream2.CombinedB(stream2.WithBackfillRequest(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Pending)
}

func Test_UpdatePhase_Suspended_with_BackfillRequest(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName,
		stream2.Combined(stream2.WithNamedStreamDefinition(objectName), stream2.WithPhase(stream2.Suspended), stream2.WithSuspendedSpec(false)),
		stream2.CombinedB(stream2.WithBackfillRequest(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Pending)
}

func Test_UpdatePhase_Suspended_without_BackfillRequest_without_job(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName,
		stream2.Combined(stream2.WithNamedStreamDefinition(objectName), stream2.WithPhase(stream2.Suspended)),
		nil,
	)
	reconciler := createReconciler(k8sClient, nil, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Suspended)
	stream2.AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Suspended_without_BackfillRequest_with_job(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName,
		stream2.Combined(stream2.WithNamedStreamDefinition(objectName), stream2.WithPhase(stream2.Suspended)),
		stream2.CombinedB(stream2.WithOutdatedJob(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Suspended)
	stream2.AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Pending_with_job_running(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName,
		stream2.Combined(stream2.WithNamedStreamDefinition(objectName), stream2.WithPhase(stream2.Backfilling), stream2.WithSuspendedSpec(false)),
		stream2.CombinedB(stream2.WithOutdatedJob(objectName), stream2.WithBackfillRequest(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Backfilling)
	stream2.AssertJobExists(t, k8sClient, objectName)
	stream2.AssertBackfillRequestNotCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Pending_with_job_completed(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName,
		stream2.Combined(stream2.WithNamedStreamDefinition(objectName), stream2.WithPhase(stream2.Backfilling), stream2.WithSuspendedSpec(false)),
		stream2.CombinedB(stream2.WithBackfillRequest(objectName), stream2.WithCompletedJob(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Pending)
	stream2.AssertJobNotExists(t, k8sClient, objectName)
	stream2.AssertBackfillRequestCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Backfilling_with_no_job(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName,
		stream2.Combined(stream2.WithNamedStreamDefinition(objectName), stream2.WithPhase(stream2.Backfilling), stream2.WithSuspendedSpec(false)),
		stream2.CombinedB(stream2.WithBackfillRequest(objectName)),
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
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Backfilling)
	stream2.AssertJobExists(t, k8sClient, objectName)
	stream2.AssertBackfillRequestNotCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Suspended(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName,
		stream2.Combined(stream2.WithNamedStreamDefinition(objectName), stream2.WithPhase(stream2.Backfilling), stream2.WithSuspendedSpec(true)),
		stream2.CombinedB(stream2.WithBackfillRequest(objectName)),
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
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Suspended)
	stream2.AssertJobNotExists(t, k8sClient, objectName)
	stream2.AssertBackfillRequestCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_Job_Failed(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName,
		stream2.Combined(stream2.WithNamedStreamDefinition(objectName), stream2.WithPhase(stream2.Backfilling), stream2.WithSuspendedSpec(false)),
		stream2.CombinedB(stream2.WithBackfillRequest(objectName), stream2.WithFailedJob(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Failed)
	stream2.AssertJobNotExists(t, k8sClient, objectName)
	stream2.AssertBackfillRequestCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Running(t *testing.T) {
	// Arrange
	k8sClient := stream2.SetupClient(objectName,
		stream2.Combined(stream2.WithNamedStreamDefinition(objectName), stream2.WithPhase(stream2.Backfilling), stream2.WithSuspendedSpec(false)),
		stream2.WithOutdatedJob(objectName),
	)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: objectName.Namespace, Name: objectName.Name}}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Pending)
	stream2.AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Failed_to_Failed(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := stream2.SetupClient(objectName,
		stream2.Combined(stream2.WithNamedStreamDefinition(name), stream2.WithPhase(stream2.Failed)),
		stream2.CombinedB(stream2.WithFailedJob(name)),
	)
	reconciler := createReconciler(k8sClient, nil, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Failed)
	stream2.AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Failed_to_Failed_without_job(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := stream2.SetupClient(objectName, stream2.Combined(stream2.WithNamedStreamDefinition(name), stream2.WithPhase(stream2.Failed), stream2.WithSuspendedSpec(false)), nil)
	reconciler := createReconciler(k8sClient, nil, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Failed)
}

func Test_UpdatePhase_Failed_to_Suspended_without_job(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := stream2.SetupClient(objectName, stream2.Combined(stream2.WithNamedStreamDefinition(name), stream2.WithPhase(stream2.Failed)), nil)
	reconciler := createReconciler(k8sClient, nil, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Suspended)
}

func Test_UpdatePhase_Failed_to_Suspended_with_BackfillRequest(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := stream2.SetupClient(objectName, stream2.Combined(stream2.WithNamedStreamDefinition(name), stream2.WithPhase(stream2.Failed)), stream2.WithBackfillRequest(objectName))
	reconciler := createReconciler(k8sClient, nil, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Suspended)
	stream2.AssertBackfillRequestCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Failed_to_Backfilling(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := stream2.SetupClient(objectName,
		stream2.Combined(stream2.WithNamedStreamDefinition(name), stream2.WithPhase(stream2.Failed), stream2.WithSuspendedSpec(false)),
		stream2.WithBackfillRequest(objectName),
	)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	stream2.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream2.Backfilling)
	stream2.AssertBackfillRequestNotCompleted(t, k8sClient, objectName)
}

func createReconciler(k8sClient client.Client, mockJob *batchv1.Job, mockCtrl *gomock.Controller, definitionParser stream2.DefinitionParser) reconcile.Reconciler {
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
	if definitionParser == nil {
		definitionParser = func(u *unstructured.Unstructured) (stream2.Definition, error) {
			var mock testv2.MockStreamDefinition
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &mock); err != nil {
				return nil, err
			}
			return stream2.NewMockDefinitionWrapper(&mock)
		}
	}
	statusManager := stream2.NewDefaultStatusManager(k8sClient, gvk, &sc, definitionParser)
	backfillBackendResourceManager := stream2.NewBackfillBackendResourceManager(&sc, k8sClient, statusManager)
	backendResourceManagers := map[stream2.Backend]stream2.BackendResourceManager{
		stream2.BatchJob: stream2.NewJobBackend(k8sClient, jobBuilder, recorder, statusManager),
		stream2.CronJob:  stream2.NewJobBackend(k8sClient, jobBuilder, recorder, statusManager),
	}
	return stream2.NewStreamReconciler(k8sClient, gvk, jobBuilder, &sc, recorder, definitionParser, backendResourceManagers, backfillBackendResourceManager)
}
