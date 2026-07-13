package v0

import (
	"strings"
	"testing"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	testv1 "github.com/SneaksAndData/arcane-operator/pkg/test/apis_test/streaming/v1"
	v2 "github.com/SneaksAndData/arcane-operator/pkg/test/generated/applyconfiguration/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/contracts"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend/cron_job"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend/job"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/tests/helpers"
	v4 "github.com/SneaksAndData/arcane-operator/services/controllers/stream/tests/helpers/v1"
	"github.com/SneaksAndData/arcane-operator/tests/mocks"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var objectName = types.NamespacedName{Name: "stream1", Namespace: "default"}

func Test_UpdatePhase_New_To_Suspended(t *testing.T) {
	// Arrange
	k8sClient := helpers.SetupClientFromBuilders(v4.NewMockStreamDefinitionBuilder(objectName), nil, nil)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
}

func Test_UpdatePhase_New_To_Pending(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithSuspendedSpec(false)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, nil)
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
}

func Test_UpdatePhase_Pending_To_Running_no_job(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Pending)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, nil)
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	// Fetch the object and ensure its status Phase is stream.Pending
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Running)
	assertJobExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Pending_To_Running_recreate_job(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Pending)
	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithOutdatedCronJob(objectName)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, resourceBuilder)

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
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Running)
	assertJobExists(t, k8sClient, objectName)
	assertJobConfiguration(t, k8sClient, objectName, "new-hash")
}

func Test_UpdatePhase_Pending_To_Running_not_recreate_job(t *testing.T) {
	// Arrange
	streamDefinitionBuilder := v4.NewMockStreamDefinitionBuilder(objectName).
		WithPhase(stream.Pending).
		WithName(objectName)
	k8sClient := helpers.SetupClientFromBuilders(streamDefinitionBuilder, nil, nil)

	u, err := helpers.GetStreamDefinitionUnstructured(t.Context(), k8sClient, objectName, helpers.GroupVersionKindV1)
	require.NoError(t, err)

	def, err := contracts.FromUnstructured(u)
	require.NoError(t, err)

	definitionHash, err := def.CurrentConfiguration(nil)
	require.NoError(t, err)

	resources := helpers.NewFakeClientResourcesBuilder().WithConsistentJob(objectName, definitionHash)
	k8sClient = helpers.SetupClientFromBuilders(streamDefinitionBuilder, nil, resources)

	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Running)
	assertJobExists(t, k8sClient, objectName)
	assertJobConfiguration(t, k8sClient, objectName, definitionHash)
}

func Test_UpdatePhase_Pending_To_Backfilling_no_job(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Pending)
	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, resourceBuilder)
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Backfilling)
	assertJobExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Pending_To_Backfilling_recreate_job(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Pending)
	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName).WithOutdatedJob(objectName)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, resourceBuilder)

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
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Backfilling)
	assertJobExists(t, k8sClient, objectName)
	assertJobConfiguration(t, k8sClient, objectName, "new-hash")
}

func Test_UpdatePhase_Running_To_Suspended_no_job(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Running).WithSuspendedSpec(true)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, nil)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
}

func Test_UpdatePhase_Running_To_Suspended_stop_job(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Running).WithSuspendedSpec(true)
	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName).WithOutdatedJob(objectName)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, resourceBuilder)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
	assertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Running_To_Suspended_to_Pending(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Suspended).WithSuspendedSpec(false)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, nil)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
}

func Test_UpdatePhase_Running_To_Suspended_to_Pending_With_BFR(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Suspended).WithSuspendedSpec(true)
	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, resourceBuilder)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
}

func Test_UpdatePhase_Running_with_BackfillRequest_no_job(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Running).WithSuspendedSpec(false)
	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, resourceBuilder)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
}

func Test_UpdatePhase_Suspended_with_BackfillRequest(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Suspended)
	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, resourceBuilder)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
}

func Test_UpdatePhase_Suspended_without_BackfillRequest_without_job(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Suspended)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, nil)

	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
	assertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Suspended_without_BackfillRequest_with_job(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Suspended)
	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithOutdatedJob(objectName)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, resourceBuilder)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
	assertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Pending_with_job_running(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Backfilling).WithSuspendedSpec(false)
	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithOutdatedJob(objectName).WithBackfillRequest(objectName)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, resourceBuilder)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Backfilling)
	assertJobExists(t, k8sClient, objectName)
	assertBackfillRequestNotCompleted(t, k8sClient)
}

func Test_UpdatePhase_Backfilling_To_Pending_with_job_completed(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Backfilling).WithSuspendedSpec(false)
	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithCompletedJob(objectName).WithBackfillRequest(objectName)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, resourceBuilder)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
	assertJobNotExists(t, k8sClient, objectName)
	assertBackfillRequestCompleted(t, k8sClient)
}

func Test_UpdatePhase_Backfilling_To_Backfilling_with_no_job(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Backfilling).WithSuspendedSpec(false)
	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, resourceBuilder)

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
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Backfilling)
	assertJobExists(t, k8sClient, objectName)
	assertBackfillRequestNotCompleted(t, k8sClient)
}

func Test_UpdatePhase_Backfilling_To_Suspended(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Backfilling).WithSuspendedSpec(true)
	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, resourceBuilder)

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
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
	assertJobNotExists(t, k8sClient, objectName)
	assertBackfillRequestNotCompleted(t, k8sClient)
}

func Test_UpdatePhase_Backfilling_Job_Failed(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Backfilling)
	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName).WithFailedJob(objectName)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, resourceBuilder)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Failed)
	assertJobNotExists(t, k8sClient, objectName)
	assertBackfillRequestNotCompleted(t, k8sClient)
}

func Test_UpdatePhase_Backfilling_To_Running(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Backfilling).WithSuspendedSpec(false)
	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithOutdatedCronJob(objectName)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, resourceBuilder)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: objectName.Namespace, Name: objectName.Name}}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
	assertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Failed_to_Failed(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Failed)
	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithFailedJob(objectName)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, resourceBuilder)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Failed)
	assertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Failed_to_Failed_without_job(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Failed)
	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithFailedJob(objectName)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, resourceBuilder)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Failed)
}

func Test_UpdatePhase_Failed_to_Suspended_without_job(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Failed)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, nil)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
}

func Test_UpdatePhase_Failed_to_Suspended_with_BackfillRequest(t *testing.T) {
	// Arrange
	builder := v4.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Failed).WithSuspendedSpec(true)
	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName)
	k8sClient := helpers.SetupClientFromBuilders(builder, nil, resourceBuilder)

	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
	assertBackfillRequestNotCompleted(t, k8sClient)
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
	statusManager := stream.NewDefaultStatusManager(k8sClient, gvk, &sc, contracts.FromUnstructured)
	backfillBackendResourceManager := job.NewBackfillBackendResourceManager(&sc, k8sClient, statusManager, recorder)
	backendResourceManagers := map[stream.Backend]stream.BackendResourceManager{
		stream.BatchJob: job.NewJobBackend(k8sClient, jobBuilder, recorder, statusManager),
		stream.CronJob:  cron_job.NewCronJobBackend(k8sClient, jobBuilder, recorder, statusManager),
	}
	return stream.NewStreamReconciler(k8sClient, gvk, jobBuilder, &sc, recorder, contracts.FromUnstructured, backendResourceManagers, backfillBackendResourceManager)
}

func assertStreamDefinitionPhase(t *testing.T, k8sClient client.Client, name types.NamespacedName, phase stream.Phase) {
	sd := &testv1.MockStreamDefinition{}
	err := k8sClient.Get(t.Context(), name, sd)
	require.NoError(t, err)
	require.Equal(t, string(phase), sd.Status.Phase)
}

func assertJobExists(t *testing.T, k8sClient client.Client, name types.NamespacedName) {
	newJob := &batchv1.Job{}
	err := k8sClient.Get(t.Context(), name, newJob)
	require.NoError(t, err)
}

func assertJobNotExists(t *testing.T, k8sClient client.Client, name types.NamespacedName) {
	newJob := &batchv1.Job{}
	err := k8sClient.Get(t.Context(), name, newJob)
	require.True(t, errors.IsNotFound(err))
}

func assertJobConfiguration(t *testing.T, k8sClient client.Client, name types.NamespacedName, expectedConfiguration string) {
	newJob := &batchv1.Job{}
	err := k8sClient.Get(t.Context(), name, newJob)
	require.NoError(t, err)

	j, err := job.FromResource(newJob)
	require.NoError(t, err)

	jobConfiguration, err := j.CurrentConfiguration()
	require.NoError(t, err)
	require.Equal(t, jobConfiguration, expectedConfiguration)
}

func assertBackfillRequestNotCompleted(t *testing.T, k8sClient client.Client) {
	backfillRequest := &v1.BackfillRequest{}
	err := k8sClient.Get(t.Context(), types.NamespacedName{Name: "backfill1", Namespace: objectName.Namespace}, backfillRequest)
	require.NoError(t, err)
	require.False(t, backfillRequest.Spec.Completed)
}

func assertBackfillRequestCompleted(t *testing.T, k8sClient client.Client) {
	backfillRequest := &v1.BackfillRequest{}
	err := k8sClient.Get(t.Context(), types.NamespacedName{Name: "backfill1", Namespace: objectName.Namespace}, backfillRequest)
	require.NoError(t, err)
	require.True(t, backfillRequest.Spec.Completed)
}
