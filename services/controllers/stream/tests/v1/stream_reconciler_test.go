package v1

import (
	"strings"
	"testing"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	v2 "github.com/SneaksAndData/arcane-operator/pkg/test/generated/applyconfiguration/streaming/v2"
	"github.com/SneaksAndData/arcane-operator/services/controllers/contracts"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend/cron_job"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend/empty"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend/job"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/tests/helpers"
	v3 "github.com/SneaksAndData/arcane-operator/services/controllers/stream/tests/helpers/v2"
	"github.com/SneaksAndData/arcane-operator/tests/mocks"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	batchv1 "k8s.io/api/batch/v1"
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
	k8sClient := helpers.SetupClientFromBuilders(nil, v3.NewMockStreamDefinitionBuilder(objectName), nil)
	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
}

func Test_UpdatePhase_New_To_Pending(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithSuspendedSpec(false)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, nil)

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
	helpers.AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_New_To_Pending_with_schedule(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithSuspendedSpec(false).WithSchedule("* * * * *")
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, nil)

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertBackfillRequests(t, k8sClient, func(bfrList *v1.BackfillRequestList, err error) {
		require.Equal(t, 0, len(bfrList.Items))
	})
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
	helpers.AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_New_To_Pending_with_no_backend(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithSuspendedSpec(false).WithNoBackend()

	k8sClient := helpers.SetupClientFromBuilders(nil, builder, nil)

	reconciler, recorder := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.New)
	helpers.AssertEventRecorded(t, recorder, objectName, func(t *testing.T, event string) {
		require.Contains(t, event, "Warning NoValidBackend")
	})
	helpers.AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Pending_To_Running_no_job(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithSuspendedSpec(false).WithPhase(stream.Pending)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, nil)
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil).AnyTimes()
	reconciler, _ := createReconciler(k8sClient, jobBuilder, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	// Fetch the object and ensure its status Phase is Pending
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Running)
	helpers.AssertJobExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Pending_To_Running_recreate_job(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithSuspendedSpec(false).WithPhase(stream.Pending)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithOutdatedJob(objectName))

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

	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil).AnyTimes()
	reconciler, _ := createReconciler(k8sClient, jobBuilder, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Running)
	helpers.AssertJobExists(t, k8sClient, objectName)
	helpers.AssertJobConfiguration(t, k8sClient, objectName, "new-hash")
}

func Test_UpdatePhase_Pending_To_Running_not_recreate_job(t *testing.T) {
	// Arrange
	// Generate hash for current configuration
	streamDefinitionBuilder := v3.NewMockStreamDefinitionBuilder(objectName).
		WithPhase(stream.Pending).
		WithName(objectName)
	k8sClient := helpers.SetupClientFromBuilders(nil, streamDefinitionBuilder, nil)

	u, err := helpers.GetStreamDefinitionUnstructured(t.Context(), k8sClient, objectName, helpers.GroupVersionKindV2)
	require.NoError(t, err)

	def, err := contracts.FromUnstructured(u)
	require.NoError(t, err)

	definitionHash, err := def.CurrentConfiguration(nil)
	require.NoError(t, err)

	// Create the fake client and resources
	resources := helpers.NewFakeClientResourcesBuilder().WithConsistentJob(objectName, definitionHash)
	k8sClient = helpers.SetupClientFromBuilders(nil, streamDefinitionBuilder, resources)
	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Running)
	helpers.AssertJobExists(t, k8sClient, objectName)
	helpers.AssertJobConfiguration(t, k8sClient, objectName, definitionHash)
}

func Test_UpdatePhase_Pending_To_Scheduled_no_job(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).
		WithPhase(stream.Pending).
		WithSchedule("* * * * *").
		WithSuspendedSpec(false)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, nil)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil).AnyTimes()
	reconciler, _ := createReconciler(k8sClient, jobBuilder, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	// Fetch the object and ensure its status Phase is Pending
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Scheduled)
	helpers.AssertCronJobExists(t, k8sClient, objectName, func(t *testing.T, cj *batchv1.CronJob) {
		require.Equal(t, "* * * * *", cj.Spec.Schedule)
		require.Equal(t, batchv1.ForbidConcurrent, cj.Spec.ConcurrencyPolicy)
	})
}

func Test_UpdatePhase_Pending_To_Backfilling_no_job(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Pending)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName))

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil).AnyTimes()
	reconciler, _ := createReconciler(k8sClient, jobBuilder, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Backfilling)
	helpers.AssertJobExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Pending_To_Backfilling_recreate_job(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Pending)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName).WithOutdatedJob(objectName))

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

	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil).AnyTimes()
	reconciler, _ := createReconciler(k8sClient, jobBuilder, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Backfilling)
	helpers.AssertJobExists(t, k8sClient, objectName)
	helpers.AssertJobConfiguration(t, k8sClient, objectName, "new-hash")
}

func Test_UpdatePhase_Running_To_Suspended_no_job(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Running).WithSuspendedSpec(true)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, nil)

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
}

func Test_UpdatePhase_Running_To_Suspended_stop_job(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Running).WithSuspendedSpec(true)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName).WithOutdatedJob(objectName))

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
	helpers.AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Running_To_Suspended_to_Pending(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Suspended).WithSuspendedSpec(false)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, nil)

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
}

func Test_UpdatePhase_Running_To_Suspended_to_Pending_With_BFR(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Suspended).WithSuspendedSpec(true)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName))

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
}

func Test_UpdatePhase_Running_To_Pending_with_schedule(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Running).WithSuspendedSpec(false).WithSchedule("* * * * *")
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithOutdatedJob(objectName))

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
	helpers.AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Running_with_BackfillRequest_no_job(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Running).WithSuspendedSpec(false)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName))

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
}

func Test_UpdatePhase_Suspended_with_BackfillRequest(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Suspended).WithSuspendedSpec(false)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName))

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
}

func Test_UpdatePhase_Suspended_without_BackfillRequest_without_job(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Suspended)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, nil)

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
	helpers.AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Suspended_without_BackfillRequest_with_job(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Suspended)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithOutdatedJob(objectName))

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
	helpers.AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Pending_with_job_completed(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Backfilling).WithSuspendedSpec(false)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName).WithCompletedJob(objectName))

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
	helpers.AssertJobNotExists(t, k8sClient, objectName)
	helpers.AssertBackfillRequestCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Pending_with_deleted_bfr(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Backfilling).WithSuspendedSpec(false)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithOutdatedJob(objectName))

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	helpers.AssertJobExists(t, k8sClient, objectName)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
	helpers.AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Backfilling_with_job_running(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Backfilling).WithSuspendedSpec(false)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithOutdatedJob(objectName).WithBackfillRequest(objectName))

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Backfilling)
	helpers.AssertJobExists(t, k8sClient, objectName)
	helpers.AssertBackfillRequestNotCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Backfilling_with_no_job(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Backfilling).WithSuspendedSpec(false)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName))

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
	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil).AnyTimes()
	reconciler, _ := createReconciler(k8sClient, jobBuilder, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Backfilling)
	helpers.AssertJobExists(t, k8sClient, objectName)
	helpers.AssertBackfillRequestNotCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Pending_To_Backfilling_with_schedule(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Pending).WithSuspendedSpec(false).WithSchedule("* * * * *")
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName))

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
	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil).AnyTimes()
	reconciler, _ := createReconciler(k8sClient, jobBuilder, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Backfilling)
	helpers.AssertJobExists(t, k8sClient, objectName)
	helpers.AssertBackfillRequestNotCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Pending_with_schedule(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Backfilling).WithSuspendedSpec(false).WithSchedule("* * * * *")
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithOutdatedJob(objectName))

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
	helpers.AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Suspended(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Backfilling).WithSuspendedSpec(true)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName))

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
	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil).AnyTimes()
	reconciler, _ := createReconciler(k8sClient, jobBuilder, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
	helpers.AssertJobNotExists(t, k8sClient, objectName)
	helpers.AssertBackfillRequestCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_Job_Failed(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Backfilling).WithSuspendedSpec(false)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName).WithFailedJob(objectName))
	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Failed)
	helpers.AssertJobNotExists(t, k8sClient, objectName)
	helpers.AssertBackfillRequestCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Running(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Backfilling).WithSuspendedSpec(false)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithOutdatedJob(objectName))

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: objectName.Namespace, Name: objectName.Name}}
	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil).AnyTimes()
	reconciler, _ := createReconciler(k8sClient, jobBuilder, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
	helpers.AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Failed_to_Failed(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Failed)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithFailedJob(objectName))

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Failed)
	helpers.AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Failed_to_Failed_without_job(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Failed).WithSuspendedSpec(false)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, nil)

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Failed)
}

func Test_UpdatePhase_Failed_to_Suspended_without_job(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Failed).WithSuspendedSpec(true)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, nil)

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
}

func Test_UpdatePhase_Failed_to_Suspended_with_BackfillRequest(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Failed).WithSuspendedSpec(true)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName))

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
	helpers.AssertBackfillRequestCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Failed_to_Backfilling(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Failed).WithSuspendedSpec(false)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName))

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil).AnyTimes()
	reconciler, _ := createReconciler(k8sClient, jobBuilder, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Backfilling)
	helpers.AssertBackfillRequestNotCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Scheduled_to_Scheduled_no_cron_job(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).
		WithPhase(stream.Scheduled).
		WithSuspendedSpec(false).
		WithSchedule("* * * * *")
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, nil)

	u, err := helpers.GetStreamDefinitionUnstructured(t.Context(), k8sClient, objectName, helpers.GroupVersionKindV2)
	require.NoError(t, err)

	def, err := contracts.FromUnstructured(u)
	require.NoError(t, err)

	definitionHash, err := def.CurrentConfiguration(nil)
	require.NoError(t, err)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil).AnyTimes()
	reconciler, _ := createReconciler(k8sClient, jobBuilder, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Scheduled)
	helpers.AssertCronJobExists(t, k8sClient, objectName, func(t *testing.T, cj *batchv1.CronJob) {
		require.NotNil(t, cj, "CronJob should be created")
		require.Equal(t, definitionHash, cj.Annotations["configuration-hash"])
	})
}

func Test_UpdatePhase_Scheduled_to_Scheduled_recreate_cron_job(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).
		WithPhase(stream.Scheduled).
		WithSuspendedSpec(false).
		WithSchedule("* * * * *")
	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithOutdatedCronJob(objectName)
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, resourceBuilder)

	u, err := helpers.GetStreamDefinitionUnstructured(t.Context(), k8sClient, objectName, helpers.GroupVersionKindV2)
	require.NoError(t, err)

	def, err := contracts.FromUnstructured(u)
	require.NoError(t, err)

	definitionHash, err := def.CurrentConfiguration(nil)
	require.NoError(t, err)

	oldJob := &batchv1.CronJob{}
	err = k8sClient.Get(t.Context(), objectName, oldJob)
	require.NoError(t, err)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil).AnyTimes()
	reconciler, _ := createReconciler(k8sClient, jobBuilder, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Scheduled)
	helpers.AssertCronJobExists(t, k8sClient, objectName, func(t *testing.T, cj *batchv1.CronJob) {
		require.NotEqual(t,
			oldJob.GetResourceVersion(),
			cj.GetResourceVersion(),
			"CronJob should be recreated with a new resource version")

		require.Equal(t, definitionHash, cj.Annotations["configuration-hash"])
	})
}

func Test_UpdatePhase_Scheduled_to_Scheduled_not_recreate_cron_job(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).
		WithPhase(stream.Scheduled).
		WithSuspendedSpec(false).
		WithSchedule("* * * * *")

	k8sClient := helpers.SetupClientFromBuilders(nil, builder, nil)
	u, err := helpers.GetStreamDefinitionUnstructured(t.Context(), k8sClient, objectName, helpers.GroupVersionKindV2)
	require.NoError(t, err)

	def, err := contracts.FromUnstructured(u)
	require.NoError(t, err)

	definitionHash, err := def.CurrentConfiguration(nil)
	require.NoError(t, err)

	resourceBuilder := helpers.NewFakeClientResourcesBuilder().WithConsistentCronJob(objectName, definitionHash)
	k8sClient = helpers.SetupClientFromBuilders(nil, builder, resourceBuilder)

	oldJob := &batchv1.CronJob{}
	err = k8sClient.Get(t.Context(), objectName, oldJob)
	require.NoError(t, err)

	reconciler, _ := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Scheduled)
	helpers.AssertCronJobExists(t, k8sClient, objectName, func(t *testing.T, cj *batchv1.CronJob) {
		require.Equal(t,
			oldJob.GetResourceVersion(),
			cj.GetResourceVersion(),
			"CronJob should not be recreated and should have the same resource version")
		require.Equal(t, definitionHash, cj.Annotations["configuration-hash"])
	})
}

func Test_UpdatePhase_Scheduled_to_Suspended(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Scheduled).WithSuspendedSpec(true).WithSchedule("* * * * *")
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithOutdatedCronJob(objectName))

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil).AnyTimes()
	reconciler, _ := createReconciler(k8sClient, jobBuilder, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
	helpers.AssertCronJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Scheduled_to_Backfilling(t *testing.T) {
	// Arrange
	builder := v3.NewMockStreamDefinitionBuilder(objectName).WithPhase(stream.Scheduled).WithSuspendedSpec(false).WithSchedule("* * * * *")
	k8sClient := helpers.SetupClientFromBuilders(nil, builder, helpers.NewFakeClientResourcesBuilder().WithBackfillRequest(objectName))

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	jobBuilder := mocks.NewMockJobBuilder(mockCtrl)
	jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(&mockJob, nil).AnyTimes()
	reconciler, _ := createReconciler(k8sClient, jobBuilder, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	helpers.AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
	helpers.AssertCronJobNotExists(t, k8sClient, objectName)
	helpers.AssertBackfillRequestNotCompleted(t, k8sClient, objectName)
}

func createReconciler(k8sClient client.Client, jobBuilder *mocks.MockJobBuilder, mockCtrl *gomock.Controller) (reconcile.Reconciler, *record.FakeRecorder) {
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
		stream.BatchJob:  job.NewJobBackend(k8sClient, jobBuilder, recorder, statusManager),
		stream.CronJob:   cron_job.NewCronJobBackend(k8sClient, jobBuilder, recorder, statusManager),
		stream.NoBackend: empty.NewEmptyBackend(recorder),
	}
	reconciler := stream.NewStreamReconciler(k8sClient,
		gvk,
		jobBuilder,
		&sc,
		recorder,
		contracts.FromUnstructured,
		backendResourceManagers,
		backfillBackendResourceManager)
	return reconciler, recorder
}
