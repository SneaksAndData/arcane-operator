package tests

import (
	"strings"
	"testing"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	v2 "github.com/SneaksAndData/arcane-operator/pkg/test/generated/applyconfiguration/streaming/v2"
	"github.com/SneaksAndData/arcane-operator/services/controllers/contracts"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend/cron_job"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend/job"
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
	k8sClient := SetupClient(objectName, nil, nil)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
}

func Test_UpdatePhase_New_To_Pending(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName, WithSuspendedSpec(false), nil)

	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
	AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_New_To_Pending_with_schedule(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName, Combined(WithSuspendedSpec(false), WithSchedule("* * * * *")), nil)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
	AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Pending_To_Running_no_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName, WithPhase(stream.Pending), nil)
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
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Running)
	AssertJobExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Pending_To_Running_recreate_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName, Combined(WithPhase(stream.Pending), WithNamedStreamDefinition(objectName)), WithOutdatedJob(objectName))

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
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Running)
	AssertJobExists(t, k8sClient, objectName)
	AssertJobConfiguration(t, k8sClient, objectName, "new-hash")
}

func Test_UpdatePhase_Pending_To_Running_not_recreate_job(t *testing.T) {
	// Arrange
	definitionHash := "5e1983484a43115237742b27e12abbd0" // computed manually for the test definition

	k8sClient := SetupClient(objectName, Combined(WithPhase(stream.Pending), WithNamedStreamDefinition(objectName)), WithConsistentJob(objectName, definitionHash))

	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Running)
	AssertJobExists(t, k8sClient, objectName)
	AssertJobConfiguration(t, k8sClient, objectName, definitionHash)
}

func Test_UpdatePhase_Pending_To_Scheduled_no_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName, Combined(WithPhase(stream.Pending), WithSchedule("* * * * *")), nil)
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
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Scheduled)
	AssertCronJobExists(t, k8sClient, objectName, func(t *testing.T, cj *batchv1.CronJob) {
		require.Equal(t, "* * * * *", cj.Spec.Schedule)
		require.Equal(t, batchv1.ForbidConcurrent, cj.Spec.ConcurrencyPolicy)
	})
}

func Test_UpdatePhase_Pending_To_Backfilling_no_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName, Combined(WithPhase(stream.Pending), WithNamedStreamDefinition(objectName)), WithBackfillRequest(objectName))
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Backfilling)
	AssertJobExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Pending_To_Backfilling_recreate_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithPhase(stream.Pending), WithNamedStreamDefinition(objectName)),
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
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Backfilling)
	AssertJobExists(t, k8sClient, objectName)
	AssertJobConfiguration(t, k8sClient, objectName, "new-hash")
}

func Test_UpdatePhase_Running_To_Suspended_no_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName, Combined(WithPhase(stream.Running), WithSuspendedSpec(true)), nil)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
}

func Test_UpdatePhase_Running_To_Suspended_stop_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), Combined(WithPhase(stream.Running), WithSuspendedSpec(true))),
		CombinedB(WithBackfillRequest(objectName), WithOutdatedJob(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
	AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Running_To_Suspended_to_Pending(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), Combined(WithPhase(stream.Suspended), WithSuspendedSpec(false))),
		nil,
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
}

func Test_UpdatePhase_Running_To_Suspended_to_Pending_With_BFR(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), Combined(WithPhase(stream.Suspended), WithSuspendedSpec(true))),
		CombinedB(WithBackfillRequest(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
}

func Test_UpdatePhase_Running_To_Pending_with_schedule(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(
			WithNamedStreamDefinition(objectName),
			WithPhase(stream.Running),
			WithSuspendedSpec(false),
			WithSchedule("* * * * *"),
		),
		WithOutdatedJob(objectName),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
	AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Running_with_BackfillRequest_no_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(stream.Running), WithSuspendedSpec(false)),
		CombinedB(WithBackfillRequest(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
}

func Test_UpdatePhase_Suspended_with_BackfillRequest(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(stream.Suspended), WithSuspendedSpec(false)),
		CombinedB(WithBackfillRequest(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
}

func Test_UpdatePhase_Suspended_without_BackfillRequest_without_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(stream.Suspended)),
		nil,
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
	AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Suspended_without_BackfillRequest_with_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(stream.Suspended)),
		CombinedB(WithOutdatedJob(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
	AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Pending_with_job_running(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(stream.Backfilling), WithSuspendedSpec(false)),
		CombinedB(WithOutdatedJob(objectName), WithBackfillRequest(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Backfilling)
	AssertJobExists(t, k8sClient, objectName)
	AssertBackfillRequestNotCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Pending_with_job_completed(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(stream.Backfilling), WithSuspendedSpec(false)),
		CombinedB(WithBackfillRequest(objectName), WithCompletedJob(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
	AssertJobNotExists(t, k8sClient, objectName)
	AssertBackfillRequestCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Backfilling_with_no_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(stream.Backfilling), WithSuspendedSpec(false)),
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
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Backfilling)
	AssertJobExists(t, k8sClient, objectName)
	AssertBackfillRequestNotCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Pending_To_Backfilling_with_schedule(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(
			WithNamedStreamDefinition(objectName),
			WithPhase(stream.Pending),
			WithSuspendedSpec(false),
			WithSchedule("* * * * *"),
		),
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
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Backfilling)
	AssertJobExists(t, k8sClient, objectName)
	AssertBackfillRequestNotCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Pending_with_schedule(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(
			WithNamedStreamDefinition(objectName),
			WithPhase(stream.Backfilling),
			WithSuspendedSpec(false),
			WithSchedule("* * * * *"),
		),
		WithOutdatedJob(objectName),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
	AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Suspended(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(stream.Backfilling), WithSuspendedSpec(true)),
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
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
	AssertJobNotExists(t, k8sClient, objectName)
	AssertBackfillRequestCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_Job_Failed(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(stream.Backfilling), WithSuspendedSpec(false)),
		CombinedB(WithBackfillRequest(objectName), WithFailedJob(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Failed)
	AssertJobNotExists(t, k8sClient, objectName)
	AssertBackfillRequestCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Running(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(objectName), WithPhase(stream.Backfilling), WithSuspendedSpec(false)),
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
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
	AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Failed_to_Failed(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(name), WithPhase(stream.Failed)),
		CombinedB(WithFailedJob(name)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Failed)
	AssertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Failed_to_Failed_without_job(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := SetupClient(objectName, Combined(WithNamedStreamDefinition(name), WithPhase(stream.Failed), WithSuspendedSpec(false)), nil)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Failed)
}

func Test_UpdatePhase_Failed_to_Suspended_without_job(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := SetupClient(objectName, Combined(WithNamedStreamDefinition(name), WithPhase(stream.Failed)), nil)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
}

func Test_UpdatePhase_Failed_to_Suspended_with_BackfillRequest(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := SetupClient(objectName, Combined(WithNamedStreamDefinition(name), WithPhase(stream.Failed)), WithBackfillRequest(objectName))
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
	AssertBackfillRequestCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Failed_to_Backfilling(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := SetupClient(objectName,
		Combined(WithNamedStreamDefinition(name), WithPhase(stream.Failed), WithSuspendedSpec(false)),
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
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Backfilling)
	AssertBackfillRequestNotCompleted(t, k8sClient, objectName)
}

func Test_UpdatePhase_Scheduled_to_Scheduled_no_cron_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(
			WithNamedStreamDefinition(objectName),
			WithPhase(stream.Scheduled),
			WithSuspendedSpec(false),
			WithSchedule("* * * * *"),
		),
		nil,
	)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Scheduled)
	AssertCronJobExists(t, k8sClient, objectName, func(t *testing.T, cj *batchv1.CronJob) {
		require.Equal(t, "* * * * *", cj.Spec.Schedule)
		require.Equal(t, batchv1.ForbidConcurrent, cj.Spec.ConcurrencyPolicy)
		// computed manually for the test definition
		require.Equal(t, "f64da5796994069c5b50e98041dd9d2f", cj.Annotations["configuration-hash"])
	})
}

func Test_UpdatePhase_Scheduled_to_Scheduled_recreate_cron_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(
			WithNamedStreamDefinition(objectName),
			WithPhase(stream.Scheduled),
			WithSuspendedSpec(false),
			WithSchedule("* * * * *"),
		),
		WithOutdatedCronJob(objectName),
	)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Scheduled)
	AssertCronJobExists(t, k8sClient, objectName, func(t *testing.T, cj *batchv1.CronJob) {
		require.Equal(t, "* * * * *", cj.Spec.Schedule)
		require.Equal(t, batchv1.ForbidConcurrent, cj.Spec.ConcurrencyPolicy)
		// computed manually for the test definition
		require.Equal(t, "f64da5796994069c5b50e98041dd9d2f", cj.Annotations["configuration-hash"])
	})
}

func Test_UpdatePhase_Scheduled_to_Scheduled_not_recreate_cron_job(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(
			WithNamedStreamDefinition(objectName),
			WithPhase(stream.Scheduled),
			WithSuspendedSpec(false),
			WithSchedule("* * * * *"),
		),
		WithConsistentCronJob(objectName, "f64da5796994069c5b50e98041dd9d2f"),
	)

	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Scheduled)
	AssertCronJobExists(t, k8sClient, objectName, nil)
}

func Test_UpdatePhase_Scheduled_to_Suspended(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(
			WithNamedStreamDefinition(objectName),
			WithPhase(stream.Scheduled),
			WithSuspendedSpec(true),
			WithSchedule("* * * * *"),
		),
		WithOutdatedCronJob(objectName),
	)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Suspended)
	AssertCronJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Scheduled_to_Backfilling(t *testing.T) {
	// Arrange
	k8sClient := SetupClient(objectName,
		Combined(
			WithNamedStreamDefinition(objectName),
			WithPhase(stream.Scheduled),
			WithSuspendedSpec(false),
			WithSchedule("* * * * *"),
		),
		WithBackfillRequest(objectName),
	)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	AssertStreamDefinitionPhase(t, k8sClient, objectName, stream.Pending)
	AssertCronJobNotExists(t, k8sClient, objectName)
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
	statusManager := stream.NewDefaultStatusManager(k8sClient, gvk, &sc, contracts.FromUnstructured)
	backfillBackendResourceManager := job.NewBackfillBackendResourceManager(&sc, k8sClient, statusManager)
	backendResourceManagers := map[stream.Backend]stream.BackendResourceManager{
		stream.BatchJob: job.NewJobBackend(k8sClient, jobBuilder, recorder, statusManager),
		stream.CronJob:  cron_job.NewCronJobBackend(k8sClient, jobBuilder, recorder, statusManager),
	}
	return stream.NewStreamReconciler(k8sClient, gvk, jobBuilder, &sc, recorder, contracts.FromUnstructured, backendResourceManagers, backfillBackendResourceManager)
}
