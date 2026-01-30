package stream

import (
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	testv1 "github.com/SneaksAndData/arcane-operator/pkg/test/apis_test/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/tests/mocks"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"testing"
)

var objectName types.NamespacedName = types.NamespacedName{Name: "stream1", Namespace: "default"}

func Test_UpdatePhase_New_To_Suspended(t *testing.T) {
	// Arrange
	k8sClient := setupClient(nil, nil)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, Suspended)
}

func Test_UpdatePhase_New_To_Pending(t *testing.T) {
	// Arrange
	k8sClient := setupClient(withSuspendedSpec(false), nil)
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, Pending)
}

func Test_UpdatePhase_Pending_To_Running_no_job(t *testing.T) {
	// Arrange
	k8sClient := setupClient(withPhase(Pending), nil)
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
	assertStreamDefinitionPhase(t, k8sClient, objectName, Running)
	assertJobExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Pending_To_Running_recreate_job(t *testing.T) {
	// Arrange
	k8sClient := setupClient(combined(withPhase(Pending), withNamedStreamDefinition(objectName)), withOutdatedJob(objectName))

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
	assertStreamDefinitionPhase(t, k8sClient, objectName, Running)
	assertJobExists(t, k8sClient, objectName)
	assertJobConfiguration(t, k8sClient, objectName, "new-hash")
}

func Test_UpdatePhase_Pending_To_Running_not_recreate_job(t *testing.T) {
	// Arrange
	definitionHash := "96dfc267c661ed2c5b9a7c32371a92b6" // computed manually for the test definition

	k8sClient := setupClient(combined(withPhase(Pending), withNamedStreamDefinition(objectName)), withConsistentJob(objectName, definitionHash))

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, Running)
	assertJobExists(t, k8sClient, objectName)
	assertJobConfiguration(t, k8sClient, objectName, definitionHash)
}

func Test_UpdatePhase_Pending_To_Backfilling_no_job(t *testing.T) {
	// Arrange
	k8sClient := setupClient(combined(withPhase(Pending), withNamedStreamDefinition(objectName)), withBackfillRequest(objectName))
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockJob := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace}}
	reconciler := createReconciler(k8sClient, &mockJob, mockCtrl)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, Backfilling)
	assertJobExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Pending_To_Backfilling_recreate_job(t *testing.T) {
	// Arrange
	k8sClient := setupClient(
		combined(withPhase(Pending), withNamedStreamDefinition(objectName)),
		combinedB(withBackfillRequest(objectName), withOutdatedJob(objectName)),
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
	assertStreamDefinitionPhase(t, k8sClient, objectName, Backfilling)
	assertJobExists(t, k8sClient, objectName)
	assertJobConfiguration(t, k8sClient, objectName, "new-hash")
}

func Test_UpdatePhase_Pending_To_Backfilling_no_secret_ref(t *testing.T) {
	// Arrange
	k8sClient := setupClient(
		combined(withPhase(Pending), withNamedStreamDefinition(objectName)),
		combinedB(withBackfillRequest(objectName), withOutdatedJob(objectName)),
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
	assertStreamDefinitionPhase(t, k8sClient, objectName, Backfilling)
	assertJobExists(t, k8sClient, objectName)
	assertJobConfiguration(t, k8sClient, objectName, "new-hash")
}

func Test_UpdatePhase_Running_To_Suspended_no_job(t *testing.T) {
	// Arrange
	k8sClient := setupClient(combined(withPhase(Running), withSuspendedSpec(true)), nil)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, Suspended)
}

func Test_UpdatePhase_Running_To_Suspended_stop_job(t *testing.T) {
	// Arrange
	k8sClient := setupClient(
		combined(withNamedStreamDefinition(objectName), combined(withPhase(Running), withSuspendedSpec(true))),
		combinedB(withBackfillRequest(objectName), withOutdatedJob(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, Suspended)
	assertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Running_To_Suspended_to_Pending(t *testing.T) {
	// Arrange
	k8sClient := setupClient(
		combined(withNamedStreamDefinition(objectName), combined(withPhase(Suspended), withSuspendedSpec(false))),
		nil,
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, Pending)
}

func Test_UpdatePhase_Running_with_BackfillRequest_no_job(t *testing.T) {
	// Arrange
	k8sClient := setupClient(
		combined(withNamedStreamDefinition(objectName), withPhase(Running), withSuspendedSpec(false)),
		combinedB(withBackfillRequest(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, Pending)
}

func Test_UpdatePhase_Suspended_with_BackfillRequest(t *testing.T) {
	// Arrange
	k8sClient := setupClient(
		combined(withNamedStreamDefinition(objectName), withPhase(Suspended), withSuspendedSpec(false)),
		combinedB(withBackfillRequest(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, Pending)
}

func Test_UpdatePhase_Suspended_without_BackfillRequest_without_job(t *testing.T) {
	// Arrange
	k8sClient := setupClient(
		combined(withNamedStreamDefinition(objectName), withPhase(Suspended)),
		nil,
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, Suspended)
	assertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Suspended_without_BackfillRequest_with_job(t *testing.T) {
	// Arrange
	k8sClient := setupClient(
		combined(withNamedStreamDefinition(objectName), withPhase(Suspended)),
		combinedB(withOutdatedJob(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, Suspended)
	assertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Backfilling_To_Pending_with_job_running(t *testing.T) {
	// Arrange
	k8sClient := setupClient(
		combined(withNamedStreamDefinition(objectName), withPhase(Backfilling), withSuspendedSpec(false)),
		combinedB(withOutdatedJob(objectName), withBackfillRequest(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, Backfilling)
	assertJobExists(t, k8sClient, objectName)
	assertBackfillRequestNotCompleted(t, k8sClient)
}

func Test_UpdatePhase_Backfilling_To_Pending_with_job_completed(t *testing.T) {
	// Arrange
	k8sClient := setupClient(
		combined(withNamedStreamDefinition(objectName), withPhase(Backfilling), withSuspendedSpec(false)),
		combinedB(withBackfillRequest(objectName), withCompletedJob(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, Pending)
	assertJobNotExists(t, k8sClient, objectName)
	assertBackfillRequestCompleted(t, k8sClient)
}

func Test_UpdatePhase_Backfilling_To_Backfilling_with_no_job(t *testing.T) {
	// Arrange
	k8sClient := setupClient(
		combined(withNamedStreamDefinition(objectName), withPhase(Backfilling), withSuspendedSpec(false)),
		combinedB(withBackfillRequest(objectName)),
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
	assertStreamDefinitionPhase(t, k8sClient, objectName, Backfilling)
	assertJobExists(t, k8sClient, objectName)
	assertBackfillRequestNotCompleted(t, k8sClient)
}

func Test_UpdatePhase_Backfilling_To_Suspended(t *testing.T) {
	// Arrange
	k8sClient := setupClient(
		combined(withNamedStreamDefinition(objectName), withPhase(Backfilling), withSuspendedSpec(true)),
		combinedB(withBackfillRequest(objectName)),
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
	assertStreamDefinitionPhase(t, k8sClient, objectName, Suspended)
	assertJobNotExists(t, k8sClient, objectName)
	assertBackfillRequestNotCompleted(t, k8sClient)
}

func Test_UpdatePhase_Job_Failed(t *testing.T) {
	// Arrange
	k8sClient := setupClient(
		combined(withNamedStreamDefinition(objectName), withPhase(Backfilling), withSuspendedSpec(false)),
		combinedB(withBackfillRequest(objectName), withFailedJob(objectName)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: objectName})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, Failed)
	assertJobNotExists(t, k8sClient, objectName)
	assertBackfillRequestNotCompleted(t, k8sClient)
}

func Test_UpdatePhase_Failed_to_Failed(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := setupClient(
		combined(withNamedStreamDefinition(name), withPhase(Failed)),
		combinedB(withFailedJob(name)),
	)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, Failed)
	assertJobNotExists(t, k8sClient, objectName)
}

func Test_UpdatePhase_Failed_to_Failed_without_job(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := setupClient(combined(withNamedStreamDefinition(name), withPhase(Failed), withSuspendedSpec(false)), nil)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, Failed)
}

func Test_UpdatePhase_Failed_to_Suspended_without_job(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := setupClient(combined(withNamedStreamDefinition(name), withPhase(Failed)), nil)
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, Suspended)
}

func Test_UpdatePhase_Failed_to_Suspended_with_BackfillRequest(t *testing.T) {
	// Arrange
	name := types.NamespacedName{Name: "stream1", Namespace: "default"}
	k8sClient := setupClient(combined(withNamedStreamDefinition(name), withPhase(Failed)), withBackfillRequest(objectName))
	reconciler := createReconciler(k8sClient, nil, nil)

	// Act
	result, err := reconciler.Reconcile(t.Context(), reconcile.Request{NamespacedName: name})
	require.NoError(t, err)
	require.Equal(t, result, reconcile.Result{})

	// Assert
	assertStreamDefinitionPhase(t, k8sClient, objectName, Suspended)
	assertBackfillRequestCompleted(t, k8sClient)
}

func setupClient(definition func(definition *testv1.MockStreamDefinition), addResources func(client2 *crfake.ClientBuilder)) client.Client {
	scheme := runtime.NewScheme()
	_ = testv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)

	obj := &testv1.MockStreamDefinition{
		TypeMeta:   metav1.TypeMeta{APIVersion: "streaming.sneaksanddata.com/v1", Kind: "MockStreamDefinition"},
		ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace},
		Spec: testv1.MockStreamDefinitionSpec{
			Source:      "sourceA",
			Destination: "destinationB",
			Suspended:   true,
		},
	}
	if definition != nil {
		definition(obj)
	}

	clientBuilder := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(obj).WithStatusSubresource(&testv1.MockStreamDefinition{}).WithStatusSubresource(&v1.BackfillRequest{})
	if addResources != nil {
		addResources(clientBuilder)
	}
	return clientBuilder.Build()
}

func createReconciler(k8sClient client.Client, mockJob *batchv1.Job, mockCtrl *gomock.Controller) reconcile.Reconciler {
	var jobBuilder *mocks.MockJobBuilder
	if mockJob != nil {
		jobBuilder = mocks.NewMockJobBuilder(mockCtrl)
		jobBuilder.EXPECT().BuildJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockJob, nil).AnyTimes()
	}
	recorder := record.NewFakeRecorder(10)
	gvk := schema.GroupVersionKind{Group: "streaming.sneaksanddata.com", Version: "v1", Kind: "MockStreamDefinition"}
	return NewStreamReconciler(k8sClient, gvk, jobBuilder, &v1.StreamClass{ObjectMeta: metav1.ObjectMeta{Name: "stream-class"}}, recorder)
}

// Helper function that combines multiple definition modifiers into one
func combined(funcs ...func(definition *testv1.MockStreamDefinition)) func(definition *testv1.MockStreamDefinition) {
	return func(definition *testv1.MockStreamDefinition) {
		for _, f := range funcs {
			f(definition)
		}
	}
}

// Helper function that combines multiple client modifiers into one
func combinedB(funcs ...func(definition *crfake.ClientBuilder)) func(definition *crfake.ClientBuilder) {
	return func(definition *crfake.ClientBuilder) {
		for _, f := range funcs {
			f(definition)
		}
	}
}

func withSuspendedSpec(spec bool) func(definition *testv1.MockStreamDefinition) {
	return func(definition *testv1.MockStreamDefinition) {
		definition.Spec.Suspended = spec
	}
}

func withPhase(phase Phase) func(definition *testv1.MockStreamDefinition) {
	return func(definition *testv1.MockStreamDefinition) {
		definition.Status.Phase = string(phase)
	}
}

func withNamedStreamDefinition(n types.NamespacedName) func(definition *testv1.MockStreamDefinition) {
	return func(definition *testv1.MockStreamDefinition) {
		definition.Name = n.Name
		definition.Namespace = n.Namespace
	}
}

func withOutdatedJob(n types.NamespacedName) func(definition *crfake.ClientBuilder) {
	return func(client2 *crfake.ClientBuilder) {
		client2.WithObjects(&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: n.Namespace,
				Name:      n.Name,
				Annotations: map[string]string{
					"configuration-hash": "old-hash",
				},
			},
		})
	}
}

func withCompletedJob(n types.NamespacedName) func(definition *crfake.ClientBuilder) {
	return func(client2 *crfake.ClientBuilder) {
		client2.WithObjects(&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: n.Namespace,
				Name:      n.Name,
				Annotations: map[string]string{
					"configuration-hash": "old-hash",
				},
			},
			Status: batchv1.JobStatus{
				Succeeded: 1,
				Conditions: []batchv1.JobCondition{
					{
						Type:   batchv1.JobComplete,
						Status: corev1.ConditionTrue,
					},
				},
			},
		})
	}
}

func withFailedJob(n types.NamespacedName) func(definition *crfake.ClientBuilder) {
	return func(client2 *crfake.ClientBuilder) {
		backoffLimit := int32(1)
		client2.WithObjects(&batchv1.Job{
			Spec: batchv1.JobSpec{
				BackoffLimit: &backoffLimit,
			},
			ObjectMeta: metav1.ObjectMeta{
				Namespace: n.Namespace,
				Name:      n.Name,
				Annotations: map[string]string{
					"configuration-hash": "old-hash",
				},
			},
			Status: batchv1.JobStatus{
				Failed: 2,
				Conditions: []batchv1.JobCondition{
					{
						Type:   batchv1.JobFailed,
						Status: corev1.ConditionTrue,
					},
					{
						Type:   batchv1.JobFailed,
						Status: corev1.ConditionTrue,
					},
				},
			},
		})
	}
}

func withConsistentJob(n types.NamespacedName, hash string) func(definition *crfake.ClientBuilder) {
	return func(client2 *crfake.ClientBuilder) {
		client2.WithObjects(&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: n.Namespace,
				Name:      n.Name,
				Annotations: map[string]string{
					"configuration-hash": hash,
				},
			},
		})
	}
}

func withBackfillRequest(n types.NamespacedName) func(definition *crfake.ClientBuilder) {
	return func(client2 *crfake.ClientBuilder) {
		client2.WithObjects(&v1.BackfillRequest{
			ObjectMeta: metav1.ObjectMeta{Name: "backfill1", Namespace: n.Namespace},
			Spec: v1.BackfillRequestSpec{
				StreamClass: "MockStreamDefinition",
				StreamId:    n.Name,
			},
		})
	}
}

func assertStreamDefinitionPhase(t *testing.T, k8sClient client.Client, name types.NamespacedName, phase Phase) {
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

	jobConfiguration, err := StreamingJob(*newJob).CurrentConfiguration()
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
