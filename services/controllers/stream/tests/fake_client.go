package tests

import (
	"testing"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	testv2 "github.com/SneaksAndData/arcane-operator/pkg/test/apis_test/streaming/v2"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend/job"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func SetupClient(objectName types.NamespacedName, definition func(definition *testv2.MockStreamDefinition), addResources func(client2 *crfake.ClientBuilder)) client.Client {
	scheme := runtime.NewScheme()
	_ = testv2.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)

	obj := &testv2.MockStreamDefinition{
		TypeMeta:   metav1.TypeMeta{APIVersion: "streaming.sneaksanddata.com/v2", Kind: "MockStreamDefinition"},
		ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace},
		Spec: testv2.MockStreamDefinitionSpec{
			Source:      "sourceA",
			Destination: "destinationB",
			ExecutionSettings: testv2.ExecutionSettings{
				APIVersion: "v1",
				Suspended:  true,
				StreamingBackend: testv2.StreamingBackend{
					BatchJobBackend: &testv2.BatchJobBackend{},
					CronJobBackend:  nil,
				},
			},
		},
	}
	if definition != nil {
		definition(obj)
	}

	clientBuilder := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(obj).WithStatusSubresource(&testv2.MockStreamDefinition{}).WithStatusSubresource(&v1.BackfillRequest{})
	if addResources != nil {
		addResources(clientBuilder)
	}
	return clientBuilder.Build()
}

func Combined(funcs ...func(definition *testv2.MockStreamDefinition)) func(definition *testv2.MockStreamDefinition) {
	return func(definition *testv2.MockStreamDefinition) {
		for _, f := range funcs {
			f(definition)
		}
	}
}

func CombinedB(funcs ...func(definition *crfake.ClientBuilder)) func(definition *crfake.ClientBuilder) {
	return func(definition *crfake.ClientBuilder) {
		for _, f := range funcs {
			f(definition)
		}
	}
}

func WithSuspendedSpec(spec bool) func(definition *testv2.MockStreamDefinition) {
	return func(definition *testv2.MockStreamDefinition) {
		definition.Spec.ExecutionSettings.Suspended = spec
	}
}

func WithPhase(phase stream.Phase) func(definition *testv2.MockStreamDefinition) {
	return func(definition *testv2.MockStreamDefinition) {
		definition.Status.Phase = string(phase)
	}
}

func WithNamedStreamDefinition(n types.NamespacedName) func(definition *testv2.MockStreamDefinition) {
	return func(definition *testv2.MockStreamDefinition) {
		definition.Name = n.Name
		definition.Namespace = n.Namespace
	}
}

func WithSchedule(schedule string) func(definition *testv2.MockStreamDefinition) {
	return func(definition *testv2.MockStreamDefinition) {
		definition.Spec.ExecutionSettings.StreamingBackend = testv2.StreamingBackend{
			BatchJobBackend: nil,
			CronJobBackend: &testv2.CronJobBackend{
				Schedule: schedule,
			},
		}
	}
}

func WithOutdatedJob(n types.NamespacedName) func(definition *crfake.ClientBuilder) {
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

func WithOutdatedCronJob(n types.NamespacedName) func(definition *crfake.ClientBuilder) {
	return func(client2 *crfake.ClientBuilder) {
		client2.WithObjects(&batchv1.CronJob{
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

func WithCompletedJob(n types.NamespacedName) func(definition *crfake.ClientBuilder) {
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

func WithFailedJob(n types.NamespacedName) func(definition *crfake.ClientBuilder) {
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

func WithConsistentJob(n types.NamespacedName, hash string) func(definition *crfake.ClientBuilder) {
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

func WithConsistentCronJob(n types.NamespacedName, hash string) func(definition *crfake.ClientBuilder) {
	return func(client2 *crfake.ClientBuilder) {
		client2.WithObjects(&batchv1.CronJob{
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

func WithBackfillRequest(n types.NamespacedName) func(definition *crfake.ClientBuilder) {
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

func AssertStreamDefinitionPhase(t *testing.T, k8sClient client.Client, name types.NamespacedName, phase stream.Phase) {
	sd := &testv2.MockStreamDefinition{}
	err := k8sClient.Get(t.Context(), name, sd)
	require.NoError(t, err)
	require.Equal(t, string(phase), sd.Status.Phase)
}

func AssertJobExists(t *testing.T, k8sClient client.Client, name types.NamespacedName) {
	newJob := &batchv1.Job{}
	err := k8sClient.Get(t.Context(), name, newJob)
	require.NoError(t, err)
}

func AssertCronJobExists(t *testing.T, k8sClient client.Client, name types.NamespacedName, additionalAssert func(*testing.T, *batchv1.CronJob)) {
	cj := &batchv1.CronJob{}
	err := k8sClient.Get(t.Context(), name, cj)
	require.NoError(t, err)
	if additionalAssert != nil {
		additionalAssert(t, cj)
	}
}

func AssertCronJobNotExists(t *testing.T, k8sClient client.Client, name types.NamespacedName) {
	newJob := &batchv1.CronJob{}
	err := k8sClient.Get(t.Context(), name, newJob)
	require.True(t, errors.IsNotFound(err))
}

func AssertJobNotExists(t *testing.T, k8sClient client.Client, name types.NamespacedName) {
	newJob := &batchv1.Job{}
	err := k8sClient.Get(t.Context(), name, newJob)
	require.True(t, errors.IsNotFound(err))
}

func AssertJobConfiguration(t *testing.T, k8sClient client.Client, name types.NamespacedName, expectedConfiguration string) {
	newJob := &batchv1.Job{}
	err := k8sClient.Get(t.Context(), name, newJob)
	require.NoError(t, err)

	j, err := job.FromResource(newJob)
	require.NoError(t, err)

	jobConfiguration, err := j.CurrentConfiguration()
	require.NoError(t, err)
	require.Equal(t, jobConfiguration, expectedConfiguration)
}

func AssertBackfillRequestNotCompleted(t *testing.T, k8sClient client.Client, objectName types.NamespacedName) {
	backfillRequest := &v1.BackfillRequest{}
	err := k8sClient.Get(t.Context(), types.NamespacedName{Name: "backfill1", Namespace: objectName.Namespace}, backfillRequest)
	require.NoError(t, err)
	require.False(t, backfillRequest.Spec.Completed)
}

func AssertBackfillRequestCompleted(t *testing.T, k8sClient client.Client, objectName types.NamespacedName) {
	backfillRequest := &v1.BackfillRequest{}
	err := k8sClient.Get(t.Context(), types.NamespacedName{Name: "backfill1", Namespace: objectName.Namespace}, backfillRequest)
	require.NoError(t, err)
	require.True(t, backfillRequest.Spec.Completed)
}
