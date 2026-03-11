package tests

import (
	"strings"
	"testing"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	testv2 "github.com/SneaksAndData/arcane-operator/pkg/test/apis_test/streaming/v2"
	v2 "github.com/SneaksAndData/arcane-operator/pkg/test/generated/applyconfiguration/streaming/v2"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/backend/job"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func Test_Get(t *testing.T) {
	k8sClient := SetupClient(objectName, nil, WithOutdatedJob(objectName))
	backfillBackendResourceManager := setupBackfillBackendResourceManagerTest(k8sClient)
	j, err := backfillBackendResourceManager.Get(t.Context(), objectName)
	require.NoError(t, err)
	require.Equal(t, objectName.Name, j.Name())
}

func Test_Get_No_Job(t *testing.T) {
	k8sClient := SetupClient(objectName, nil, nil)
	backfillBackendResourceManager := setupBackfillBackendResourceManagerTest(k8sClient)
	j, err := backfillBackendResourceManager.Get(t.Context(), objectName)
	require.NoError(t, err)
	require.Nil(t, j)
}

func Test_Remove(t *testing.T) {
	k8sClient := SetupClient(objectName, nil, WithCompletedJob(objectName))
	backfillBackendResourceManager := setupBackfillBackendResourceManagerTest(k8sClient)
	m, err := stream.NewMockDefinitionWrapper(&testv2.MockStreamDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace},
	})
	require.NoError(t, err)
	result, err := backfillBackendResourceManager.Remove(t.Context(), m, stream.Pending, func() {
		/* do nothing */
	})
	require.NoError(t, err)
	require.NotNil(t, result)
}

func Test_Remove_WithBackfillRequest(t *testing.T) {
	k8sClient := SetupClient(objectName,
		WithNamedStreamDefinition(objectName),
		CombinedB(WithCompletedJob(objectName), WithBackfillRequest(objectName)),
	)
	backfillBackendResourceManager := setupBackfillBackendResourceManagerTest(k8sClient)
	m, err := stream.NewMockDefinitionWrapper(&testv2.MockStreamDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace},
	})
	require.NoError(t, err)
	result, err := backfillBackendResourceManager.Remove(t.Context(), m, stream.Pending, func() {
		/* do nothing */
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	AssertBackfillRequestCompleted(t, k8sClient, objectName)
}

func Test_Apply(t *testing.T) {
	k8sClient := SetupClient(objectName, WithNamedStreamDefinition(objectName), nil)
	backfillBackendResourceManager := setupBackfillBackendResourceManagerTest(k8sClient)
	m, err := stream.NewMockDefinitionWrapper(&testv2.MockStreamDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace},
	})
	require.NoError(t, err)
	bfr := &v1.BackfillRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "backfill1", Namespace: objectName.Namespace},
	}
	result, err := backfillBackendResourceManager.Apply(t.Context(), m, bfr, stream.Pending, nil, func() {
		/* do nothing */
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	AssertBackfillRequestNotCompleted(t, k8sClient, objectName)
}

func Test_GetBackfillRequest_Empty(t *testing.T) {
	k8sClient := SetupClient(objectName, WithNamedStreamDefinition(objectName), nil)
	backfillBackendResourceManager := setupBackfillBackendResourceManagerTest(k8sClient)
	m, err := stream.NewMockDefinitionWrapper(&testv2.MockStreamDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace},
	})
	require.NoError(t, err)

	result, err := backfillBackendResourceManager.GetBackfillRequest(t.Context(), m)
	require.NoError(t, err)
	require.Nil(t, result)
}

func setupBackfillBackendResourceManagerTest(k8sClient client.Client) *job.BackfillBackendResourceManager {
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
	definitionParser := func(u *unstructured.Unstructured) (stream.Definition, error) {
		var mock testv2.MockStreamDefinition
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &mock); err != nil {
			return nil, err
		}
		return stream.NewMockDefinitionWrapper(&mock)
	}
	gvk := schema.GroupVersionKind{Group: "streaming.sneaksanddata.com", Version: "v1", Kind: "MockStreamDefinition"}
	statusManager := stream.NewDefaultStatusManager(k8sClient, gvk, &sc, definitionParser)
	return job.NewBackfillBackendResourceManager(&sc, k8sClient, statusManager)
}
