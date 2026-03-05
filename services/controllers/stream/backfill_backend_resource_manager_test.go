package stream

import (
	"strings"
	"testing"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	testv1 "github.com/SneaksAndData/arcane-operator/pkg/test/apis_test/streaming/v1"
	v2 "github.com/SneaksAndData/arcane-operator/pkg/test/generated/applyconfiguration/streaming/v1"
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
	job, err := backfillBackendResourceManager.Get(t.Context(), objectName)
	require.NoError(t, err)
	require.Equal(t, objectName.Name, job.Name)
}

func Test_Get_No_Job(t *testing.T) {
	k8sClient := SetupClient(objectName, nil, nil)
	backfillBackendResourceManager := setupBackfillBackendResourceManagerTest(k8sClient)
	job, err := backfillBackendResourceManager.Get(t.Context(), objectName)
	require.NoError(t, err)
	require.Nil(t, job)
}

func Test_Remove(t *testing.T) {
	k8sClient := SetupClient(objectName, nil, WithCompletedJob(objectName))
	backfillBackendResourceManager := setupBackfillBackendResourceManagerTest(k8sClient)
	m, err := NewMockDefinitionWrapper(&testv1.MockStreamDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace},
	})
	require.NoError(t, err)
	result, err := backfillBackendResourceManager.Remove(t.Context(), m, Pending, func() {
		/* do nothing */
	})
	require.NoError(t, err)
	require.NotNil(t, result)
}

func setupBackfillBackendResourceManagerTest(k8sClient client.Client) *BackfillBackendResourceManager {
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
	gvk := schema.GroupVersionKind{Group: "streaming.sneaksanddata.com", Version: "v1", Kind: "MockStreamDefinition"}
	statusManager := NewDefaultStatusManager(k8sClient, gvk, &sc, definitionParser)
	return NewBackfillBackendResourceManager(&sc, k8sClient, statusManager)
}
