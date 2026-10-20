package helpers

import (
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	testv1 "github.com/SneaksAndData/arcane-operator/pkg/test/apis_test/streaming/v1"
	testv2 "github.com/SneaksAndData/arcane-operator/pkg/test/apis_test/streaming/v2"
	mockv1 "github.com/SneaksAndData/arcane-operator/services/controllers/stream/tests/helpers/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream/tests/helpers/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// SetupClientFromBuilders constructs a fake controller-runtime client seeded with the *testv1.MockStreamDefinition
// and/or *testv2.MockStreamDefinition produced by the provided builders.
func SetupClientFromBuilders(builderV1 *mockv1.MockStreamDefinitionBuilder, builderV2 *v2.MockStreamDefinitionBuilder, resources *FakeClientResourcesBuilder) client.Client {
	scheme := runtime.NewScheme()
	_ = testv1.AddToScheme(scheme)
	_ = testv2.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)

	clientBuilder := crfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1.BackfillRequest{})
	if builderV1 != nil {
		obj := builderV1.Build()
		clientBuilder = clientBuilder.WithObjects(obj).WithStatusSubresource(&testv1.MockStreamDefinition{})
	}
	if builderV2 != nil {
		obj := builderV2.Build()
		clientBuilder = clientBuilder.WithObjects(obj).WithStatusSubresource(&testv2.MockStreamDefinition{})
	}

	if resources != nil {
		applyResourcesFunc := resources.Build()
		applyResourcesFunc(clientBuilder)
	}

	return clientBuilder.Build()
}
