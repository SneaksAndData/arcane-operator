package helpers

import (
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	testv2 "github.com/SneaksAndData/arcane-operator/pkg/test/apis_test/streaming/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// SetupClientFromBuilders constructs a fake controller-runtime client seeded with
// the *testv2.MockStreamDefinition produced by the provided builder.
//
// Unlike SetupClient, the stream definition is supplied directly through the
// builder, avoiding the need for functional-option mutators on the definition.
// Additional resources can be registered through a FakeClientResourcesBuilder.
func SetupClientFromBuilders(builder *MockStreamDefinitionBuilder, resources *FakeClientResourcesBuilder) client.Client {
	scheme := runtime.NewScheme()
	_ = testv2.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)

	obj := builder.Build()

	clientBuilder := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(obj).
		WithStatusSubresource(&testv2.MockStreamDefinition{}).
		WithStatusSubresource(&v1.BackfillRequest{})

	if resources != nil {
		applyResourcesFunc := resources.Build()
		applyResourcesFunc(clientBuilder)
	}

	return clientBuilder.Build()
}
